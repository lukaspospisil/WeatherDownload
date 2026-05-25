from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from weatherdownload import list_providers, list_resolutions, list_supported_countries


STATUS_CONFIG_PATH = Path('docs/coverage/europe_resolution_status.yml')
GEODATA_PATH = Path('docs/coverage/geodata/ne_50m_admin_0_countries.geojson')
OUTPUT_JSON_PATH = Path('docs/coverage/europe_coverage.json')
OUTPUT_SVG_PATHS = {
    'daily': Path('docs/assets/europe_daily_coverage_map.svg'),
    'hourly': Path('docs/assets/europe_hourly_coverage_map.svg'),
    '10min': Path('docs/assets/europe_10min_coverage_map.svg'),
}

STATUS_COLORS = {
    'national_daily': '#1b5e20',
    'ghcnd_daily': '#8bc34a',
    'attempted_no_reliable_daily': '#c62828',
    'national_hourly': '#1b5e20',
    'attempted_no_reliable_hourly': '#c62828',
    'national_10min': '#1b5e20',
    'attempted_no_reliable_10min': '#c62828',
    'not_attempted': '#b0bec5',
}

VIEW_BBOX = {
    'min_lon': -18.0,
    'max_lon': 42.0,
    'min_lat': 34.0,
    'max_lat': 72.0,
}

SVG_MAP_WIDTH = 900

# Purely visual vertical scaling of the rectangular lon/lat projection.
# This makes the map less horizontally flattened without changing the
# geographic viewport, coverage semantics, country selection, or colors.
Y_STRETCH = 1.2

SVG_PADDING_FRACTION = 0.03

COVERAGE_COUNTRIES = (
    'AD', 'AL', 'AT', 'BA', 'BE', 'BG', 'BY', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR',
    'GB', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MC', 'MD', 'ME', 'MK', 'MT',
    'NL', 'NO', 'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'SM', 'TR', 'UA', 'VA',
)

# Backward-compatible alias used by older tests/docs wording.
EUROPE_COUNTRIES = COVERAGE_COUNTRIES

COUNTRY_NAME_FALLBACKS = {
    'FR': 'France',
    'GB': 'United Kingdom',
    'NO': 'Norway',
    'VA': 'Vatican',
}

CONTEXT_LAND_FILL = '#d6dee4'

RESOLUTION_SPECS = {
    'daily': {
        'discovery_resolutions': ('daily',),
        'national_status': 'national_daily',
        'attempted_status': 'attempted_no_reliable_daily',
        'attempted_config_key': 'attempted_no_reliable',
        'aria_label': 'Daily data coverage in Europe',
    },
    'hourly': {
        'discovery_resolutions': ('1hour', 'hourly'),
        'national_status': 'national_hourly',
        'attempted_status': 'attempted_no_reliable_hourly',
        'attempted_config_key': 'attempted_no_reliable',
        'aria_label': 'Hourly data coverage in Europe',
    },
    '10min': {
        'discovery_resolutions': ('10min',),
        'national_status': 'national_10min',
        'attempted_status': 'attempted_no_reliable_10min',
        'attempted_config_key': 'attempted_no_reliable',
        'aria_label': '10-minute data coverage in Europe',
    },
}


def load_status_config(path: Path = STATUS_CONFIG_PATH) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding='utf-8'))
    for resolution_name in RESOLUTION_SPECS:
        resolution_config = raw.get(resolution_name, {})
        if not isinstance(resolution_config, dict):
            raise ValueError(f'{resolution_name} status config must be a mapping.')
        attempted = resolution_config.get('attempted_no_reliable', {})
        if not isinstance(attempted, dict):
            raise ValueError(f'{resolution_name}.attempted_no_reliable must be a mapping.')
    return raw


def classify_europe_coverage(status_config: dict[str, Any] | None = None) -> dict[str, dict[str, dict[str, Any]]]:
    config = load_status_config() if status_config is None else status_config
    supported_countries = set(list_supported_countries())
    summary: dict[str, dict[str, dict[str, Any]]] = {}

    for resolution_name, spec in RESOLUTION_SPECS.items():
        attempted = config.get(resolution_name, {}).get(spec['attempted_config_key'], {})
        resolution_summary: dict[str, dict[str, Any]] = {}

        for country in COVERAGE_COUNTRIES:
            providers = _providers_for_resolution(
                country=country,
                supported_countries=supported_countries,
                discovery_resolutions=spec['discovery_resolutions'],
            )

            if country in attempted:
                note = attempted[country].get('note', '') if isinstance(attempted[country], dict) else ''
                resolution_summary[country] = {
                    'status': spec['attempted_status'],
                    'providers': providers,
                    'note': note,
                    'project_status_override': True,
                }
            elif resolution_name == 'daily' and any(provider != 'ghcnd' for provider in providers):
                resolution_summary[country] = {
                    'status': spec['national_status'],
                    'providers': [provider for provider in providers if provider != 'ghcnd'],
                }
            elif resolution_name == 'daily' and 'ghcnd' in providers:
                resolution_summary[country] = {
                    'status': 'ghcnd_daily',
                    'providers': ['ghcnd'],
                }
            elif resolution_name != 'daily' and providers:
                resolution_summary[country] = {
                    'status': spec['national_status'],
                    'providers': providers,
                }
            else:
                resolution_summary[country] = {
                    'status': 'not_attempted',
                    'providers': [],
                }

        summary[resolution_name] = resolution_summary

    return summary


def _providers_for_resolution(
    *,
    country: str,
    supported_countries: set[str],
    discovery_resolutions: tuple[str, ...],
) -> list[str]:
    providers: list[str] = []

    if country not in supported_countries:
        return providers

    for provider in list_providers(country=country):
        available_resolutions = set(list_resolutions(country=country, provider=provider))
        if available_resolutions.intersection(discovery_resolutions):
            providers.append(provider)

    return sorted(set(providers))


def load_geodata(path: Path = GEODATA_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def build_country_geometries(
    geojson: dict[str, Any],
    *,
    country_codes: set[str] | None = None,
) -> dict[str, list[list[list[tuple[float, float]]]]]:
    geometries: dict[str, list[list[list[tuple[float, float]]]]] = {}

    for feature in geojson.get('features', []):
        properties = feature.get('properties', {})
        iso_a2 = _resolve_country_code(properties)

        if not iso_a2:
            continue
        if country_codes is not None and iso_a2 not in country_codes:
            continue

        geometry = feature.get('geometry', {})
        geometry_type = geometry.get('type')
        coordinates = geometry.get('coordinates', [])

        polygons: list[list[list[tuple[float, float]]]] = []

        if geometry_type == 'Polygon':
            polygons = [_convert_polygon(coordinates)]
        elif geometry_type == 'MultiPolygon':
            polygons = [_convert_polygon(polygon) for polygon in coordinates]

        if polygons:
            geometries.setdefault(iso_a2, []).extend(polygons)

    return geometries


def _resolve_country_code(properties: dict[str, Any]) -> str:
    iso_a2 = str(properties.get('ISO_A2', '')).strip()
    if iso_a2 and iso_a2 != '-99':
        return iso_a2

    name = str(properties.get('NAME', '')).strip()
    admin = str(properties.get('ADMIN', '')).strip()

    for country_code, fallback_name in COUNTRY_NAME_FALLBACKS.items():
        if name == fallback_name or admin == fallback_name:
            return country_code

    return ''


def _convert_polygon(coordinates: list[Any]) -> list[list[tuple[float, float]]]:
    rings: list[list[tuple[float, float]]] = []
    for ring in coordinates:
        rings.append([(float(point[0]), float(point[1])) for point in ring])
    return rings


def render_coverage_summary_json(summary: dict[str, dict[str, dict[str, Any]]]) -> str:
    return json.dumps(summary, indent=2, sort_keys=True) + '\n'


def render_europe_coverage_svg(resolution_name: str, summary: dict[str, dict[str, Any]]) -> str:
    resolution_spec = RESOLUTION_SPECS[resolution_name]

    geodata = load_geodata()
    country_geometries = build_country_geometries(geodata)
    rendered_geometries = _clip_country_geometries(country_geometries)

    canvas_bounds = _projected_view_bounds()
    width, height = _svg_size_from_bounds(canvas_bounds, width=SVG_MAP_WIDTH)

    used_statuses = sorted({country_info['status'] for country_info in summary.values()})
    context_countries = sorted(country for country in rendered_geometries if country not in COVERAGE_COUNTRIES)

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" preserveAspectRatio="xMidYMid meet" role="img" aria-label="{resolution_spec["aria_label"]}" style="background-color:#f4f7f8">',
        '  <style>',
        '    .country { stroke: none; fill-rule: evenodd; }',
        '    .country-border { fill: none; stroke: #1f2933; stroke-width: 0.85; stroke-linejoin: round; stroke-linecap: round; }',
        f'    .context-country {{ fill: {CONTEXT_LAND_FILL}; }}',
    ]

    for status_name in used_statuses:
        lines.append(f'    .{status_name} {{ fill: {STATUS_COLORS[status_name]}; }}')

    lines.append('  </style>')

    # 1. Neutral context land fill.
    for country in context_countries:
        polygons = rendered_geometries.get(country, [])
        if not polygons:
            continue

        path_data = _country_path_data(
            polygons,
            canvas_width=width,
            canvas_height=height,
            projection_bounds=canvas_bounds,
        )

        if path_data:
            lines.append(f'  <path id="context-country-{country}" class="country context-country" d="{path_data}"/>')

    # 2. Context borders.
    for country in context_countries:
        polygons = rendered_geometries.get(country, [])
        if not polygons:
            continue

        path_data = _country_border_path_data(
            polygons,
            canvas_width=width,
            canvas_height=height,
            projection_bounds=canvas_bounds,
        )

        if path_data:
            lines.append(f'  <path id="context-country-border-{country}" class="country-border" d="{path_data}"/>')

    # 3. Coverage country fills.
    for country in COVERAGE_COUNTRIES:
        polygons = rendered_geometries.get(country, [])
        if not polygons:
            continue

        path_data = _country_path_data(
            polygons,
            canvas_width=width,
            canvas_height=height,
            projection_bounds=canvas_bounds,
        )

        if not path_data:
            continue

        status = summary[country]['status']
        providers = ', '.join(summary[country].get('providers', [])) or 'none'

        lines.append(
            f'  <path id="country-{country}" class="country {status}" data-status="{status}" data-providers="{providers}" d="{path_data}"/>'
        )

    # 4. Coverage borders on top.
    for country in COVERAGE_COUNTRIES:
        polygons = rendered_geometries.get(country, [])
        if not polygons:
            continue

        path_data = _country_border_path_data(
            polygons,
            canvas_width=width,
            canvas_height=height,
            projection_bounds=canvas_bounds,
        )

        if path_data:
            lines.append(f'  <path id="country-border-{country}" class="country-border" d="{path_data}"/>')

    lines.append('</svg>')
    return '\n'.join(lines) + '\n'


def _padded_bounds(bounds: dict[str, float], *, padding_fraction: float) -> dict[str, float]:
    width_span = bounds['max_x'] - bounds['min_x']
    height_span = bounds['max_y'] - bounds['min_y']

    padding_x = padding_fraction * width_span
    padding_y = padding_fraction * height_span

    return {
        'min_x': bounds['min_x'] - padding_x,
        'max_x': bounds['max_x'] + padding_x,
        'min_y': bounds['min_y'] - padding_y,
        'max_y': bounds['max_y'] + padding_y,
    }


def _svg_size_from_bounds(bounds: dict[str, float], *, width: int) -> tuple[int, int]:
    width_span = bounds['max_x'] - bounds['min_x']
    height_span = bounds['max_y'] - bounds['min_y']

    if width_span <= 0.0 or height_span <= 0.0:
        raise ValueError('Projected map bounds must have positive width and height.')

    height = round(width * height_span / width_span)
    return width, height


def _clip_country_geometries(
    country_geometries: dict[str, list[list[list[tuple[float, float]]]]]
) -> dict[str, list[list[list[tuple[float, float]]]]]:
    rendered: dict[str, list[list[list[tuple[float, float]]]]] = {}

    for country, polygons in country_geometries.items():
        clipped_polygons: list[list[list[tuple[float, float]]]] = []

        for polygon in polygons:
            clipped_rings: list[list[tuple[float, float]]] = []

            for ring in polygon:
                clipped = _clip_ring_to_bbox(ring)
                if len(clipped) >= 3:
                    clipped_rings.append(clipped)

            if clipped_rings:
                clipped_polygons.append(clipped_rings)

        if clipped_polygons:
            rendered[country] = clipped_polygons

    return rendered


def _filter_country_geometries_to_view(
    country_geometries: dict[str, list[list[list[tuple[float, float]]]]]
) -> dict[str, list[list[list[tuple[float, float]]]]]:
    rendered: dict[str, list[list[list[tuple[float, float]]]]] = {}

    for country, polygons in country_geometries.items():
        if _country_intersects_view(polygons):
            rendered[country] = polygons

    return rendered


def _country_intersects_view(polygons: list[list[list[tuple[float, float]]]]) -> bool:
    min_lon = float('inf')
    max_lon = float('-inf')
    min_lat = float('inf')
    max_lat = float('-inf')

    for polygon in polygons:
        for ring in polygon:
            for lon, lat in ring:
                min_lon = min(min_lon, lon)
                max_lon = max(max_lon, lon)
                min_lat = min(min_lat, lat)
                max_lat = max(max_lat, lat)

    if not math.isfinite(min_lon) or not math.isfinite(min_lat):
        return False

    return not (
        max_lon < VIEW_BBOX['min_lon']
        or min_lon > VIEW_BBOX['max_lon']
        or max_lat < VIEW_BBOX['min_lat']
        or min_lat > VIEW_BBOX['max_lat']
    )


def _projected_bounds(
    country_geometries: dict[str, list[list[list[tuple[float, float]]]]]
) -> dict[str, float]:
    min_x = float('inf')
    max_x = float('-inf')
    min_y = float('inf')
    max_y = float('-inf')

    for polygons in country_geometries.values():
        for polygon in polygons:
            for ring in polygon:
                for lon, lat in ring:
                    x, y = _project_lon_lat(lon, lat)
                    min_x = min(min_x, x)
                    max_x = max(max_x, x)
                    min_y = min(min_y, y)
                    max_y = max(max_y, y)

    if not math.isfinite(min_x) or not math.isfinite(min_y):
        raise ValueError('No projected geometry bounds were computed.')

    return {
        'min_x': min_x,
        'max_x': max_x,
        'min_y': min_y,
        'max_y': max_y,
    }


def _projected_view_bounds() -> dict[str, float]:
    corners = [
        (VIEW_BBOX['min_lon'], VIEW_BBOX['min_lat']),
        (VIEW_BBOX['min_lon'], VIEW_BBOX['max_lat']),
        (VIEW_BBOX['max_lon'], VIEW_BBOX['min_lat']),
        (VIEW_BBOX['max_lon'], VIEW_BBOX['max_lat']),
    ]

    projected = [_project_lon_lat(lon, lat) for lon, lat in corners]
    xs = [x for x, _ in projected]
    ys = [y for _, y in projected]

    return {
        'min_x': min(xs),
        'max_x': max(xs),
        'min_y': min(ys),
        'max_y': max(ys),
    }


def _country_path_data(
    polygons: list[list[list[tuple[float, float]]]],
    *,
    canvas_width: int,
    canvas_height: int,
    projection_bounds: dict[str, float],
) -> str:
    parts: list[str] = []

    for polygon in polygons:
        for ring in polygon:
            projected = [
                _project(
                    point[0],
                    point[1],
                    canvas_width=canvas_width,
                    canvas_height=canvas_height,
                    projection_bounds=projection_bounds,
                )
                for point in ring
            ]

            if not projected:
                continue

            start_x, start_y = projected[0]
            commands = [f'M {start_x:.2f} {start_y:.2f}']

            for x, y in projected[1:]:
                commands.append(f'L {x:.2f} {y:.2f}')

            commands.append('Z')
            parts.append(' '.join(commands))

    return ' '.join(parts)


def _country_border_path_data(
    polygons: list[list[list[tuple[float, float]]]],
    *,
    canvas_width: int,
    canvas_height: int,
    projection_bounds: dict[str, float],
) -> str:
    parts: list[str] = []

    for polygon in polygons:
        for ring in polygon:
            if len(ring) < 2:
                continue

            commands: list[str] = []
            segment_open = False

            for start, end in zip(ring, ring[1:]):
                if _segment_on_view_boundary(start, end):
                    segment_open = False
                    continue

                start_x, start_y = _project(
                    start[0],
                    start[1],
                    canvas_width=canvas_width,
                    canvas_height=canvas_height,
                    projection_bounds=projection_bounds,
                )
                end_x, end_y = _project(
                    end[0],
                    end[1],
                    canvas_width=canvas_width,
                    canvas_height=canvas_height,
                    projection_bounds=projection_bounds,
                )

                if _projected_segment_on_canvas_boundary(
                    (start_x, start_y),
                    (end_x, end_y),
                    canvas_width=canvas_width,
                    canvas_height=canvas_height,
                ):
                    segment_open = False
                    continue

                if not segment_open:
                    commands.append(f'M {start_x:.2f} {start_y:.2f}')
                    segment_open = True

                commands.append(f'L {end_x:.2f} {end_y:.2f}')

            if commands:
                parts.append(' '.join(commands))

    return ' '.join(parts)


def _segment_on_view_boundary(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    tolerance: float = 1e-9,
) -> bool:
    start_lon, start_lat = start
    end_lon, end_lat = end

    return (
        _same_coordinate(start_lon, VIEW_BBOX['min_lon'], tolerance)
        and _same_coordinate(end_lon, VIEW_BBOX['min_lon'], tolerance)
    ) or (
        _same_coordinate(start_lon, VIEW_BBOX['max_lon'], tolerance)
        and _same_coordinate(end_lon, VIEW_BBOX['max_lon'], tolerance)
    ) or (
        _same_coordinate(start_lat, VIEW_BBOX['min_lat'], tolerance)
        and _same_coordinate(end_lat, VIEW_BBOX['min_lat'], tolerance)
    ) or (
        _same_coordinate(start_lat, VIEW_BBOX['max_lat'], tolerance)
        and _same_coordinate(end_lat, VIEW_BBOX['max_lat'], tolerance)
    )


def _same_coordinate(value: float, boundary: float, tolerance: float) -> bool:
    return abs(value - boundary) <= tolerance


def _projected_segment_on_canvas_boundary(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    canvas_width: int,
    canvas_height: int,
    tolerance: float = 0.01,
) -> bool:
    start_x, start_y = start
    end_x, end_y = end

    return (
        _same_coordinate(start_x, 0.0, tolerance)
        and _same_coordinate(end_x, 0.0, tolerance)
    ) or (
        _same_coordinate(start_x, float(canvas_width), tolerance)
        and _same_coordinate(end_x, float(canvas_width), tolerance)
    ) or (
        _same_coordinate(start_y, 0.0, tolerance)
        and _same_coordinate(end_y, 0.0, tolerance)
    ) or (
        _same_coordinate(start_y, float(canvas_height), tolerance)
        and _same_coordinate(end_y, float(canvas_height), tolerance)
    )


def _clip_ring_to_bbox(ring: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if len(ring) < 3:
        return []

    points = ring[:-1] if ring[0] == ring[-1] else ring[:]
    clipped = points

    for edge_name in ('left', 'right', 'bottom', 'top'):
        clipped = _clip_polygon_against_edge(clipped, edge_name)
        if not clipped:
            return []

    if clipped and clipped[0] != clipped[-1]:
        clipped.append(clipped[0])

    return clipped


def _clip_polygon_against_edge(points: list[tuple[float, float]], edge_name: str) -> list[tuple[float, float]]:
    if not points:
        return []

    output: list[tuple[float, float]] = []

    previous = points[-1]
    previous_inside = _point_inside(previous, edge_name)

    for current in points:
        current_inside = _point_inside(current, edge_name)

        if current_inside:
            if not previous_inside:
                output.append(_intersection(previous, current, edge_name))
            output.append(current)
        elif previous_inside:
            output.append(_intersection(previous, current, edge_name))

        previous = current
        previous_inside = current_inside

    return output


def _point_inside(point: tuple[float, float], edge_name: str) -> bool:
    lon, lat = point

    if edge_name == 'left':
        return lon >= VIEW_BBOX['min_lon']
    if edge_name == 'right':
        return lon <= VIEW_BBOX['max_lon']
    if edge_name == 'bottom':
        return lat >= VIEW_BBOX['min_lat']
    if edge_name == 'top':
        return lat <= VIEW_BBOX['max_lat']

    raise ValueError(f'Unsupported edge: {edge_name}')


def _intersection(start: tuple[float, float], end: tuple[float, float], edge_name: str) -> tuple[float, float]:
    x1, y1 = start
    x2, y2 = end

    if edge_name in {'left', 'right'}:
        x_edge = VIEW_BBOX['min_lon'] if edge_name == 'left' else VIEW_BBOX['max_lon']

        if x2 == x1:
            return (x_edge, y1)

        ratio = (x_edge - x1) / (x2 - x1)
        return (x_edge, y1 + ratio * (y2 - y1))

    y_edge = VIEW_BBOX['min_lat'] if edge_name == 'bottom' else VIEW_BBOX['max_lat']

    if y2 == y1:
        return (x1, y_edge)

    ratio = (y_edge - y1) / (y2 - y1)
    return (x1 + ratio * (x2 - x1), y_edge)


def _project(
    lon: float,
    lat: float,
    *,
    canvas_width: int,
    canvas_height: int,
    projection_bounds: dict[str, float],
) -> tuple[float, float]:
    projected_x, projected_y = _project_lon_lat(lon, lat)

    x_fraction = (projected_x - projection_bounds['min_x']) / (
        projection_bounds['max_x'] - projection_bounds['min_x']
    )
    y_fraction = (projection_bounds['max_y'] - projected_y) / (
        projection_bounds['max_y'] - projection_bounds['min_y']
    )

    x = x_fraction * canvas_width
    y = y_fraction * canvas_height

    return x, y


def _project_lon_lat(lon: float, lat: float) -> tuple[float, float]:
    return lon, lat * Y_STRETCH


def write_europe_coverage_assets(
    *,
    svg_paths: dict[str, Path] = OUTPUT_SVG_PATHS,
    json_path: Path = OUTPUT_JSON_PATH,
    status_config_path: Path = STATUS_CONFIG_PATH,
) -> tuple[dict[str, Path], Path]:
    summary = classify_europe_coverage(load_status_config(status_config_path))

    for resolution_name, svg_path in svg_paths.items():
        svg_path.parent.mkdir(parents=True, exist_ok=True)
        svg_path.write_text(
            render_europe_coverage_svg(resolution_name, summary[resolution_name]),
            encoding='utf-8',
        )

    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(render_coverage_summary_json(summary), encoding='utf-8')

    return svg_paths, json_path


if __name__ == '__main__':
    svg_destinations, json_destination = write_europe_coverage_assets()

    for resolution_name in ('daily', 'hourly', '10min'):
        print(f'Wrote {svg_destinations[resolution_name]}')

    print(f'Wrote {json_destination}')