from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from weatherdownload import list_providers, list_resolutions, list_supported_countries


STATUS_CONFIG_PATH = Path('docs/coverage/europe_daily_status.yml')
GEODATA_PATH = Path('docs/coverage/geodata/ne_50m_admin_0_countries.geojson')
OUTPUT_SVG_PATH = Path('docs/assets/europe_daily_coverage_map.svg')
OUTPUT_JSON_PATH = Path('docs/coverage/europe_daily_coverage.json')

STATUS_COLORS = {
    'national_daily': '#1b5e20',
    'ghcnd_daily': '#8bc34a',
    'attempted_no_reliable_daily': '#c62828',
    'not_attempted': '#b0bec5',
}
VIEW_BBOX = {
    'min_lon': -25.0,
    'max_lon': 45.0,
    'min_lat': 34.0,
    'max_lat': 72.0,
}
EUROPE_COUNTRIES = (
    'AD', 'AL', 'AT', 'BA', 'BE', 'BG', 'BY', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR',
    'GB', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MC', 'MD', 'ME', 'MK', 'MT',
    'NL', 'NO', 'PL', 'PT', 'RO', 'RS', 'SE', 'SI', 'SK', 'SM', 'TR', 'UA', 'VA',
)
COUNTRY_NAME_FALLBACKS = {
    'FR': 'France',
    'GB': 'United Kingdom',
    'NO': 'Norway',
    'VA': 'Vatican',
}


def load_status_config(path: Path = STATUS_CONFIG_PATH) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding='utf-8'))
    attempted = raw.get('attempted_no_reliable_daily', {})
    if not isinstance(attempted, dict):
        raise ValueError('attempted_no_reliable_daily must be a mapping.')
    return raw


def classify_europe_daily_coverage(status_config: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    config = load_status_config() if status_config is None else status_config
    attempted = config.get('attempted_no_reliable_daily', {})
    supported_countries = set(list_supported_countries())
    summary: dict[str, dict[str, Any]] = {}

    for country in EUROPE_COUNTRIES:
        daily_providers: list[str] = []
        if country in supported_countries:
            for provider in list_providers(country=country):
                if 'daily' in list_resolutions(country=country, provider=provider):
                    daily_providers.append(provider)
        daily_providers = sorted(set(daily_providers))

        if country in attempted:
            note = attempted[country].get('note', '') if isinstance(attempted[country], dict) else ''
            summary[country] = {
                'status': 'attempted_no_reliable_daily',
                'providers': daily_providers,
                'note': note,
                'project_status_override': True,
            }
        elif any(provider != 'ghcnd' for provider in daily_providers):
            summary[country] = {
                'status': 'national_daily',
                'providers': [provider for provider in daily_providers if provider != 'ghcnd'],
            }
        elif 'ghcnd' in daily_providers:
            summary[country] = {
                'status': 'ghcnd_daily',
                'providers': ['ghcnd'],
            }
        else:
            summary[country] = {
                'status': 'not_attempted',
                'providers': [],
            }
    return summary


def load_geodata(path: Path = GEODATA_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def build_country_geometries(geojson: dict[str, Any]) -> dict[str, list[list[list[tuple[float, float]]]]]:
    geometries: dict[str, list[list[list[tuple[float, float]]]]] = {}
    for feature in geojson.get('features', []):
        properties = feature.get('properties', {})
        iso_a2 = str(properties.get('ISO_A2', '')).strip()
        if iso_a2 not in EUROPE_COUNTRIES:
            name = str(properties.get('NAME', '')).strip()
            admin = str(properties.get('ADMIN', '')).strip()
            for country_code, fallback_name in COUNTRY_NAME_FALLBACKS.items():
                if (name == fallback_name or admin == fallback_name) and country_code in EUROPE_COUNTRIES:
                    iso_a2 = country_code
                    break
        if iso_a2 not in EUROPE_COUNTRIES:
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


def _convert_polygon(coordinates: list[Any]) -> list[list[tuple[float, float]]]:
    rings: list[list[tuple[float, float]]] = []
    for ring in coordinates:
        rings.append([(float(point[0]), float(point[1])) for point in ring])
    return rings


def render_coverage_summary_json(summary: dict[str, dict[str, Any]]) -> str:
    return json.dumps(summary, indent=2, sort_keys=True) + '\n'


def render_europe_daily_coverage_svg(summary: dict[str, dict[str, Any]]) -> str:
    geodata = load_geodata()
    country_geometries = build_country_geometries(geodata)

    width = 980
    height = 760
    map_left, map_top, map_width, map_height = _fit_map_frame(
        frame_left=36,
        frame_top=34,
        frame_width=908,
        frame_height=692,
    )

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        '  <title id="title">Daily data coverage in Europe</title>',
        '  <desc id="desc">WeatherDownload implementation status for daily meteorological observation downloads in Europe. This is not a FAO coverage map.</desc>',
        '  <metadata>',
        f'    {json.dumps(summary, sort_keys=True)}',
        '  </metadata>',
        '  <style>',
        '    .country { stroke: #1f2933; stroke-width: 0.85; fill-rule: evenodd; }',
        f'    .national_daily {{ fill: {STATUS_COLORS["national_daily"]}; }}',
        f'    .ghcnd_daily {{ fill: {STATUS_COLORS["ghcnd_daily"]}; }}',
        f'    .attempted_no_reliable_daily {{ fill: {STATUS_COLORS["attempted_no_reliable_daily"]}; }}',
        f'    .not_attempted {{ fill: {STATUS_COLORS["not_attempted"]}; }}',
        '  </style>',
        f'  <rect x="0" y="0" width="{width}" height="{height}" fill="#f6f8fa"/>',
    ]

    for country in EUROPE_COUNTRIES:
        polygons = country_geometries.get(country, [])
        if not polygons:
            continue
        path_data = _country_path_data(polygons, map_left=map_left, map_top=map_top, map_width=map_width, map_height=map_height)
        if not path_data:
            continue
        status = summary[country]['status']
        providers = ', '.join(summary[country].get('providers', [])) or 'none'
        lines.extend(
            [
                f'  <path id="country-{country}" class="country {status}" d="{path_data}">',
                f'    <title>{country}: {status}; providers={providers}</title>',
                '  </path>',
            ]
        )

    lines.append('</svg>')
    return '\n'.join(lines) + '\n'


def _fit_map_frame(
    *,
    frame_left: int,
    frame_top: int,
    frame_width: int,
    frame_height: int,
) -> tuple[float, float, float, float]:
    lon_span = VIEW_BBOX['max_lon'] - VIEW_BBOX['min_lon']
    lat_span = VIEW_BBOX['max_lat'] - VIEW_BBOX['min_lat']
    scale = min(frame_width / lon_span, frame_height / lat_span)
    map_width = lon_span * scale
    map_height = lat_span * scale
    map_left = frame_left + (frame_width - map_width) / 2.0
    map_top = frame_top + (frame_height - map_height) / 2.0
    return map_left, map_top, map_width, map_height


def _country_path_data(
    polygons: list[list[list[tuple[float, float]]]],
    *,
    map_left: int,
    map_top: int,
    map_width: int,
    map_height: int,
) -> str:
    parts: list[str] = []
    for polygon in polygons:
        for ring in polygon:
            clipped = _clip_ring_to_bbox(ring)
            if len(clipped) < 3:
                continue
            projected = [_project(point[0], point[1], map_left=map_left, map_top=map_top, map_width=map_width, map_height=map_height) for point in clipped]
            if not projected:
                continue
            start_x, start_y = projected[0]
            commands = [f'M {start_x:.2f} {start_y:.2f}']
            for x, y in projected[1:]:
                commands.append(f'L {x:.2f} {y:.2f}')
            commands.append('Z')
            parts.append(' '.join(commands))
    return ' '.join(parts)


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
    map_left: int,
    map_top: int,
    map_width: int,
    map_height: int,
) -> tuple[float, float]:
    lon_fraction = (lon - VIEW_BBOX['min_lon']) / (VIEW_BBOX['max_lon'] - VIEW_BBOX['min_lon'])
    lat_fraction = (VIEW_BBOX['max_lat'] - lat) / (VIEW_BBOX['max_lat'] - VIEW_BBOX['min_lat'])
    x = map_left + lon_fraction * map_width
    y = map_top + lat_fraction * map_height
    return x, y


def write_europe_daily_coverage_assets(
    *,
    svg_path: Path = OUTPUT_SVG_PATH,
    json_path: Path = OUTPUT_JSON_PATH,
    status_config_path: Path = STATUS_CONFIG_PATH,
) -> tuple[Path, Path]:
    summary = classify_europe_daily_coverage(load_status_config(status_config_path))
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    svg_path.write_text(render_europe_daily_coverage_svg(summary), encoding='utf-8')
    json_path.write_text(render_coverage_summary_json(summary), encoding='utf-8')
    return svg_path, json_path


if __name__ == '__main__':
    svg_destination, json_destination = write_europe_daily_coverage_assets()
    print(f'Wrote {svg_destination}')
    print(f'Wrote {json_destination}')
