from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from weatherdownload import list_providers, list_resolutions, list_supported_countries


STATUS_CONFIG_PATH = Path('docs/coverage/europe_daily_status.yml')
OUTPUT_SVG_PATH = Path('docs/assets/europe_daily_coverage_map.svg')
OUTPUT_JSON_PATH = Path('docs/coverage/europe_daily_coverage.json')

STATUS_COLORS = {
    'national_daily': '#1b5e20',
    'ghcnd_daily': '#8bc34a',
    'attempted_no_reliable_daily': '#c62828',
    'not_attempted': '#b0bec5',
}
STATUS_LABELS = {
    'national_daily': 'national daily downloader',
    'ghcnd_daily': 'GHCN-Daily',
    'attempted_no_reliable_daily': 'attempted, no reliable support yet',
    'not_attempted': 'not attempted',
}
EUROPE_TILE_POSITIONS = {
    'IS': (0, 1),
    'IE': (1, 3),
    'GB': (2, 2),
    'PT': (2, 7),
    'ES': (3, 7),
    'AD': (4, 6),
    'FR': (5, 5),
    'BE': (6, 4),
    'NL': (7, 4),
    'LU': (7, 5),
    'DE': (8, 4),
    'DK': (8, 2),
    'NO': (8, 0),
    'SE': (9, 1),
    'FI': (10, 1),
    'EE': (10, 3),
    'LV': (10, 4),
    'LT': (10, 5),
    'PL': (9, 5),
    'CZ': (8, 5),
    'SK': (9, 6),
    'AT': (8, 6),
    'CH': (7, 6),
    'LI': (7, 7),
    'HU': (10, 6),
    'SI': (9, 7),
    'HR': (10, 7),
    'BA': (10, 8),
    'RS': (11, 8),
    'ME': (11, 9),
    'AL': (12, 9),
    'MK': (12, 8),
    'GR': (13, 10),
    'BG': (12, 7),
    'RO': (12, 6),
    'MD': (13, 6),
    'UA': (13, 5),
    'BY': (12, 4),
    'IT': (7, 8),
    'SM': (7, 9),
    'VA': (8, 9),
    'MC': (5, 6),
    'MT': (8, 10),
    'CY': (15, 10),
    'TR': (15, 8),
}
EUROPE_COUNTRIES = tuple(EUROPE_TILE_POSITIONS)


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


def render_coverage_summary_json(summary: dict[str, dict[str, Any]]) -> str:
    return json.dumps(summary, indent=2, sort_keys=True) + '\n'


def render_europe_daily_coverage_svg(summary: dict[str, dict[str, Any]]) -> str:
    cell_width = 56
    cell_height = 42
    margin_x = 28
    margin_y = 80
    legend_top = 630
    width = 980
    height = 790

    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="980" height="790" viewBox="0 0 980 790" role="img" aria-labelledby="title desc">',
        '  <title id="title">Daily data coverage in Europe</title>',
        '  <desc id="desc">WeatherDownload Europe-only daily meteorological observation coverage map by implementation status.</desc>',
        '  <metadata>',
        f'    {json.dumps(summary, sort_keys=True)}',
        '  </metadata>',
        '  <style>',
        '    .title { font: 700 28px Arial, sans-serif; fill: #102027; }',
        '    .subtitle { font: 400 14px Arial, sans-serif; fill: #37474f; }',
        '    .country-label { font: 700 13px Arial, sans-serif; fill: #102027; text-anchor: middle; dominant-baseline: middle; }',
        '    .legend-label { font: 400 14px Arial, sans-serif; fill: #102027; dominant-baseline: middle; }',
        '    .legend-title { font: 700 16px Arial, sans-serif; fill: #102027; }',
        '    .country rect { stroke: #455a64; stroke-width: 1; rx: 7; ry: 7; }',
        f'    .national_daily rect {{ fill: {STATUS_COLORS["national_daily"]}; }}',
        f'    .ghcnd_daily rect {{ fill: {STATUS_COLORS["ghcnd_daily"]}; }}',
        f'    .attempted_no_reliable_daily rect {{ fill: {STATUS_COLORS["attempted_no_reliable_daily"]}; }}',
        f'    .not_attempted rect {{ fill: {STATUS_COLORS["not_attempted"]}; }}',
        '  </style>',
        '  <rect x="0" y="0" width="980" height="790" fill="#f7fafc"/>',
        '  <text class="title" x="28" y="40">Daily data coverage in Europe</text>',
        '  <text class="subtitle" x="28" y="63">WeatherDownload implementation status for daily meteorological observation downloads</text>',
    ]

    for country in EUROPE_COUNTRIES:
        x, y = EUROPE_TILE_POSITIONS[country]
        left = margin_x + x * cell_width
        top = margin_y + y * cell_height
        status = summary[country]['status']
        providers = ', '.join(summary[country].get('providers', [])) or 'none'
        lines.extend(
            [
                f'  <g id="country-{country}" class="country {status}">',
                f'    <title>{country}: {status}; providers={providers}</title>',
                f'    <rect x="{left}" y="{top}" width="48" height="34"/>',
                f'    <text class="country-label" x="{left + 24}" y="{top + 17}">{country}</text>',
                '  </g>',
            ]
        )

    lines.extend(
        [
            '  <text class="legend-title" x="28" y="655">Legend</text>',
            _render_legend_row(28, legend_top, 'national_daily', 'dark green = national daily downloader'),
            _render_legend_row(28, legend_top + 34, 'ghcnd_daily', 'light green = GHCN-Daily'),
            _render_legend_row(28, legend_top + 68, 'attempted_no_reliable_daily', 'red = attempted, no reliable support yet'),
            _render_legend_row(28, legend_top + 102, 'not_attempted', 'gray = not attempted'),
        ]
    )
    lines.append('</svg>')
    return '\n'.join(lines) + '\n'


def _render_legend_row(x: int, y: int, status: str, label: str) -> str:
    return (
        f'  <g class="legend {status}">'
        f'<rect x="{x}" y="{y}" width="24" height="18" fill="{STATUS_COLORS[status]}" stroke="#455a64" stroke-width="1"/>'
        f'<text class="legend-label" x="{x + 36}" y="{y + 10}">{label}</text>'
        f'</g>'
    )


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
