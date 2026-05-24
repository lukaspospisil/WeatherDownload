from __future__ import annotations

from scripts.dev.generate_europe_coverage_maps import OUTPUT_JSON_PATH, OUTPUT_SVG_PATHS, write_europe_coverage_assets


def write_europe_daily_coverage_assets() -> tuple[object, object]:
    svg_paths, json_path = write_europe_coverage_assets()
    return OUTPUT_SVG_PATHS['daily'], OUTPUT_JSON_PATH if json_path == OUTPUT_JSON_PATH else json_path


if __name__ == '__main__':
    svg_destination, json_destination = write_europe_daily_coverage_assets()
    print(f'Wrote {svg_destination}')
    print(f'Wrote {json_destination}')
