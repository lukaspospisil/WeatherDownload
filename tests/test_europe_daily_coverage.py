import importlib.util
import json
import re
import sys
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from weatherdownload import list_providers, list_resolutions, list_supported_countries


MODULE_PATH = Path('scripts/dev/generate_europe_coverage_maps.py')
SPEC = importlib.util.spec_from_file_location('generate_europe_coverage_maps', MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class EuropeCoverageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        MODULE.write_europe_coverage_assets()

    def test_classification_matches_registry_and_documented_overrides(self) -> None:
        summary = MODULE.classify_europe_coverage()

        self.assertEqual(summary['daily']['CZ']['status'], 'national_daily')
        self.assertIn('historical_csv', summary['daily']['CZ']['providers'])
        self.assertEqual(summary['daily']['FR']['status'], 'national_daily')
        self.assertIn('meteo_france', summary['daily']['FR']['providers'])
        self.assertEqual(summary['daily']['IE']['status'], 'national_daily')
        self.assertIn('meteireann', summary['daily']['IE']['providers'])
        self.assertEqual(summary['daily']['LU']['status'], 'national_daily')
        self.assertIn('meteolux', summary['daily']['LU']['providers'])
        self.assertEqual(summary['daily']['GB']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['GB']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['BG']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['BG']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['EE']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['EE']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['GR']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['GR']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['HR']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['HR']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['IS']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['IS']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['IT']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['IT']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['KV']['status'], 'not_attempted')
        self.assertEqual(summary['daily']['KV']['providers'], [])
        self.assertEqual(summary['daily']['LT']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['LT']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['LV']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['LV']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['NO']['status'], 'national_daily')
        self.assertEqual(summary['daily']['NO']['providers'], ['frost'])
        self.assertEqual(summary['daily']['RO']['status'], 'national_daily')
        self.assertEqual(summary['daily']['RO']['providers'], ['anm'])
        self.assertEqual(summary['daily']['SI']['status'], 'national_daily')
        self.assertEqual(summary['daily']['SI']['providers'], ['arso'])
        self.assertEqual(summary['daily']['SK']['status'], 'national_daily')
        self.assertEqual(summary['daily']['SK']['providers'], ['recent'])

        self.assertEqual(summary['hourly']['DE']['status'], 'national_hourly')
        self.assertIn('historical', summary['hourly']['DE']['providers'])
        self.assertEqual(summary['10min']['NL']['status'], 'national_10min')
        self.assertIn('historical', summary['10min']['NL']['providers'])

    def test_generated_assets_exist_and_match_generator(self) -> None:
        json_path = Path('docs/coverage/europe_coverage.json')
        self.assertTrue(json_path.exists())

        expected_summary = MODULE.classify_europe_coverage()
        expected_json = MODULE.render_coverage_summary_json(expected_summary)
        self.assertEqual(json_path.read_text(encoding='utf-8'), expected_json)

        for resolution_name, svg_path in MODULE.OUTPUT_SVG_PATHS.items():
            self.assertTrue(svg_path.exists())
            expected_svg = MODULE.render_europe_coverage_svg(resolution_name, expected_summary[resolution_name])
            self.assertEqual(svg_path.read_text(encoding='utf-8'), expected_svg)

    def test_json_has_expected_top_level_keys(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        self.assertEqual(set(summary.keys()), {'daily', 'hourly', '10min'})

    def test_generated_json_contains_only_coverage_countries(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        expected_countries = set(MODULE.COVERAGE_COUNTRIES)
        context_only_examples = {'RU', 'MA', 'DZ', 'TN', 'LY', 'EG'}
        for resolution_name in ('daily', 'hourly', '10min'):
            self.assertEqual(set(summary[resolution_name].keys()), expected_countries)
            self.assertTrue(context_only_examples.isdisjoint(summary[resolution_name].keys()))

    def test_view_filter_keeps_context_countries_without_clipping(self) -> None:
        geodata = MODULE.load_geodata()
        country_geometries = MODULE.build_country_geometries(geodata)
        filtered_geometries = MODULE._filter_country_geometries_to_view(country_geometries)

        self.assertIn('RU', filtered_geometries)
        self.assertTrue(any(code in filtered_geometries for code in ('MA', 'DZ', 'TN', 'LY', 'EG')))
        self.assertEqual(filtered_geometries['RU'], country_geometries['RU'])

    def test_daily_distinguishes_national_and_ghcnd_support(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        expected = _expected_daily_summary_from_discovery()
        ghcnd_daily_countries = {
            country
            for country, country_info in expected.items()
            if country_info['status'] == 'ghcnd_daily'
        }

        self.assertEqual(summary['daily']['CZ']['status'], 'national_daily')
        self.assertEqual(summary['daily']['IE']['status'], 'national_daily')
        self.assertEqual(summary['daily']['LU']['status'], 'national_daily')
        self.assertEqual(summary['daily']['NO']['status'], 'national_daily')
        self.assertEqual(summary['daily']['NO']['providers'], ['frost'])
        self.assertEqual(summary['daily']['KV']['status'], 'not_attempted')
        self.assertEqual(summary['daily']['KV']['providers'], [])
        self.assertTrue({'BG', 'EE', 'GR', 'HR', 'IS', 'LT', 'LV'}.issubset(ghcnd_daily_countries))

        for country in MODULE.COVERAGE_COUNTRIES:
            with self.subTest(country=country):
                self.assertEqual(summary['daily'][country], expected[country])

    def test_ghcnd_is_not_used_for_hourly_or_tenmin(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        for resolution_name in ('hourly', '10min'):
            for country_info in summary[resolution_name].values():
                self.assertNotEqual(country_info['status'], 'ghcnd_daily')
                self.assertNotIn('ghcnd', country_info.get('providers', []))

    def test_svg_files_contain_only_map_content(self) -> None:
        for svg_path in MODULE.OUTPUT_SVG_PATHS.values():
            svg_text = svg_path.read_text(encoding='utf-8')
            self.assertNotIn('<title', svg_text)
            self.assertNotIn('<desc', svg_text)
            self.assertNotIn('<metadata', svg_text)
            self.assertNotIn('<rect', svg_text)

    def test_projected_view_bbox_is_axis_aligned_rectangle(self) -> None:
        projection_bounds = MODULE._projected_view_bounds()
        width, height = MODULE._svg_size_from_bounds(
            projection_bounds,
            width=MODULE.SVG_MAP_WIDTH,
        )

        bottom_left = MODULE._project(
            MODULE.VIEW_BBOX['min_lon'],
            MODULE.VIEW_BBOX['min_lat'],
            canvas_width=width,
            canvas_height=height,
            projection_bounds=projection_bounds,
        )
        top_left = MODULE._project(
            MODULE.VIEW_BBOX['min_lon'],
            MODULE.VIEW_BBOX['max_lat'],
            canvas_width=width,
            canvas_height=height,
            projection_bounds=projection_bounds,
        )
        bottom_right = MODULE._project(
            MODULE.VIEW_BBOX['max_lon'],
            MODULE.VIEW_BBOX['min_lat'],
            canvas_width=width,
            canvas_height=height,
            projection_bounds=projection_bounds,
        )
        top_right = MODULE._project(
            MODULE.VIEW_BBOX['max_lon'],
            MODULE.VIEW_BBOX['max_lat'],
            canvas_width=width,
            canvas_height=height,
            projection_bounds=projection_bounds,
        )

        self.assertAlmostEqual(top_left[0], bottom_left[0], places=6)
        self.assertAlmostEqual(top_right[0], bottom_right[0], places=6)
        self.assertAlmostEqual(top_left[1], top_right[1], places=6)
        self.assertAlmostEqual(bottom_left[1], bottom_right[1], places=6)
        self.assertAlmostEqual(top_left[0], 0.0, places=6)
        self.assertAlmostEqual(top_left[1], 0.0, places=6)
        self.assertAlmostEqual(bottom_right[0], float(width), places=6)
        self.assertAlmostEqual(bottom_right[1], float(height), places=6)

    def test_svg_files_use_map_derived_size_and_fill_canvas(self) -> None:
        projection_bounds = MODULE._projected_view_bounds()
        expected_width, expected_height = MODULE._svg_size_from_bounds(
            projection_bounds,
            width=MODULE.SVG_MAP_WIDTH,
        )
        geodata = MODULE.load_geodata()
        country_geometries = MODULE.build_country_geometries(geodata)
        rendered_geometries = MODULE._clip_country_geometries(country_geometries)
        expected_path_data = MODULE._country_path_data(
            [polygon for polygons in rendered_geometries.values() for polygon in polygons],
            canvas_width=expected_width,
            canvas_height=expected_height,
            projection_bounds=projection_bounds,
        )
        expected_coords = [float(value) for value in re.findall(r'-?\d+(?:\.\d+)?', expected_path_data)]
        expected_xs = expected_coords[0::2]
        expected_ys = expected_coords[1::2]

        for svg_path in MODULE.OUTPUT_SVG_PATHS.values():
            root = ET.fromstring(svg_path.read_text(encoding='utf-8'))
            self.assertEqual(root.attrib['width'], str(expected_width))
            self.assertEqual(root.attrib['height'], str(expected_height))
            self.assertEqual(root.attrib['viewBox'], f'0 0 {expected_width} {expected_height}')
            self.assertEqual(root.attrib['preserveAspectRatio'], 'xMidYMid meet')

            coords: list[float] = []
            for path in root.findall('{http://www.w3.org/2000/svg}path'):
                if path.attrib.get('class') == 'country-border':
                    continue
                coords.extend(float(value) for value in re.findall(r'-?\d+(?:\.\d+)?', path.attrib['d']))

            xs = coords[0::2]
            ys = coords[1::2]
            self.assertAlmostEqual(min(xs), min(expected_xs), places=2)
            self.assertAlmostEqual(max(xs), max(expected_xs), places=2)
            self.assertAlmostEqual(min(ys), min(expected_ys), places=2)
            self.assertAlmostEqual(max(ys), max(expected_ys), places=2)

    def test_svg_renders_requested_european_country_set(self) -> None:
        expected = [
            'IS', 'IE', 'GB', 'PT', 'ES', 'FR', 'BE', 'NL', 'LU', 'DE', 'DK', 'NO', 'SE', 'FI',
            'EE', 'LV', 'LT', 'PL', 'CZ', 'SK', 'AT', 'CH', 'IT', 'KV', 'SI', 'HR', 'HU', 'RO', 'BG',
            'GR', 'BA', 'RS', 'ME', 'AL', 'MK', 'MD', 'UA', 'BY', 'TR',
        ]
        for svg_path in MODULE.OUTPUT_SVG_PATHS.values():
            svg_text = svg_path.read_text(encoding='utf-8')
            for country in expected:
                self.assertIn(f'id="country-{country}"', svg_text)

    def test_generated_svg_renders_context_land_separately_from_coverage_status(self) -> None:
        svg_text = Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8')

        for country in ('CZ', 'DE', 'FR', 'IE', 'IT', 'SK'):
            self.assertIn(f'id="country-{country}"', svg_text)

        self.assertIn('class="country context-country"', svg_text)
        self.assertIn('.country { stroke: none; fill-rule: evenodd; }', svg_text)
        self.assertIn('.country-border { fill: none; stroke: #1f2933;', svg_text)
        self.assertIn(f'.context-country {{ fill: {MODULE.CONTEXT_LAND_FILL}; }}', svg_text)
        self.assertIn('id="context-country-RU"', svg_text)
        self.assertIn('id="context-country-border-RU"', svg_text)
        self.assertRegex(svg_text, r'id="context-country-(MA|DZ|TN|LY|EG)"')
        self.assertNotRegex(svg_text, r'id="context-country-[A-Z]{2}" class="country context-country [^"]+"')
        self.assertNotIn('id="context-country-RU" class="country national_daily"', svg_text)
        self.assertNotIn('id="context-country-RU" data-status=', svg_text)
        self.assertNotIn('.context-country { fill: #e7ecef; }', svg_text)

    def test_hourly_and_tenmin_svg_match_daily_map_layout(self) -> None:
        daily_root = ET.fromstring(Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8'))
        daily_paths = _svg_paths_by_id(daily_root)
        daily_viewbox = daily_root.attrib['viewBox']
        daily_width = daily_root.attrib['width']
        daily_height = daily_root.attrib['height']

        shared_ids = [
            'context-country-RU',
            'context-country-border-RU',
            'country-CZ',
            'country-DE',
            'country-FR',
            'country-IE',
            'country-IT',
            'country-SK',
            'country-border-CZ',
            'country-border-DE',
            'country-border-FR',
            'country-border-IE',
            'country-border-IT',
            'country-border-SK',
        ]

        for resolution_name in ('hourly', '10min'):
            root = ET.fromstring(MODULE.OUTPUT_SVG_PATHS[resolution_name].read_text(encoding='utf-8'))
            paths = _svg_paths_by_id(root)

            self.assertEqual(root.attrib['width'], daily_width)
            self.assertEqual(root.attrib['height'], daily_height)
            self.assertEqual(root.attrib['viewBox'], daily_viewbox)
            self.assertEqual(root.attrib['preserveAspectRatio'], daily_root.attrib['preserveAspectRatio'])

            for path_id in shared_ids:
                self.assertIn(path_id, paths)
                self.assertEqual(paths[path_id].attrib['d'], daily_paths[path_id].attrib['d'])

            self.assertIn('context-country-RU', paths)
            self.assertTrue(any(context_id in paths for context_id in ('context-country-MA', 'context-country-DZ', 'context-country-TN', 'context-country-LY', 'context-country-EG')))
            self.assertEqual(paths['context-country-RU'].attrib['class'], 'country context-country')
            self.assertNotIn('data-status', paths['context-country-RU'].attrib)

    def test_country_border_path_data_excludes_view_boundary_segments(self) -> None:
        projection_bounds = MODULE._projected_view_bounds()
        width, height = MODULE._svg_size_from_bounds(
            projection_bounds,
            width=MODULE.SVG_MAP_WIDTH,
        )
        clipped_polygon = [[
            (MODULE.VIEW_BBOX['min_lon'], MODULE.VIEW_BBOX['min_lat']),
            (MODULE.VIEW_BBOX['max_lon'], MODULE.VIEW_BBOX['min_lat']),
            (MODULE.VIEW_BBOX['max_lon'], 50.0),
            (0.0, 50.0),
            (MODULE.VIEW_BBOX['min_lon'], 50.0),
            (MODULE.VIEW_BBOX['min_lon'], MODULE.VIEW_BBOX['min_lat']),
        ]]

        path_data = MODULE._country_border_path_data(
            [clipped_polygon],
            canvas_width=width,
            canvas_height=height,
            projection_bounds=projection_bounds,
        )
        path_segments = _path_segments(path_data)

        excluded_segments = [
            (clipped_polygon[0][0], clipped_polygon[0][1]),
            (clipped_polygon[0][1], clipped_polygon[0][2]),
            (clipped_polygon[0][4], clipped_polygon[0][5]),
        ]
        included_segments = [
            (clipped_polygon[0][2], clipped_polygon[0][3]),
            (clipped_polygon[0][3], clipped_polygon[0][4]),
        ]

        for start, end in excluded_segments:
            self.assertTrue(MODULE._segment_on_view_boundary(start, end))
            self.assertNotIn(_projected_segment(start, end, width, height, projection_bounds), path_segments)
        for start, end in included_segments:
            self.assertFalse(MODULE._segment_on_view_boundary(start, end))
            self.assertIn(_projected_segment(start, end, width, height, projection_bounds), path_segments)

    def test_generated_svg_border_paths_do_not_stroke_view_boundaries(self) -> None:
        geodata = MODULE.load_geodata()
        country_geometries = MODULE.build_country_geometries(geodata)
        rendered_geometries = MODULE._clip_country_geometries(country_geometries)
        projection_bounds = MODULE._projected_view_bounds()
        width, height = MODULE._svg_size_from_bounds(
            projection_bounds,
            width=MODULE.SVG_MAP_WIDTH,
        )

        for polygons in rendered_geometries.values():
            for polygon in polygons:
                for ring in polygon:
                    for start, end in zip(ring, ring[1:]):
                        if not MODULE._segment_on_view_boundary(start, end):
                            continue
                        segment = _projected_segment(start, end, width, height, projection_bounds)
                        border_path_data = MODULE._country_border_path_data(
                            [polygon],
                            canvas_width=width,
                            canvas_height=height,
                            projection_bounds=projection_bounds,
                        )
                        self.assertNotIn(segment, _path_segments(border_path_data))

    def test_generated_svg_border_paths_do_not_draw_viewport_rectangle_edges(self) -> None:
        for svg_path in MODULE.OUTPUT_SVG_PATHS.values():
            root = ET.fromstring(svg_path.read_text(encoding='utf-8'))
            _, _, width, height = (float(value) for value in root.attrib['viewBox'].split())

            for path in root.findall('{http://www.w3.org/2000/svg}path'):
                if path.attrib.get('class') != 'country-border':
                    continue
                for start, end in _path_segments(path.attrib['d']):
                    self.assertFalse(_same_svg_coordinate(start[0], 0.0) and _same_svg_coordinate(end[0], 0.0))
                    self.assertFalse(_same_svg_coordinate(start[0], width) and _same_svg_coordinate(end[0], width))
                    self.assertFalse(_same_svg_coordinate(start[1], 0.0) and _same_svg_coordinate(end[1], 0.0))
                    self.assertFalse(_same_svg_coordinate(start[1], height) and _same_svg_coordinate(end[1], height))

    def test_daily_svg_uses_clipped_fill_and_separate_border_rendering(self) -> None:
        summary = MODULE.classify_europe_coverage()
        svg_text = MODULE.render_europe_coverage_svg('daily', summary['daily'])
        geodata = MODULE.load_geodata()
        country_geometries = MODULE.build_country_geometries(geodata)
        rendered_geometries = MODULE._clip_country_geometries(country_geometries)
        projection_bounds = MODULE._projected_view_bounds()
        width, height = MODULE._svg_size_from_bounds(
            projection_bounds,
            width=MODULE.SVG_MAP_WIDTH,
        )

        ru_path = MODULE._country_path_data(
            rendered_geometries['RU'],
            canvas_width=width,
            canvas_height=height,
            projection_bounds=projection_bounds,
        )
        ru_border_path = MODULE._country_border_path_data(
            rendered_geometries['RU'],
            canvas_width=width,
            canvas_height=height,
            projection_bounds=projection_bounds,
        )
        self.assertIn(f'id="context-country-RU" class="country context-country" d="{ru_path}"', svg_text)
        self.assertIn(f'id="context-country-border-RU" class="country-border" d="{ru_border_path}"', svg_text)

    def test_data_coverage_documentation_references_all_maps_and_non_fao_scope(self) -> None:
        doc_text = Path('docs/data_coverage.md').read_text(encoding='utf-8')

        self.assertIn('assets/europe_daily_coverage_map.svg', doc_text)
        self.assertIn('assets/europe_hourly_coverage_map.svg', doc_text)
        self.assertIn('assets/europe_10min_coverage_map.svg', doc_text)
        self.assertIn('They are not FAO-readiness maps.', doc_text)
        self.assertIn('They do not imply that all variables are available at all stations.', doc_text)
        self.assertIn('They reflect WeatherDownload implementation status, not general public data availability in each country.', doc_text)
        self.assertIn('shown only as neutral geographic context', doc_text)
        self.assertIn('not part of the coverage classification', doc_text)

    def test_readme_shows_daily_map_and_links_to_data_coverage_page(self) -> None:
        readme_text = Path('README.md').read_text(encoding='utf-8')

        self.assertIn('docs/assets/europe_daily_coverage_map.svg', readme_text)
        self.assertNotIn('docs/assets/europe_hourly_coverage_map.svg', readme_text)
        self.assertNotIn('docs/assets/europe_10min_coverage_map.svg', readme_text)
        self.assertIn('This is daily-data coverage, not FAO-readiness coverage', readme_text)
        self.assertIn('it does not imply', readme_text)
        self.assertIn('shown only as geographic context', readme_text)
        self.assertIn('Legend: dark green = national daily downloader', readme_text)
        self.assertIn('very light gray = context land outside the coverage classification', readme_text)
        self.assertIn('European data coverage maps: [Data Coverage](docs/data_coverage.md)', readme_text)
        self.assertIn('[Data Coverage](docs/data_coverage.md)', readme_text)

    def test_supported_capabilities_and_coverage_classify_sk_recent_daily_consistently(self) -> None:
        capabilities_doc = Path('docs/supported_capabilities.md').read_text(encoding='utf-8')
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        self.assertIn('| `SK` | `recent` | `daily` | `tas_max`, `tas_min`, `sunshine_duration`, `precipitation`, `open_water_evaporation` |', capabilities_doc)
        self.assertEqual(summary['daily']['SK']['status'], 'national_daily')
        self.assertEqual(summary['daily']['SK']['providers'], ['recent'])
        self.assertNotIn('project_status_override', summary['daily']['SK'])
        self.assertNotIn('note', summary['daily']['SK'])

    def test_supported_capabilities_and_coverage_classify_no_frost_daily_consistently(self) -> None:
        capabilities_doc = Path('docs/supported_capabilities.md').read_text(encoding='utf-8')
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        self.assertIn('| `NO` | `frost` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `snow_depth` |', capabilities_doc)
        self.assertIn('| `NO` | `ghcnd` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `snow_depth` |', capabilities_doc)
        self.assertEqual(summary['daily']['NO']['status'], 'national_daily')
        self.assertEqual(summary['daily']['NO']['providers'], ['frost'])


def _path_segments(path_data: str) -> set[tuple[tuple[float, float], tuple[float, float]]]:
    tokens = re.findall(r'([ML]) (-?\d+(?:\.\d+)?) (-?\d+(?:\.\d+)?)', path_data)
    segments: set[tuple[tuple[float, float], tuple[float, float]]] = set()
    previous: tuple[float, float] | None = None
    for command, x_text, y_text in tokens:
        point = (float(x_text), float(y_text))
        if command == 'M':
            previous = point
            continue
        if previous is not None:
            segments.add((previous, point))
        previous = point
    return segments


def _svg_paths_by_id(root: ET.Element) -> dict[str, ET.Element]:
    paths: dict[str, ET.Element] = {}
    for path in root.findall('{http://www.w3.org/2000/svg}path'):
        path_id = path.attrib.get('id')
        if path_id:
            paths[path_id] = path
    return paths


def _projected_segment(
    start: tuple[float, float],
    end: tuple[float, float],
    width: int,
    height: int,
    projection_bounds: dict[str, float],
) -> tuple[tuple[float, float], tuple[float, float]]:
    projected_start = MODULE._project(
        start[0],
        start[1],
        canvas_width=width,
        canvas_height=height,
        projection_bounds=projection_bounds,
    )
    projected_end = MODULE._project(
        end[0],
        end[1],
        canvas_width=width,
        canvas_height=height,
        projection_bounds=projection_bounds,
    )
    return (
        (round(projected_start[0], 2), round(projected_start[1], 2)),
        (round(projected_end[0], 2), round(projected_end[1], 2)),
    )


def _same_svg_coordinate(value: float, boundary: float) -> bool:
    return abs(value - boundary) <= 0.01


def _expected_daily_summary_from_discovery() -> dict[str, dict[str, object]]:
    supported_countries = set(list_supported_countries())
    attempted = MODULE.load_status_config()['daily']['attempted_no_reliable']
    expected: dict[str, dict[str, object]] = {}

    for country in MODULE.COVERAGE_COUNTRIES:
        providers = _daily_providers_for_country(country, supported_countries)

        if country in attempted:
            attempted_info = attempted[country]
            expected[country] = {
                'status': 'attempted_no_reliable_daily',
                'providers': providers,
                'note': attempted_info.get('note', '') if isinstance(attempted_info, dict) else '',
                'project_status_override': True,
            }
        elif any(provider != 'ghcnd' for provider in providers):
            expected[country] = {
                'status': 'national_daily',
                'providers': [provider for provider in providers if provider != 'ghcnd'],
            }
        elif 'ghcnd' in providers:
            expected[country] = {
                'status': 'ghcnd_daily',
                'providers': ['ghcnd'],
            }
        else:
            expected[country] = {
                'status': 'not_attempted',
                'providers': [],
            }

    return expected


def _daily_providers_for_country(country: str, supported_countries: set[str]) -> list[str]:
    if country not in supported_countries:
        return []

    providers: list[str] = []
    for provider in list_providers(country=country):
        if 'daily' in set(list_resolutions(country=country, provider=provider)):
            providers.append(provider)
    return sorted(set(providers))


if __name__ == '__main__':
    unittest.main()
