import importlib.util
import json
import re
import sys
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


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
        self.assertEqual(summary['daily']['IT']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['IT']['providers'], ['ghcnd'])
        self.assertEqual(summary['daily']['SK']['status'], 'attempted_no_reliable_daily')
        self.assertTrue(summary['daily']['SK']['project_status_override'])
        self.assertIn('ghcnd', summary['daily']['SK']['providers'])
        self.assertIn('recent', summary['daily']['SK']['providers'])

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

        self.assertEqual(summary['daily']['CZ']['status'], 'national_daily')
        self.assertEqual(summary['daily']['IE']['status'], 'national_daily')
        self.assertEqual(summary['daily']['LU']['status'], 'national_daily')
        self.assertEqual(summary['daily']['IT']['status'], 'ghcnd_daily')
        self.assertEqual(summary['daily']['IT']['providers'], ['ghcnd'])

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

    def test_svg_files_use_map_derived_size_and_fill_canvas(self) -> None:
        projection_bounds = MODULE._projected_view_bounds()
        canvas_bounds = MODULE._padded_bounds(
            projection_bounds,
            padding_fraction=MODULE.SVG_PADDING_FRACTION,
        )
        expected_width, expected_height = MODULE._svg_size_from_bounds(
            canvas_bounds,
            width=MODULE.SVG_MAP_WIDTH,
        )
        geodata = MODULE.load_geodata()
        country_geometries = MODULE.build_country_geometries(geodata)
        rendered_geometries = MODULE._filter_country_geometries_to_view(country_geometries)
        expected_path_data = MODULE._country_path_data(
            [polygon for polygons in rendered_geometries.values() for polygon in polygons],
            canvas_width=expected_width,
            canvas_height=expected_height,
            projection_bounds=canvas_bounds,
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
            'EE', 'LV', 'LT', 'PL', 'CZ', 'SK', 'AT', 'CH', 'IT', 'SI', 'HR', 'HU', 'RO', 'BG',
            'GR', 'BA', 'RS', 'ME', 'AL', 'MK', 'MD', 'UA', 'BY', 'TR',
        ]
        svg_text = Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8')
        for country in expected:
            self.assertIn(f'id="country-{country}"', svg_text)

    def test_daily_svg_renders_context_land_separately_from_coverage_status(self) -> None:
        svg_text = Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8')

        for country in ('CZ', 'DE', 'FR', 'IE', 'IT', 'SK'):
            self.assertIn(f'id="country-{country}"', svg_text)

        self.assertIn('class="country context-country"', svg_text)
        self.assertIn(f'.context-country {{ fill: {MODULE.CONTEXT_LAND_FILL}; }}', svg_text)
        self.assertIn('id="context-country-RU"', svg_text)
        self.assertRegex(svg_text, r'id="context-country-(MA|DZ|TN|LY|EG)"')
        self.assertNotRegex(svg_text, r'id="context-country-[A-Z]{2}" class="country context-country [^"]+"')
        self.assertNotIn('id="context-country-RU" class="country national_daily"', svg_text)
        self.assertNotIn('id="context-country-RU" data-status=', svg_text)
        self.assertNotIn('.context-country { fill: #e7ecef; }', svg_text)

    def test_daily_svg_uses_unclipped_geometry_rendering(self) -> None:
        summary = MODULE.classify_europe_coverage()
        svg_text = MODULE.render_europe_coverage_svg('daily', summary['daily'])
        geodata = MODULE.load_geodata()
        country_geometries = MODULE.build_country_geometries(geodata)
        rendered_geometries = MODULE._filter_country_geometries_to_view(country_geometries)
        projection_bounds = MODULE._padded_bounds(
            MODULE._projected_view_bounds(),
            padding_fraction=MODULE.SVG_PADDING_FRACTION,
        )
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
        self.assertIn(f'id="context-country-RU" class="country context-country" d="{ru_path}"', svg_text)

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

    def test_generated_json_contains_documented_sk_override_note(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        self.assertEqual(summary['daily']['SK']['status'], 'attempted_no_reliable_daily')
        self.assertIn('no reliable daily downloader', summary['daily']['SK']['note'])


if __name__ == '__main__':
    unittest.main()
