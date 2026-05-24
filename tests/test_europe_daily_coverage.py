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

    def test_daily_distinguishes_national_and_ghcnd_support(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        self.assertEqual(summary['daily']['CZ']['status'], 'national_daily')
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
        geodata = MODULE.load_geodata()
        country_geometries = MODULE.build_country_geometries(geodata)
        rendered_geometries = MODULE._clip_country_geometries(country_geometries)
        projection_bounds = MODULE._projected_bounds(rendered_geometries)
        canvas_bounds = MODULE._padded_bounds(
            projection_bounds,
            padding_fraction=MODULE.SVG_PADDING_FRACTION,
        )
        expected_width, expected_height = MODULE._svg_size_from_bounds(
            canvas_bounds,
            width=MODULE.SVG_MAP_WIDTH,
        )

        for svg_path in MODULE.OUTPUT_SVG_PATHS.values():
            root = ET.fromstring(svg_path.read_text(encoding='utf-8'))
            self.assertEqual(root.attrib['width'], str(expected_width))
            self.assertEqual(root.attrib['height'], str(expected_height))
            self.assertEqual(root.attrib['viewBox'], f'0 0 {expected_width} {expected_height}')
            self.assertEqual(root.attrib['preserveAspectRatio'], 'xMidYMid meet')

            coords: list[float] = []
            for path in root.findall('{http://www.w3.org/2000/svg}path'):
                if not path.attrib.get('id', '').startswith('country-'):
                    continue
                coords.extend(float(value) for value in re.findall(r'-?\d+(?:\.\d+)?', path.attrib['d']))

            xs = coords[0::2]
            ys = coords[1::2]
            width = float(root.attrib['width'])
            height = float(root.attrib['height'])
            self.assertGreaterEqual((max(xs) - min(xs)) / width, 0.93)
            self.assertGreaterEqual((max(ys) - min(ys)) / height, 0.93)

    def test_svg_renders_requested_european_country_set(self) -> None:
        expected = [
            'IS', 'IE', 'GB', 'PT', 'ES', 'FR', 'BE', 'NL', 'LU', 'DE', 'DK', 'NO', 'SE', 'FI',
            'EE', 'LV', 'LT', 'PL', 'CZ', 'SK', 'AT', 'CH', 'IT', 'SI', 'HR', 'HU', 'RO', 'BG',
            'GR', 'BA', 'RS', 'ME', 'AL', 'MK', 'MD', 'UA', 'BY', 'TR',
        ]
        svg_text = Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8')
        for country in expected:
            self.assertIn(f'id="country-{country}"', svg_text)

    def test_data_coverage_documentation_references_all_maps_and_non_fao_scope(self) -> None:
        doc_text = Path('docs/data_coverage.md').read_text(encoding='utf-8')

        self.assertIn('assets/europe_daily_coverage_map.svg', doc_text)
        self.assertIn('assets/europe_hourly_coverage_map.svg', doc_text)
        self.assertIn('assets/europe_10min_coverage_map.svg', doc_text)
        self.assertIn('They are not FAO-readiness maps.', doc_text)
        self.assertIn('They do not imply that all variables are available at all stations.', doc_text)
        self.assertIn('They reflect WeatherDownload implementation status, not general public data availability in each country.', doc_text)
        self.assertIn('Some non-European countries may also be supported, but are not shown here.', doc_text)

    def test_readme_shows_daily_map_and_links_to_data_coverage_page(self) -> None:
        readme_text = Path('README.md').read_text(encoding='utf-8')

        self.assertIn('docs/assets/europe_daily_coverage_map.svg', readme_text)
        self.assertNotIn('docs/assets/europe_hourly_coverage_map.svg', readme_text)
        self.assertNotIn('docs/assets/europe_10min_coverage_map.svg', readme_text)
        self.assertIn('This is daily-data coverage, not FAO-readiness coverage', readme_text)
        self.assertIn('it does not imply', readme_text)
        self.assertIn('Legend: dark green = national daily downloader', readme_text)
        self.assertIn('European data coverage maps: [Data Coverage](docs/data_coverage.md)', readme_text)
        self.assertIn('[Data Coverage](docs/data_coverage.md)', readme_text)

    def test_generated_json_contains_documented_sk_override_note(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_coverage.json').read_text(encoding='utf-8'))

        self.assertEqual(summary['daily']['SK']['status'], 'attempted_no_reliable_daily')
        self.assertIn('no reliable daily downloader', summary['daily']['SK']['note'])


if __name__ == '__main__':
    unittest.main()
