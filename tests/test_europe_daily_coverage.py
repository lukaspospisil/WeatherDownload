import importlib.util
import json
import re
import sys
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


MODULE_PATH = Path('scripts/dev/generate_europe_daily_coverage_map.py')
SPEC = importlib.util.spec_from_file_location('generate_europe_daily_coverage_map', MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class EuropeDailyCoverageTests(unittest.TestCase):
    def test_classification_matches_registry_and_documented_overrides(self) -> None:
        summary = MODULE.classify_europe_daily_coverage()

        self.assertEqual(summary['CZ']['status'], 'national_daily')
        self.assertIn('historical_csv', summary['CZ']['providers'])
        self.assertEqual(summary['DE']['status'], 'national_daily')
        self.assertIn('historical', summary['DE']['providers'])
        self.assertEqual(summary['FR']['status'], 'national_daily')
        self.assertIn('meteo_france', summary['FR']['providers'])
        self.assertEqual(summary['IT']['status'], 'ghcnd_daily')
        self.assertEqual(summary['IT']['providers'], ['ghcnd'])
        self.assertEqual(summary['NO']['status'], 'ghcnd_daily')
        self.assertEqual(summary['SK']['status'], 'attempted_no_reliable_daily')
        self.assertTrue(summary['SK']['project_status_override'])
        self.assertIn('ghcnd', summary['SK']['providers'])
        self.assertIn('recent', summary['SK']['providers'])
        self.assertEqual(summary['IE']['status'], 'not_attempted')

    def test_generated_assets_exist_and_match_generator(self) -> None:
        svg_path = Path('docs/assets/europe_daily_coverage_map.svg')
        json_path = Path('docs/coverage/europe_daily_coverage.json')

        self.assertTrue(svg_path.exists())
        self.assertTrue(json_path.exists())

        expected_summary = MODULE.classify_europe_daily_coverage()
        expected_svg = MODULE.render_europe_daily_coverage_svg(expected_summary)
        expected_json = MODULE.render_coverage_summary_json(expected_summary)

        self.assertEqual(svg_path.read_text(encoding='utf-8'), expected_svg)
        self.assertEqual(json_path.read_text(encoding='utf-8'), expected_json)

    def test_svg_contains_category_metadata_without_in_image_legend(self) -> None:
        svg_text = Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8')

        self.assertIn('national_daily', svg_text)
        self.assertIn('ghcnd_daily', svg_text)
        self.assertIn('attempted_no_reliable_daily', svg_text)
        self.assertIn('not_attempted', svg_text)
        self.assertNotIn('dark green = national daily downloader', svg_text)
        self.assertNotIn('light green = GHCN-Daily', svg_text)
        self.assertNotIn('red = attempted, no reliable support yet', svg_text)
        self.assertNotIn('gray = not attempted', svg_text)

    def test_svg_root_uses_balanced_intrinsic_aspect_ratio(self) -> None:
        svg_path = Path('docs/assets/europe_daily_coverage_map.svg')
        root = ET.fromstring(svg_path.read_text(encoding='utf-8'))

        width = float(root.attrib['width'])
        height = float(root.attrib['height'])
        self.assertLessEqual(width / height, 1.35)

        view_box = root.attrib['viewBox'].split()
        self.assertEqual(len(view_box), 4)
        view_box_width = float(view_box[2])
        view_box_height = float(view_box[3])
        self.assertLessEqual(view_box_width / view_box_height, 1.35)

    def test_svg_renders_requested_european_country_set(self) -> None:
        svg_text = Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8')

        expected = [
            'IS', 'IE', 'GB', 'PT', 'ES', 'FR', 'BE', 'NL', 'LU', 'DE', 'DK', 'NO', 'SE', 'FI',
            'EE', 'LV', 'LT', 'PL', 'CZ', 'SK', 'AT', 'CH', 'IT', 'SI', 'HR', 'HU', 'RO', 'BG',
            'GR', 'BA', 'RS', 'ME', 'AL', 'MK', 'MD', 'UA', 'BY', 'TR',
        ]
        for country in expected:
            self.assertIn(f'id="country-{country}"', svg_text)

    def test_country_paths_fill_most_of_canvas(self) -> None:
        root = ET.fromstring(Path('docs/assets/europe_daily_coverage_map.svg').read_text(encoding='utf-8'))
        width = float(root.attrib['width'])
        height = float(root.attrib['height'])

        coords: list[float] = []
        for path in root.findall('{http://www.w3.org/2000/svg}path'):
            if not path.attrib.get('id', '').startswith('country-'):
                continue
            coords.extend(float(value) for value in re.findall(r'-?\d+(?:\.\d+)?', path.attrib['d']))

        xs = coords[0::2]
        ys = coords[1::2]
        self.assertGreaterEqual((max(xs) - min(xs)) / width, 0.85)
        self.assertGreaterEqual((max(ys) - min(ys)) / height, 0.75)

    def test_documentation_references_generated_svg_and_non_fao_scope(self) -> None:
        readme_text = Path('README.md').read_text(encoding='utf-8')

        self.assertIn('docs/assets/europe_daily_coverage_map.svg', readme_text)
        self.assertIn('<img src="docs/assets/europe_daily_coverage_map.svg"', readme_text)
        self.assertIn('width="900"', readme_text)
        self.assertIn('It is not a FAO-readiness map', readme_text)
        self.assertIn('Dark green - national daily downloader implemented', readme_text)
        self.assertIn('Light green - daily data available via GHCN-Daily', readme_text)
        self.assertIn('Some non-European countries may also be supported', readme_text)

    def test_generated_json_contains_documented_sk_override_note(self) -> None:
        summary = json.loads(Path('docs/coverage/europe_daily_coverage.json').read_text(encoding='utf-8'))

        self.assertEqual(summary['SK']['status'], 'attempted_no_reliable_daily')
        self.assertIn('no reliable daily downloader', summary['SK']['note'])


if __name__ == '__main__':
    unittest.main()
