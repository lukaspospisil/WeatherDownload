import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

MODULE_PATH = Path('examples/download_tenmin.py')
SPEC = importlib.util.spec_from_file_location('download_tenmin_example', MODULE_PATH)
download_tenmin = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = download_tenmin
assert SPEC.loader is not None
SPEC.loader.exec_module(download_tenmin)


class DownloadTenminExampleTests(unittest.TestCase):
    def test_build_parser_accepts_country_at_ch_dk_hu_and_nl(self) -> None:
        parser = download_tenmin.build_parser()
        self.assertEqual(parser.parse_args(['--country', 'AT']).country, 'AT')
        self.assertEqual(parser.parse_args(['--country', 'CH']).country, 'CH')
        self.assertEqual(parser.parse_args(['--country', 'DK']).country, 'DK')
        self.assertEqual(parser.parse_args(['--country', 'HU']).country, 'HU')
        self.assertEqual(parser.parse_args(['--country', 'NL']).country, 'NL')

    def test_main_uses_shared_at_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '1', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'tl', 'timestamp': '2024-01-01T00:10:00Z', 'value': 0.1, 'flag': '12', 'quality': None, 'dataset_scope': 'historical', 'resolution': '10min'}])
        buffer = io.StringIO()
        with patch.object(download_tenmin, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_tenmin.py', '--country', 'AT']):
                with redirect_stdout(buffer):
                    download_tenmin.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'AT')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.station_ids, ['1'])
        self.assertEqual(query.elements, ['tl', 'p'])
        self.assertIn('1', buffer.getvalue())

    def test_main_uses_shared_ch_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': 'AIG', 'gh_id': '0-20000-0-06712', 'element': 'tas_mean', 'element_raw': 'tre200s0', 'timestamp': '2025-12-31T23:50:00Z', 'value': -3.7, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '10min'}])
        buffer = io.StringIO()
        with patch.object(download_tenmin, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_tenmin.py', '--country', 'CH']):
                with redirect_stdout(buffer):
                    download_tenmin.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'CH')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.station_ids, ['AIG'])
        self.assertEqual(query.elements, ['tre200s0', 'prestas0'])
        self.assertIn('AIG', buffer.getvalue())

    def test_main_uses_shared_dk_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '06180', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'temp_dry', 'timestamp': '2024-01-01T00:10:00Z', 'value': 2.1, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '10min'}])
        buffer = io.StringIO()
        with patch.object(download_tenmin, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_tenmin.py', '--country', 'DK']):
                with redirect_stdout(buffer):
                    download_tenmin.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'DK')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.station_ids, ['06180'])
        self.assertEqual(query.elements, ['temp_dry', 'pressure'])
        self.assertIn('06180', buffer.getvalue())

    def test_main_uses_shared_hu_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '13704', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'ta', 'timestamp': '2026-01-01T00:00:00Z', 'value': 1.4, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '10min'}])
        buffer = io.StringIO()
        with patch.object(download_tenmin, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_tenmin.py', '--country', 'HU']):
                with redirect_stdout(buffer):
                    download_tenmin.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'HU')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.station_ids, ['13704'])
        self.assertEqual(query.elements, ['ta', 'p'])
        self.assertIn('13704', buffer.getvalue())

    def test_main_uses_shared_nl_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '0-20000-0-06260', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'ta', 'timestamp': '2024-01-01T09:10:00Z', 'value': 3.1, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '10min'}])
        buffer = io.StringIO()
        with patch.object(download_tenmin, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_tenmin.py', '--country', 'NL']):
                with redirect_stdout(buffer):
                    download_tenmin.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'NL')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.station_ids, ['0-20000-0-06260'])
        self.assertEqual(query.elements, ['ta', 'pp'])
        self.assertIn('0-20000-0-06260', buffer.getvalue())


if __name__ == '__main__':
    unittest.main()
