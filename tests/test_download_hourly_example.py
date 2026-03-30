import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

MODULE_PATH = Path('examples/basic/download_hourly.py')
SPEC = importlib.util.spec_from_file_location('download_hourly_example', MODULE_PATH)
download_hourly = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = download_hourly
assert SPEC.loader is not None
SPEC.loader.exec_module(download_hourly)


class DownloadHourlyExampleTests(unittest.TestCase):
    def test_build_parser_accepts_country_at_be_ch_dk_hu_nl_pl_and_se(self) -> None:
        parser = download_hourly.build_parser()
        self.assertEqual(parser.parse_args(['--country', 'AT']).country, 'AT')
        self.assertEqual(parser.parse_args(['--country', 'BE']).country, 'BE')
        self.assertEqual(parser.parse_args(['--country', 'CH']).country, 'CH')
        self.assertEqual(parser.parse_args(['--country', 'DK']).country, 'DK')
        self.assertEqual(parser.parse_args(['--country', 'HU']).country, 'HU')
        self.assertEqual(parser.parse_args(['--country', 'NL']).country, 'NL')
        self.assertEqual(parser.parse_args(['--country', 'PL']).country, 'PL')
        self.assertEqual(parser.parse_args(['--country', 'SE']).country, 'SE')

    def test_main_uses_shared_at_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '1', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'tl', 'timestamp': '2024-01-01T00:00:00Z', 'value': 2.1, 'flag': '20', 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'AT']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'AT')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['1'])
        self.assertEqual(query.elements, ['tl', 'p'])
        self.assertIn('1', buffer.getvalue())

    def test_main_uses_shared_be_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '6414', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'temp_dry_shelter_avg', 'timestamp': '2024-01-01T01:00:00Z', 'value': 4.1, 'flag': '{"validated":{"TEMP_DRY_SHELTER_AVG":true}}', 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'BE']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'BE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['6414'])
        self.assertEqual(query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertIn('6414', buffer.getvalue())

    def test_main_uses_shared_ch_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': 'AIG', 'gh_id': '0-20000-0-06712', 'element': 'tas_mean', 'element_raw': 'tre200h0', 'timestamp': '2025-12-31T23:00:00Z', 'value': -3.4, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'CH']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'CH')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['AIG'])
        self.assertEqual(query.elements, ['tre200h0', 'prestah0'])
        self.assertIn('AIG', buffer.getvalue())

    def test_main_uses_shared_dk_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '06180', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'mean_temp', 'timestamp': '2024-01-01T01:00:00Z', 'value': 2.8, 'flag': '{"qcStatus":"manual","validity":true}', 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'DK']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'DK')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['06180'])
        self.assertEqual(query.elements, ['mean_temp', 'mean_pressure'])
        self.assertIn('06180', buffer.getvalue())

    def test_main_uses_shared_hu_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '13704', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'ta', 'timestamp': '2026-01-01T00:00:00Z', 'value': 1.2, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'HU']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'HU')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['13704'])
        self.assertEqual(query.elements, ['ta', 'p'])
        self.assertIn('13704', buffer.getvalue())

    def test_main_uses_shared_nl_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '0-20000-0-06260', 'gh_id': None, 'element': 'tas_mean', 'element_raw': 'T', 'timestamp': '2024-01-01T01:00:00Z', 'value': 3.1, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'NL']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'NL')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['0-20000-0-06260'])
        self.assertEqual(query.elements, ['T', 'P'])
        self.assertIn('0-20000-0-06260', buffer.getvalue())

    def test_main_uses_shared_pl_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '00375', 'gh_id': '352200375', 'element': 'tas_mean', 'element_raw': 'TEMP', 'timestamp': '2025-01-01T00:00:00Z', 'value': 1.2, 'flag': None, 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'PL']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'PL')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['00375'])
        self.assertEqual(query.elements, ['TEMP', 'PPPS'])
        self.assertIn('00375', buffer.getvalue())

    def test_main_uses_shared_se_query_shape(self) -> None:
        sample = pd.DataFrame([{'station_id': '98230', 'gh_id': None, 'element': 'tas_mean', 'element_raw': '1', 'timestamp': '2012-11-29T11:00:00Z', 'value': 3.1, 'flag': 'G', 'quality': None, 'dataset_scope': 'historical', 'resolution': '1hour'}])
        buffer = io.StringIO()
        with patch.object(download_hourly, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_hourly.py', '--country', 'SE']):
                with redirect_stdout(buffer):
                    download_hourly.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'SE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.station_ids, ['98230'])
        self.assertEqual(query.elements, ['1', '9'])
        self.assertIn('98230', buffer.getvalue())


if __name__ == '__main__':
    unittest.main()
