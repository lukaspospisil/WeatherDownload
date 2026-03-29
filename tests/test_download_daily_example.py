import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

MODULE_PATH = Path('examples/basic/download_daily.py')
SPEC = importlib.util.spec_from_file_location('download_daily_example', MODULE_PATH)
download_daily = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = download_daily
assert SPEC.loader is not None
SPEC.loader.exec_module(download_daily)


class DownloadDailyExampleTests(unittest.TestCase):
    def test_build_parser_accepts_country_ch_dk_pl_and_se(self) -> None:
        parser = download_daily.build_parser()
        self.assertEqual(parser.parse_args(['--country', 'CH']).country, 'CH')
        self.assertEqual(parser.parse_args(['--country', 'DK']).country, 'DK')
        self.assertEqual(parser.parse_args(['--country', 'PL']).country, 'PL')
        self.assertEqual(parser.parse_args(['--country', 'SE']).country, 'SE')

    def test_main_uses_shared_ch_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': 'AIG',
                'gh_id': '0-20000-0-06712',
                'element': 'tas_mean',
                'element_raw': 'tre200d0',
                'observation_date': '2025-12-31',
                'time_function': None,
                'value': -2.2,
                'flag': None,
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])
        buffer = io.StringIO()
        with patch.object(download_daily, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_daily.py', '--country', 'CH']):
                with redirect_stdout(buffer):
                    download_daily.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'CH')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.station_ids, ['AIG'])
        self.assertEqual(query.elements, ['tre200d0', 'rre150d0', 'sre000d0'])
        self.assertIn('AIG', buffer.getvalue())

    def test_main_uses_shared_dk_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '06180',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'mean_temp',
                'observation_date': '2024-01-01',
                'time_function': None,
                'value': 3.5,
                'flag': '{"qcStatus":"manual","validity":true}',
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])
        buffer = io.StringIO()
        with patch.object(download_daily, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_daily.py', '--country', 'DK']):
                with redirect_stdout(buffer):
                    download_daily.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'DK')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.station_ids, ['06180'])
        self.assertEqual(query.elements, ['mean_temp', 'acc_precip', 'bright_sunshine'])
        self.assertIn('06180', buffer.getvalue())

    def test_main_uses_shared_pl_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '00375',
                'gh_id': '352200375',
                'element': 'tas_mean',
                'element_raw': 'STD',
                'observation_date': '2025-01-01',
                'time_function': None,
                'value': 3.8,
                'flag': None,
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])
        buffer = io.StringIO()
        with patch.object(download_daily, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_daily.py', '--country', 'PL']):
                with redirect_stdout(buffer):
                    download_daily.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'PL')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.station_ids, ['00375'])
        self.assertEqual(query.elements, ['STD', 'SMDB', 'USL'])
        self.assertIn('00375', buffer.getvalue())

    def test_main_uses_shared_se_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '98230',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': '2',
                'observation_date': '1996-10-01',
                'time_function': None,
                'value': 11.1,
                'flag': 'Y',
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])
        buffer = io.StringIO()
        with patch.object(download_daily, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_daily.py', '--country', 'SE']):
                with redirect_stdout(buffer):
                    download_daily.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'SE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.station_ids, ['98230'])
        self.assertEqual(query.elements, ['2', '20', '5'])
        self.assertIn('98230', buffer.getvalue())


if __name__ == '__main__':
    unittest.main()
