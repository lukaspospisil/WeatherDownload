import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

MODULE_PATH = Path('examples/download_daily.py')
SPEC = importlib.util.spec_from_file_location('download_daily_example', MODULE_PATH)
download_daily = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = download_daily
assert SPEC.loader is not None
SPEC.loader.exec_module(download_daily)


class DownloadDailyExampleTests(unittest.TestCase):
    def test_build_parser_accepts_country_be(self) -> None:
        parser = download_daily.build_parser()
        args = parser.parse_args(['--country', 'BE'])
        self.assertEqual(args.country, 'BE')

    def test_main_uses_shared_be_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '6414',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'temp_avg',
                'observation_date': '2024-01-01',
                'time_function': None,
                'value': 4.2,
                'flag': '{"validated":{"TEMP_AVG":true}}',
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': 'daily',
            }
        ])
        buffer = io.StringIO()
        with patch.object(download_daily, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_daily.py', '--country', 'BE']):
                with redirect_stdout(buffer):
                    download_daily.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'BE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.station_ids, ['6414'])
        self.assertEqual(query.elements, ['temp_avg', 'precip_quantity', 'sun_duration'])
        self.assertIn('6414', buffer.getvalue())


if __name__ == '__main__':
    unittest.main()

