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
    def test_build_parser_accepts_country_at(self) -> None:
        parser = download_tenmin.build_parser()
        args = parser.parse_args(['--country', 'AT'])
        self.assertEqual(args.country, 'AT')

    def test_build_parser_accepts_country_dk(self) -> None:
        parser = download_tenmin.build_parser()
        args = parser.parse_args(['--country', 'DK'])
        self.assertEqual(args.country, 'DK')

    def test_main_uses_shared_at_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '1',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'tl',
                'timestamp': '2024-01-01T00:10:00Z',
                'value': 0.1,
                'flag': '12',
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': '10min',
            }
        ])
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

    def test_main_uses_shared_dk_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '06180',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'temp_dry',
                'timestamp': '2024-01-01T00:10:00Z',
                'value': 2.1,
                'flag': None,
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': '10min',
            }
        ])
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


if __name__ == '__main__':
    unittest.main()


