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
    def test_build_parser_accepts_country_be(self) -> None:
        parser = download_tenmin.build_parser()
        args = parser.parse_args(['--country', 'BE'])
        self.assertEqual(args.country, 'BE')

    def test_main_uses_shared_be_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '6414',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'temp_dry_shelter_avg',
                'timestamp': '2024-01-01T00:10:00Z',
                'value': 4.15,
                'flag': '{"validated":{"TEMP_DRY_SHELTER_AVG":true}}',
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': '10min',
            }
        ])
        buffer = io.StringIO()
        with patch.object(download_tenmin, 'download_observations', return_value=sample) as download_mock:
            with patch.object(sys, 'argv', ['download_tenmin.py', '--country', 'BE']):
                with redirect_stdout(buffer):
                    download_tenmin.main()
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'BE')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.station_ids, ['6414'])
        self.assertEqual(query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertIn('6414', buffer.getvalue())


if __name__ == '__main__':
    unittest.main()
