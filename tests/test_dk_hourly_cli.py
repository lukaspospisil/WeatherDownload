import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import pandas as pd

from weatherdownload.cli import main


class DenmarkHourlyCliTests(unittest.TestCase):
    def test_hourly_cli_explicit_country_dk_uses_dk_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '06180',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'mean_temp',
                'timestamp': '2024-01-01T01:00:00Z',
                'value': 2.8,
                'flag': '{"qcStatus":"manual","validity":true}',
                'quality': None,
                'dataset_scope': 'historical',
                'resolution': '1hour',
            }
        ])
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=sample) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'hourly', '--country', 'DK', '--station-id', '06180', '--element', 'tas_mean', '--start', '2024-01-01T01:00:00Z', '--end', '2024-01-01T02:00:00Z'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'DK')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.elements, ['mean_temp'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'DK')
        self.assertIn('06180', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())


if __name__ == '__main__':
    unittest.main()
