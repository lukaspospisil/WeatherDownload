import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import pandas as pd

from weatherdownload.cli import main


class BelgiumHourlyCliTests(unittest.TestCase):
    def test_hourly_cli_explicit_country_be_uses_be_query_shape(self) -> None:
        sample = pd.DataFrame([
            {
                'station_id': '6414',
                'gh_id': None,
                'element': 'tas_mean',
                'element_raw': 'temp_dry_shelter_avg',
                'timestamp': '2024-01-01T01:00:00Z',
                'value': 4.1,
                'flag': '{"validated":{"TEMP_DRY_SHELTER_AVG":true}}',
                'quality': None,
                'provider': 'historical',
                'resolution': '1hour',
            }
        ])
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=sample) as download_mock:
            with redirect_stdout(buffer):
                exit_code = main([
                    'observations', 'hourly', '--country', 'BE', '--station-id', '6414', '--element', 'tas_mean', '--start', '2024-01-01T01:00:00Z', '--end', '2024-01-01T02:00:00Z'
                ])
        self.assertEqual(exit_code, 0)
        query = download_mock.call_args.args[0]
        self.assertEqual(query.country, 'BE')
        self.assertEqual(query.provider, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.elements, ['temp_dry_shelter_avg'])
        self.assertEqual(download_mock.call_args.kwargs['country'], 'BE')
        self.assertIn('6414', buffer.getvalue())
        self.assertIn('tas_mean', buffer.getvalue())


if __name__ == '__main__':
    unittest.main()
