import io
import unittest
from unittest.mock import patch

import pandas as pd

from weatherdownload.cli import main


class GeosphereCliTests(unittest.TestCase):
    def test_daily_cli_country_at_uses_historical_dataset_scope(self) -> None:
        captured: dict[str, object] = {}

        def fake_download(query, country=None):
            captured['query'] = query
            captured['country'] = country
            return pd.DataFrame([
                {
                    'station_id': '1',
                    'gh_id': pd.NA,
                    'element': 'tas_mean',
                    'element_raw': 'tl_mittel',
                    'observation_date': pd.Timestamp('2024-01-01').date(),
                    'time_function': pd.NA,
                    'value': 2.2,
                    'flag': pd.NA,
                    'quality': 20,
                    'dataset_scope': 'historical',
                    'resolution': 'daily',
                }
            ])

        with patch('weatherdownload.cli.download_observations', side_effect=fake_download):
            with patch('sys.stdout', new_callable=io.StringIO) as stdout:
                exit_code = main([
                    'observations',
                    'daily',
                    '--country', 'AT',
                    '--station-id', '1',
                    '--element', 'tas_mean',
                    '--start-date', '2024-01-01',
                    '--end-date', '2024-01-01',
                ])

        self.assertEqual(exit_code, 0)
        query = captured['query']
        self.assertEqual(query.country, 'AT')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.elements, ['tl_mittel'])
        self.assertEqual(captured['country'], 'AT')
        self.assertIn('tas_mean', stdout.getvalue())


if __name__ == '__main__':
    unittest.main()
