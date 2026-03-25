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

    def test_hourly_cli_country_at_uses_historical_dataset_scope(self) -> None:
        captured: dict[str, object] = {}

        def fake_download(query, country=None):
            captured['query'] = query
            captured['country'] = country
            return pd.DataFrame([
                {
                    'station_id': '1',
                    'gh_id': pd.NA,
                    'element': 'tas_mean',
                    'element_raw': 'tl',
                    'timestamp': pd.Timestamp('2024-01-01T00:00:00Z'),
                    'value': 2.1,
                    'flag': '20',
                    'quality': pd.NA,
                    'dataset_scope': 'historical',
                    'resolution': '1hour',
                }
            ])

        with patch('weatherdownload.cli.download_observations', side_effect=fake_download):
            with patch('sys.stdout', new_callable=io.StringIO) as stdout:
                exit_code = main([
                    'observations',
                    'hourly',
                    '--country', 'AT',
                    '--station-id', '1',
                    '--element', 'tas_mean',
                    '--start', '2024-01-01T00:00:00Z',
                    '--end', '2024-01-01T01:00:00Z',
                ])

        self.assertEqual(exit_code, 0)
        query = captured['query']
        self.assertEqual(query.country, 'AT')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '1hour')
        self.assertEqual(query.elements, ['tl'])
        self.assertEqual(captured['country'], 'AT')
        self.assertIn('tas_mean', stdout.getvalue())


    def test_tenmin_cli_country_at_uses_historical_dataset_scope(self) -> None:
        captured: dict[str, object] = {}

        def fake_download(query, country=None):
            captured['query'] = query
            captured['country'] = country
            return pd.DataFrame([
                {
                    'station_id': '1',
                    'gh_id': pd.NA,
                    'element': 'tas_mean',
                    'element_raw': 'tl',
                    'timestamp': pd.Timestamp('2024-01-01T00:10:00Z'),
                    'value': 0.1,
                    'flag': '12',
                    'quality': pd.NA,
                    'dataset_scope': 'historical',
                    'resolution': '10min',
                }
            ])

        with patch('weatherdownload.cli.download_observations', side_effect=fake_download):
            with patch('sys.stdout', new_callable=io.StringIO) as stdout:
                exit_code = main([
                    'observations',
                    '10min',
                    '--country', 'AT',
                    '--station-id', '1',
                    '--element', 'tas_mean',
                    '--start', '2024-01-01T00:10:00Z',
                    '--end', '2024-01-01T00:20:00Z',
                ])

        self.assertEqual(exit_code, 0)
        query = captured['query']
        self.assertEqual(query.country, 'AT')
        self.assertEqual(query.dataset_scope, 'historical')
        self.assertEqual(query.resolution, '10min')
        self.assertEqual(query.elements, ['tl'])
        self.assertEqual(captured['country'], 'AT')
        self.assertIn('tas_mean', stdout.getvalue())
if __name__ == '__main__':
    unittest.main()

