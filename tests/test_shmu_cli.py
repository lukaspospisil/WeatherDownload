import io
import unittest
from unittest.mock import patch

import pandas as pd

from weatherdownload.cli import main


class ShmuCliTests(unittest.TestCase):
    def test_daily_cli_country_sk_uses_recent_dataset_scope(self) -> None:
        captured: dict[str, object] = {}

        def fake_download(query, country=None):
            captured['query'] = query
            captured['country'] = country
            return pd.DataFrame([
                {
                    'station_id': '11800',
                    'gh_id': pd.NA,
                    'element': 'tas_max',
                    'element_raw': 't_max',
                    'observation_date': pd.Timestamp('2025-01-01').date(),
                    'time_function': pd.NA,
                    'value': 5.2,
                    'flag': pd.NA,
                    'quality': pd.NA,
                    'dataset_scope': 'recent',
                    'resolution': 'daily',
                }
            ])

        with patch('weatherdownload.cli.download_observations', side_effect=fake_download):
            with patch('sys.stdout', new_callable=io.StringIO) as stdout:
                exit_code = main([
                    'observations',
                    'daily',
                    '--country', 'SK',
                    '--station-id', '11800',
                    '--element', 'tas_max',
                    '--start-date', '2025-01-01',
                    '--end-date', '2025-01-01',
                ])

        self.assertEqual(exit_code, 0)
        query = captured['query']
        self.assertEqual(query.country, 'SK')
        self.assertEqual(query.dataset_scope, 'recent')
        self.assertEqual(query.resolution, 'daily')
        self.assertEqual(query.elements, ['t_max'])
        self.assertEqual(captured['country'], 'SK')
        self.assertIn('tas_max', stdout.getvalue())


if __name__ == '__main__':
    unittest.main()
