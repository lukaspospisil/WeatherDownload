import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload.cli import main


class DailyCliTests(unittest.TestCase):
    def _sample_daily_table(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'gh_id': 'L3CHEB01',
                'element': 'TMA',
                'observation_date': '1865-06-01',
                'time_function': '20:00',
                'value': 21.0,
                'flag': None,
                'quality': 0,
                'dataset_scope': 'historical_csv',
                'resolution': 'daily',
            }
        ])

    def test_daily_cli_screen_output(self) -> None:
        buffer = io.StringIO()
        with patch('weatherdownload.cli.download_observations', return_value=self._sample_daily_table()):
            with redirect_stdout(buffer):
                exit_code = main(['observations', 'daily', '--station-id', '0-20000-0-11406', '--element', 'TMA', '--start-date', '1865-06-01', '--end-date', '1865-06-03'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('0-20000-0-11406', output)
        self.assertIn('TMA', output)

    def test_daily_cli_csv_export_uses_outputs_for_bare_filename(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                buffer = io.StringIO()
                with patch('weatherdownload.cli.download_observations', return_value=self._sample_daily_table()):
                    with redirect_stdout(buffer):
                        exit_code = main(['observations', 'daily', '--station-id', '0-20000-0-11406', '--element', 'TMA', '--start-date', '1865-06-01', '--end-date', '1865-06-03', '--format', 'csv', '--output', 'daily.csv'])
                self.assertEqual(exit_code, 0)
                output_path = Path('outputs/daily.csv')
                self.assertTrue(output_path.exists())
                self.assertIn('0-20000-0-11406', output_path.read_text(encoding='utf-8'))
                self.assertIn('Exported daily observations to outputs', buffer.getvalue())
            finally:
                os.chdir(original_cwd)

    def test_daily_cli_csv_export_keeps_explicit_relative_path(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                output_path = Path('reports/daily.csv')
                with patch('weatherdownload.cli.download_observations', return_value=self._sample_daily_table()):
                    exit_code = main(['observations', 'daily', '--station-id', '0-20000-0-11406', '--element', 'TMA', '--start-date', '1865-06-01', '--end-date', '1865-06-03', '--format', 'csv', '--output', str(output_path)])
                self.assertEqual(exit_code, 0)
                self.assertTrue(output_path.exists())
            finally:
                os.chdir(original_cwd)


if __name__ == '__main__':
    unittest.main()
