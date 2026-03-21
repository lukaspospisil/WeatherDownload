import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from weatherdownload import export_table
from weatherdownload.exporting import resolve_output_path


class ExporterTests(unittest.TestCase):
    def _sample_table(self) -> pd.DataFrame:
        return pd.DataFrame([{'station_id': '0-20000-0-11406', 'gh_id': 'L3CHEB01', 'begin_date': '2001-01-01T00:00Z', 'end_date': '3999-12-31T23:59Z', 'full_name': 'Cheb', 'longitude': 12.391389, 'latitude': 50.068333, 'elevation_m': 483.0}])

    def test_resolve_output_path_uses_outputs_for_bare_filename(self) -> None:
        self.assertEqual(resolve_output_path('stations.csv'), Path('outputs/stations.csv'))

    def test_resolve_output_path_keeps_relative_path_with_directory(self) -> None:
        self.assertEqual(resolve_output_path(Path('reports/stations.csv')), Path('reports/stations.csv'))

    def test_resolve_output_path_keeps_absolute_path(self) -> None:
        absolute = Path(tempfile.gettempdir()) / 'stations.csv'
        self.assertEqual(resolve_output_path(absolute), absolute)

    def test_export_csv_creates_file_under_outputs_for_bare_filename(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                destination = export_table(self._sample_table(), 'stations.csv', format='csv')
                self.assertEqual(destination, Path('outputs/stations.csv'))
                self.assertTrue(destination.exists())
                self.assertIn('0-20000-0-11406', destination.read_text(encoding='utf-8'))
            finally:
                os.chdir(original_cwd)

    def test_export_csv_creates_missing_parent_directories(self) -> None:
        original_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            try:
                destination = export_table(self._sample_table(), Path('nested/reports/stations.csv'), format='csv')
                self.assertEqual(destination, Path('nested/reports/stations.csv'))
                self.assertTrue(destination.exists())
            finally:
                os.chdir(original_cwd)


if __name__ == '__main__':
    unittest.main()
