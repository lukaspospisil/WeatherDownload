import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
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

    def test_export_mat_sanitizes_missing_values_for_station_metadata(self) -> None:
        table = pd.DataFrame([
            {
                'station_id': '0-20000-0-11406',
                'gh_id': pd.NA,
                'begin_date': pd.Timestamp('2001-01-01T00:00:00Z'),
                'end_date': pd.NaT,
                'full_name': 'Cheb',
                'longitude': 12.391389,
                'latitude': np.nan,
                'elevation_m': np.nan,
            },
            {
                'station_id': '0-20000-0-11414',
                'gh_id': 'L3KVAL01',
                'begin_date': pd.Timestamp('1950-06-01T00:00:00Z'),
                'end_date': pd.Timestamp('1958-07-31T23:59:00Z'),
                'full_name': 'Karlovy Vary',
                'longitude': 12.9131,
                'latitude': 50.2019,
                'elevation_m': 603.0,
            },
        ])
        captured: dict[str, object] = {}

        def fake_savemat(destination: Path, payload: dict[str, object]) -> None:
            captured['destination'] = destination
            captured['payload'] = payload

        fake_scipy = types.ModuleType('scipy')
        fake_io = types.ModuleType('scipy.io')
        fake_io.savemat = fake_savemat
        fake_scipy.io = fake_io

        with tempfile.TemporaryDirectory() as temp_dir:
            destination = Path(temp_dir) / 'stations.mat'
            with patch.dict(sys.modules, {'scipy': fake_scipy, 'scipy.io': fake_io}):
                export_table(table, destination, format='mat')

        payload = captured['payload']['table']
        self.assertEqual(payload['station_id'].tolist(), ['0-20000-0-11406', '0-20000-0-11414'])
        self.assertEqual(payload['gh_id'].tolist(), ['', 'L3KVAL01'])
        self.assertEqual(payload['begin_date'].tolist(), ['2001-01-01T00:00:00+00:00', '1950-06-01T00:00:00+00:00'])
        self.assertEqual(payload['end_date'].tolist(), ['', '1958-07-31T23:59:00+00:00'])
        self.assertTrue(np.isnan(payload['latitude'][0]))
        self.assertTrue(np.isnan(payload['elevation_m'][0]))
        self.assertEqual(payload['elevation_m'][1], 603.0)


if __name__ == '__main__':
    unittest.main()
