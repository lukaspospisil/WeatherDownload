from __future__ import annotations

import unittest
from pathlib import Path
import tempfile

import pandas as pd
from scipy.io import loadmat

from examples.workflows import build_cz_fao_wind_height_candidate as workflow


class BuildCzFaoWindHeightCandidateTests(unittest.TestCase):
    def test_build_summary_reports_expected_counts(self) -> None:
        candidate = pd.DataFrame(
            [
                {
                    'station_id': 'A',
                    'date': '2024-01-01',
                    'matched_height_m': 10.0,
                    'matched_zero_height': False,
                    'matched_metadata_rows': 1,
                    'wind_height_source': 'metadata',
                    'wind_height_issue': '',
                    'u2_abs_diff_m_s': 0.0,
                    'u2_rel_diff': 0.0,
                },
                {
                    'station_id': 'A',
                    'date': '2024-01-02',
                    'matched_height_m': 12.0,
                    'matched_zero_height': False,
                    'matched_metadata_rows': 2,
                    'wind_height_source': 'fallback_10m',
                    'wind_height_issue': 'overlap_conflict',
                    'u2_abs_diff_m_s': 0.2,
                    'u2_rel_diff': 0.1,
                },
                {
                    'station_id': 'B',
                    'date': '2024-01-01',
                    'matched_height_m': float('nan'),
                    'matched_zero_height': True,
                    'matched_metadata_rows': 1,
                    'wind_height_source': 'fallback_10m',
                    'wind_height_issue': 'height_zero_only',
                    'u2_abs_diff_m_s': 0.0,
                    'u2_rel_diff': 0.0,
                },
            ]
        )

        summary = workflow.build_summary(candidate, fallback_height_m=10.0)

        self.assertEqual(summary['num_stations'], 2)
        self.assertEqual(summary['num_wind_observations'], 3)
        self.assertEqual(summary['height_10_count'], 1)
        self.assertEqual(summary['height_non10_count'], 1)
        self.assertEqual(summary['height_zero_count'], 1)
        self.assertEqual(summary['overlap_count'], 1)
        self.assertEqual(summary['fallback_count'], 2)

    def test_build_candidate_series_records_includes_corrected_wind_fields(self) -> None:
        candidate = pd.DataFrame(
            [
                {
                    'station_id': 'A',
                    'full_name': 'Alpha',
                    'latitude': 50.0,
                    'longitude': 14.0,
                    'elevation_m': 300.0,
                    'date': pd.Timestamp('2024-01-01').date(),
                    'tas_mean': 1.0,
                    'tas_max': 2.0,
                    'tas_min': 0.0,
                    'wind_speed': 3.0,
                    'wind_speed_raw_m_s': 3.0,
                    'vapour_pressure': 7.0,
                    'sunshine_duration': 1.0,
                    'wind_measurement_height_m': 12.0,
                    'wind_height_source': 'metadata',
                    'u2_m_s': 2.8,
                }
            ]
        )
        station_rows = [
            {
                'station_id': 'A',
                'full_name': 'Alpha',
                'latitude': 50.0,
                'longitude': 14.0,
                'elevation_m': 300.0,
                'num_complete_days': 1,
                'first_complete_date': '2024-01-01',
                'last_complete_date': '2024-01-01',
            }
        ]

        records = workflow.build_candidate_series_records(candidate, station_rows=station_rows)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['wind_speed_raw_m_s'], [3.0])
        self.assertEqual(records[0]['wind_measurement_height_m'], [12.0])
        self.assertEqual(records[0]['wind_height_source'], ['metadata'])
        self.assertEqual(records[0]['u2_m_s'], [2.8])

    def test_candidate_mat_bundle_writes_corrected_wind_fields(self) -> None:
        station_rows = [
            {
                'station_id': 'A',
                'full_name': 'Alpha',
                'latitude': 50.0,
                'longitude': 14.0,
                'elevation_m': 300.0,
                'num_complete_days': 1,
                'first_complete_date': '2024-01-01',
                'last_complete_date': '2024-01-01',
            }
        ]
        candidate = pd.DataFrame(
            [
                {
                    'station_id': 'A',
                    'full_name': 'Alpha',
                    'latitude': 50.0,
                    'longitude': 14.0,
                    'elevation_m': 300.0,
                    'date': pd.Timestamp('2024-01-01').date(),
                    'tas_mean': 1.0,
                    'tas_max': 2.0,
                    'tas_min': 0.0,
                    'wind_speed': 3.0,
                    'wind_speed_raw_m_s': 3.0,
                    'vapour_pressure': 7.0,
                    'sunshine_duration': 1.0,
                    'wind_measurement_height_m': 12.0,
                    'wind_height_source': 'metadata',
                    'u2_m_s': 2.8,
                }
            ]
        )
        summary = workflow.build_summary(
            candidate.assign(
                matched_height_m=12.0,
                matched_zero_height=False,
                matched_metadata_rows=1,
                wind_height_issue='',
                u2_abs_diff_m_s=0.1,
                u2_rel_diff=0.02,
            ),
            fallback_height_m=10.0,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            mat_path = Path(tmpdir) / 'candidate.mat'
            workflow.download_fao.export_mat_bundle(
                mat_path,
                data_info=summary,
                stations=station_rows,
                series=workflow.build_candidate_series_records(candidate, station_rows=station_rows),
            )

            payload = loadmat(mat_path, squeeze_me=True, struct_as_record=False)
        self.assertIn('data_info', payload)
        self.assertIn('stations', payload)
        self.assertIn('series', payload)

        series = payload['series']
        first_series = series[0] if getattr(series, 'ndim', 0) else series
        self.assertTrue(hasattr(first_series, 'wind_speed_raw_m_s'))
        self.assertTrue(hasattr(first_series, 'wind_measurement_height_m'))
        self.assertTrue(hasattr(first_series, 'wind_height_source'))
        self.assertTrue(hasattr(first_series, 'u2_m_s'))


if __name__ == '__main__':
    unittest.main()
