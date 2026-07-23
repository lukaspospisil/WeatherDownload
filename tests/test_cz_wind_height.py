from __future__ import annotations

import unittest

import pandas as pd

from weatherdownload.cz_wind_height import (
    FALLBACK_HEIGHT_SOURCE,
    METADATA_HEIGHT_SOURCE,
    audit_and_standardize_cz_daily_wind,
    build_cz_daily_wind_height_lookup,
)


def _meta2_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    base_columns = ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height']
    return pd.DataFrame.from_records(rows, columns=base_columns)


class CzWindHeightTests(unittest.TestCase):
    def test_matches_simple_10m_observation(self) -> None:
        meta2 = _meta2_frame([
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2000-01-01T00:00Z',
                'end_date': '2025-12-31T23:59Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 10.0,
            }
        ])
        observations = pd.DataFrame([{'station_id': 'A', 'date': '2024-01-15', 'wind_speed': 4.0}])

        audit = audit_and_standardize_cz_daily_wind(observations, build_cz_daily_wind_height_lookup(meta2))

        self.assertEqual(int(audit.loc[0, 'matched_metadata_rows']), 1)
        self.assertEqual(float(audit.loc[0, 'wind_measurement_height_m']), 10.0)
        self.assertEqual(str(audit.loc[0, 'wind_height_source']), METADATA_HEIGHT_SOURCE)
        self.assertEqual(str(audit.loc[0, 'wind_height_issue']), '')

    def test_matches_non_10m_observation(self) -> None:
        meta2 = _meta2_frame([
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2000-01-01T00:00Z',
                'end_date': '2025-12-31T23:59Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 14.53,
            }
        ])
        observations = pd.DataFrame([{'station_id': 'A', 'date': '2024-01-15', 'wind_speed': 4.0}])

        audit = audit_and_standardize_cz_daily_wind(observations, build_cz_daily_wind_height_lookup(meta2))

        self.assertEqual(float(audit.loc[0, 'wind_measurement_height_m']), 14.53)
        self.assertEqual(str(audit.loc[0, 'wind_height_source']), METADATA_HEIGHT_SOURCE)
        self.assertGreater(float(audit.loc[0, 'u2_abs_diff_m_s']), 0.0)

    def test_handles_height_change_within_station(self) -> None:
        meta2 = _meta2_frame([
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2000-01-01T00:00Z',
                'end_date': '2010-12-31T23:59Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 10.0,
            },
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2011-01-01T00:00Z',
                'end_date': '2025-12-31T23:59Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 12.0,
            },
        ])
        observations = pd.DataFrame(
            [
                {'station_id': 'A', 'date': '2010-12-31', 'wind_speed': 4.0},
                {'station_id': 'A', 'date': '2011-01-01', 'wind_speed': 4.0},
            ]
        )

        audit = audit_and_standardize_cz_daily_wind(observations, build_cz_daily_wind_height_lookup(meta2))

        self.assertEqual(list(audit['wind_measurement_height_m']), [10.0, 12.0])

    def test_respects_open_ended_metadata_validity(self) -> None:
        meta2 = _meta2_frame([
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2018-01-01T00:00Z',
                'end_date': '3999-12-31T23:59Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 10.15,
            }
        ])
        observations = pd.DataFrame([{'station_id': 'A', 'date': '2025-12-31', 'wind_speed': 4.0}])

        audit = audit_and_standardize_cz_daily_wind(observations, build_cz_daily_wind_height_lookup(meta2))

        self.assertEqual(float(audit.loc[0, 'wind_measurement_height_m']), 10.15)
        self.assertEqual(str(audit.loc[0, 'wind_height_source']), METADATA_HEIGHT_SOURCE)

    def test_flags_overlapping_metadata_with_conflicting_heights(self) -> None:
        meta2 = _meta2_frame([
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2010-02-19T00:00Z',
                'end_date': '2010-02-19T11:00Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 10.0,
            },
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2010-02-19T11:01Z',
                'end_date': '2010-12-31T23:59Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 12.0,
            },
        ])
        observations = pd.DataFrame([{'station_id': 'A', 'date': '2010-02-19', 'wind_speed': 4.0}])

        audit = audit_and_standardize_cz_daily_wind(observations, build_cz_daily_wind_height_lookup(meta2))

        self.assertEqual(int(audit.loc[0, 'matched_metadata_rows']), 2)
        self.assertTrue(bool(audit.loc[0, 'matched_height_conflict']))
        self.assertEqual(str(audit.loc[0, 'wind_height_source']), FALLBACK_HEIGHT_SOURCE)
        self.assertEqual(str(audit.loc[0, 'wind_height_issue']), 'overlap_conflict')
        self.assertEqual(float(audit.loc[0, 'wind_measurement_height_m']), 10.0)

    def test_uses_fallback_when_metadata_is_missing(self) -> None:
        observations = pd.DataFrame([{'station_id': 'A', 'date': '2024-01-15', 'wind_speed': 4.0}])

        audit = audit_and_standardize_cz_daily_wind(observations, build_cz_daily_wind_height_lookup(_meta2_frame([])))

        self.assertEqual(int(audit.loc[0, 'matched_metadata_rows']), 0)
        self.assertEqual(str(audit.loc[0, 'wind_height_source']), FALLBACK_HEIGHT_SOURCE)
        self.assertEqual(str(audit.loc[0, 'wind_height_issue']), 'no_metadata')
        self.assertEqual(float(audit.loc[0, 'wind_measurement_height_m']), 10.0)

    def test_height_zero_never_enters_log_conversion(self) -> None:
        meta2 = _meta2_frame([
            {
                'obs_type': 'DLY',
                'station_id': 'A',
                'begin_date': '2000-01-01T00:00Z',
                'end_date': '2025-12-31T23:59Z',
                'element': 'F',
                'schedule': 'AVG',
                'name': 'Wind',
                'description': 'm/s',
                'height': 0.0,
            }
        ])
        observations = pd.DataFrame([{'station_id': 'A', 'date': '2024-01-15', 'wind_speed': 4.0}])

        audit = audit_and_standardize_cz_daily_wind(observations, build_cz_daily_wind_height_lookup(meta2))

        self.assertTrue(bool(audit.loc[0, 'matched_zero_height']))
        self.assertEqual(str(audit.loc[0, 'wind_height_source']), FALLBACK_HEIGHT_SOURCE)
        self.assertEqual(str(audit.loc[0, 'wind_height_issue']), 'height_zero_only')
        self.assertEqual(float(audit.loc[0, 'wind_measurement_height_m']), 10.0)


if __name__ == '__main__':
    unittest.main()
