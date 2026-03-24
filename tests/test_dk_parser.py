import json
import unittest
from pathlib import Path

import pandas as pd

from weatherdownload.dk_hourly import normalize_hourly_observations_dk
from weatherdownload.dk_parser import (
    build_dk_flag,
    normalize_dk_observation_metadata,
    normalize_dk_station_metadata,
    observation_date_from_interval_start,
    observation_timestamp_from_interval_end,
    parse_dk_feature_collection_json,
)
from weatherdownload.dk_registry import DK_DAILY_PARAMETER_METADATA, DK_HOURLY_PARAMETER_METADATA, get_dataset_spec
from weatherdownload.dk_daily import normalize_daily_observations_dk
from weatherdownload import ObservationQuery

SAMPLE_STATIONS_TEXT = Path('tests/data/sample_dk_dmi_stations.json').read_text(encoding='utf-8')
SAMPLE_DAILY_TEXT = Path('tests/data/sample_dk_dmi_daily.json').read_text(encoding='utf-8')
SAMPLE_HOURLY_TEXT = Path('tests/data/sample_dk_dmi_hourly.json').read_text(encoding='utf-8')


class DenmarkParserTests(unittest.TestCase):
    def test_parse_dk_feature_collection_json_requires_features(self) -> None:
        payload = parse_dk_feature_collection_json(SAMPLE_STATIONS_TEXT)
        self.assertIn('features', payload)

    def test_normalize_dk_station_metadata_filters_to_denmark(self) -> None:
        stations = normalize_dk_station_metadata(parse_dk_feature_collection_json(SAMPLE_STATIONS_TEXT))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['06030', '06180'])
        self.assertTrue(stations['gh_id'].isna().all())
        first = stations.iloc[0]
        self.assertEqual(first['full_name'], 'Odense')
        self.assertAlmostEqual(float(first['longitude']), 10.6197)
        self.assertAlmostEqual(float(first['latitude']), 55.4331)

    def test_normalize_dk_observation_metadata_uses_supported_daily_parameter_ids(self) -> None:
        spec = get_dataset_spec('historical', 'daily')
        metadata = normalize_dk_observation_metadata(parse_dk_feature_collection_json(SAMPLE_STATIONS_TEXT), spec, DK_DAILY_PARAMETER_METADATA)
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertIn('mean_temp', metadata['element'].tolist())
        self.assertIn('bright_sunshine', metadata['element'].tolist())
        self.assertNotIn('04250', metadata['station_id'].tolist())
        self.assertTrue(metadata['schedule'].eq('P1D DMI climateData stationValue').all())

    def test_normalize_dk_observation_metadata_uses_supported_hourly_parameter_ids(self) -> None:
        spec = get_dataset_spec('historical', '1hour')
        metadata = normalize_dk_observation_metadata(parse_dk_feature_collection_json(SAMPLE_STATIONS_TEXT), spec, DK_HOURLY_PARAMETER_METADATA)
        self.assertIn('mean_temp', metadata['element'].tolist())
        self.assertIn('mean_pressure', metadata['element'].tolist())
        self.assertTrue(metadata['obs_type'].eq('HISTORICAL_HOURLY').all())
        self.assertTrue(metadata['schedule'].eq('PT1H DMI climateData stationValue').all())

    def test_observation_date_from_interval_start_uses_denmark_local_day(self) -> None:
        self.assertEqual(observation_date_from_interval_start('2023-12-31T23:00:00Z').isoformat(), '2024-01-01')

    def test_observation_timestamp_from_interval_end_preserves_utc_hour(self) -> None:
        self.assertEqual(str(observation_timestamp_from_interval_end('2024-01-01T01:00:00Z')), '2024-01-01 01:00:00+00:00')

    def test_build_dk_flag_preserves_raw_qc_status_and_validity(self) -> None:
        flag = build_dk_flag({'qcStatus': 'manual', 'validity': True})
        self.assertEqual(flag, '{"qcStatus":"manual","validity":true}')

    def test_normalize_daily_observations_dk_maps_canonical_elements(self) -> None:
        station_metadata = normalize_dk_station_metadata(parse_dk_feature_collection_json(SAMPLE_STATIONS_TEXT))
        query = ObservationQuery(
            country='DK',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['06180'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation'],
        )
        observations = normalize_daily_observations_dk(parse_dk_feature_collection_json(SAMPLE_DAILY_TEXT), query, station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'dataset_scope', 'resolution'])
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['acc_precip', 'mean_temp'])
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01').date())]), 3.5)
        self.assertAlmostEqual(float(lookup[('precipitation', pd.Timestamp('2024-01-02').date())]), 0.2)
        self.assertEqual(str(observations['quality'].dtype), 'Int64')

    def test_normalize_hourly_observations_dk_maps_canonical_elements(self) -> None:
        station_metadata = normalize_dk_station_metadata(parse_dk_feature_collection_json(SAMPLE_STATIONS_TEXT))
        query = ObservationQuery(
            country='DK',
            dataset_scope='historical',
            resolution='1hour',
            station_ids=['06180'],
            start='2024-01-01T01:00:00Z',
            end='2024-01-01T02:00:00Z',
            elements=['tas_mean', 'pressure'],
        )
        observations = normalize_hourly_observations_dk(parse_dk_feature_collection_json(SAMPLE_HOURLY_TEXT), query, station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), ['station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'dataset_scope', 'resolution'])
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['mean_pressure', 'mean_temp'])
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01T01:00:00Z'))]), 2.8)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2024-01-01T02:00:00Z'))]), 1007.4)
        self.assertEqual(str(observations['quality'].dtype), 'Int64')


if __name__ == '__main__':
    unittest.main()
