import json
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_dataset_scopes,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.dk_daily import DMI_CLIMATE_STATION_VALUE_URL

SAMPLE_STATIONS_PATH = Path('tests/data/sample_dk_dmi_stations.json')
SAMPLE_DAILY_TEXT = Path('tests/data/sample_dk_dmi_daily.json').read_text(encoding='utf-8')
SAMPLE_DAILY_PAYLOAD = json.loads(SAMPLE_DAILY_TEXT)
SAMPLE_HOURLY_TEXT = Path('tests/data/sample_dk_dmi_hourly.json').read_text(encoding='utf-8')
SAMPLE_HOURLY_PAYLOAD = json.loads(SAMPLE_HOURLY_TEXT)

EXPECTED_DK_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
EXPECTED_DK_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
EXPECTED_DK_DAILY_CANONICAL_MAPPING = {
    'tas_mean': 'mean_temp',
    'tas_max': 'mean_daily_max_temp',
    'tas_min': 'mean_daily_min_temp',
    'precipitation': 'acc_precip',
    'wind_speed': 'mean_wind_speed',
    'relative_humidity': 'mean_relative_hum',
    'pressure': 'mean_pressure',
    'sunshine_duration': 'bright_sunshine',
}
EXPECTED_DK_HOURLY_CANONICAL_MAPPING = {
    'tas_mean': 'mean_temp',
    'precipitation': 'acc_precip',
    'wind_speed': 'mean_wind_speed',
    'relative_humidity': 'mean_relative_hum',
    'pressure': 'mean_pressure',
    'sunshine_duration': 'bright_sunshine',
}
EXPECTED_RAW_FLAG = '{"qcStatus":"manual","validity":true}'


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class DenmarkProviderTests(unittest.TestCase):
    def test_supported_countries_include_dk(self) -> None:
        self.assertIn('DK', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='DK'), ['historical'])
        self.assertEqual(list_resolutions(country='DK', dataset_scope='historical'), ['1hour', 'daily'])

    def test_read_station_metadata_country_dk_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='DK', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['06030', '06180'])
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_dk_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='DK', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertIn('mean_temp', metadata['element'].tolist())
        self.assertIn('bright_sunshine', metadata['element'].tolist())
        self.assertIn('HISTORICAL_HOURLY', metadata['obs_type'].tolist())

    def test_discovery_country_dk_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='DK', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='DK', dataset_scope='historical', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='DK', dataset_scope='historical', resolution='1hour', provider_raw=True),
            ['mean_temp', 'acc_precip', 'mean_wind_speed', 'mean_relative_hum', 'mean_pressure', 'bright_sunshine'],
        )

    def test_dk_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='daily', station_ids=['06180'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='daily', station_ids=['06180'], start_date='2024-01-01', end_date='2024-01-02', elements=['mean_temp', 'acc_precip'])
        self.assertEqual(canonical_query.elements, ['mean_temp', 'acc_precip'])
        self.assertEqual(raw_query.elements, ['mean_temp', 'acc_precip'])

    def test_dk_hourly_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        raw_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['mean_temp', 'mean_pressure'])
        self.assertEqual(canonical_query.elements, ['mean_temp', 'mean_pressure'])
        self.assertEqual(raw_query.elements, ['mean_temp', 'mean_pressure'])

    def test_download_daily_observations_country_dk_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='DK', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            if url != DMI_CLIMATE_STATION_VALUE_URL:
                raise AssertionError(f'unexpected url: {url}')
            self.assertEqual(params['stationId'], '06180')
            self.assertIn(params['parameterId'], {'mean_temp', 'acc_precip'})
            self.assertEqual(params['timeResolution'], 'day')
            filtered = {
                'type': 'FeatureCollection',
                'features': [
                    feature for feature in SAMPLE_DAILY_PAYLOAD['features']
                    if feature['properties'].get('stationId') == params['stationId']
                    and feature['properties'].get('parameterId') == params['parameterId']
                ],
            }
            return _MockResponse(json.dumps(filtered))

        query = ObservationQuery(country='DK', dataset_scope='historical', resolution='daily', station_ids=['06180'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        with patch('weatherdownload.dk_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DK', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), EXPECTED_DK_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_mean'])
        self.assertEqual(observations.iloc[0]['flag'], EXPECTED_RAW_FLAG)
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertEqual(str(observations['quality'].dtype), 'Int64')

    def test_download_hourly_observations_country_dk_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='DK', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            if url != DMI_CLIMATE_STATION_VALUE_URL:
                raise AssertionError(f'unexpected url: {url}')
            self.assertEqual(params['stationId'], '06180')
            self.assertIn(params['parameterId'], {'mean_temp', 'mean_pressure'})
            self.assertEqual(params['timeResolution'], 'hour')
            self.assertEqual(params['datetime'], '2024-01-01T01:00:00Z/2024-01-01T02:00:00Z')
            filtered = {
                'type': 'FeatureCollection',
                'features': [
                    feature for feature in SAMPLE_HOURLY_PAYLOAD['features']
                    if feature['properties'].get('stationId') == params['stationId']
                    and feature['properties'].get('parameterId') == params['parameterId']
                ],
            }
            return _MockResponse(json.dumps(filtered))

        query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.dk_hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DK', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), EXPECTED_DK_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['mean_pressure', 'mean_temp'])
        self.assertEqual(observations.iloc[0]['flag'], EXPECTED_RAW_FLAG)
        self.assertEqual(str(observations.iloc[0]['timestamp']), '2024-01-01 01:00:00+00:00')

    def test_dk_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='DK', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            filtered = {
                'type': 'FeatureCollection',
                'features': [
                    feature for feature in SAMPLE_DAILY_PAYLOAD['features']
                    if feature['properties'].get('stationId') == params['stationId']
                    and feature['properties'].get('parameterId') == params['parameterId']
                ],
            }
            return _MockResponse(json.dumps(filtered))

        query = ObservationQuery(country='DK', dataset_scope='historical', resolution='daily', station_ids=['06180'], start_date='2024-01-01', end_date='2024-01-01', elements=list(EXPECTED_DK_DAILY_CANONICAL_MAPPING.keys()))
        with patch('weatherdownload.dk_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DK', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_DK_DAILY_CANONICAL_MAPPING)
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01').date())]), 3.5)
        self.assertAlmostEqual(float(lookup[('sunshine_duration', pd.Timestamp('2024-01-01').date())]), 1.3)

    def test_dk_hourly_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='DK', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            filtered = {
                'type': 'FeatureCollection',
                'features': [
                    feature for feature in SAMPLE_HOURLY_PAYLOAD['features']
                    if feature['properties'].get('stationId') == params['stationId']
                    and feature['properties'].get('parameterId') == params['parameterId']
                ],
            }
            return _MockResponse(json.dumps(filtered))

        query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=list(EXPECTED_DK_HOURLY_CANONICAL_MAPPING.keys()))
        with patch('weatherdownload.dk_hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DK', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_DK_HOURLY_CANONICAL_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01T01:00:00Z'))]), 2.8)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2024-01-01T02:00:00Z'))]), 1007.4)


if __name__ == '__main__':
    unittest.main()
