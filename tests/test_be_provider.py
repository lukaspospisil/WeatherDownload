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
from weatherdownload.providers.be.daily import RMI_AWS_WFS_URL

SAMPLE_STATIONS_PATH = Path('tests/data/sample_be_aws_station.json')
SAMPLE_DAILY_TEXT = Path('tests/data/sample_be_aws_1day.json').read_text(encoding='utf-8')
SAMPLE_HOURLY_TEXT = Path('tests/data/sample_be_aws_1hour.json').read_text(encoding='utf-8')
SAMPLE_TENMIN_TEXT = Path('tests/data/sample_be_aws_10min.json').read_text(encoding='utf-8')

EXPECTED_BE_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
EXPECTED_BE_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
EXPECTED_BE_DAILY_CANONICAL_MAPPING = {
    'tas_mean': 'temp_avg',
    'tas_max': 'temp_max',
    'tas_min': 'temp_min',
    'precipitation': 'precip_quantity',
    'wind_speed': 'wind_speed_10m',
    'relative_humidity': 'humidity_rel_shelter_avg',
    'pressure': 'pressure',
    'sunshine_duration': 'sun_duration',
}
EXPECTED_BE_SUBDAILY_CANONICAL_MAPPING = {
    'tas_mean': 'temp_dry_shelter_avg',
    'precipitation': 'precip_quantity',
    'wind_speed': 'wind_speed_10m',
    'relative_humidity': 'humidity_rel_shelter_avg',
    'pressure': 'pressure',
    'sunshine_duration': 'sun_duration',
}
EXPECTED_DAILY_RAW_QC_FLAG = '{"validated":{"PRECIP_QUANTITY":true,"TEMP_AVG":true}}'
EXPECTED_HOURLY_RAW_QC_FLAG = '{"validated":{"TEMP_DRY_SHELTER_AVG":true,"PRESSURE":true}}'
EXPECTED_TENMIN_RAW_QC_FLAG = '{"validated":{"TEMP_DRY_SHELTER_AVG":true,"PRESSURE":true}}'


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class BelgiumProviderTests(unittest.TestCase):
    def test_supported_countries_include_be(self) -> None:
        self.assertIn('BE', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='BE'), ['historical'])
        self.assertEqual(list_resolutions(country='BE', dataset_scope='historical'), ['10min', '1hour', 'daily'])

    def test_read_station_metadata_country_be_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['6414', '6438'])
        self.assertTrue(stations['gh_id'].isna().all())

    def test_be_station_metadata_keeps_only_source_backed_fields(self) -> None:
        stations = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))
        first = stations.iloc[0]
        self.assertEqual(first['station_id'], '6414')
        self.assertEqual(first['full_name'], 'BEITEM')
        self.assertAlmostEqual(float(first['longitude']), 3.122)
        self.assertAlmostEqual(float(first['latitude']), 50.904)
        self.assertAlmostEqual(float(first['elevation_m']), 24.8)
        self.assertEqual(first['begin_date'], '2003-07-26T00:10Z')
        self.assertEqual(first['end_date'], '')
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_be_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertIn('temp_avg', metadata['element'].tolist())
        self.assertIn('temp_dry_shelter_avg', metadata['element'].tolist())
        self.assertIn('HISTORICAL_HOURLY', metadata['obs_type'].tolist())
        self.assertIn('HISTORICAL_10MIN', metadata['obs_type'].tolist())

    def test_discovery_country_be_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='BE', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='BE', dataset_scope='historical', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='BE', dataset_scope='historical', resolution='10min'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )

    def test_be_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='daily', station_ids=['6414'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='daily', station_ids=['6414'], start_date='2024-01-01', end_date='2024-01-02', elements=['temp_avg', 'precip_quantity'])
        self.assertEqual(canonical_query.elements, ['temp_avg', 'precip_quantity'])
        self.assertEqual(raw_query.elements, ['temp_avg', 'precip_quantity'])

    def test_be_hourly_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        raw_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(canonical_query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(raw_query.elements, ['temp_dry_shelter_avg', 'pressure'])

    def test_be_tenmin_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        raw_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(canonical_query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(raw_query.elements, ['temp_dry_shelter_avg', 'pressure'])

    def test_download_daily_observations_country_be_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            if url == RMI_AWS_WFS_URL:
                self.assertIn('code = 6414', params['cql_filter'])
                self.assertIn("timestamp >= '2024-01-01T00:00:00Z'", params['cql_filter'])
                self.assertEqual(params['typeName'], 'aws:aws_1day')
                return _MockResponse(SAMPLE_DAILY_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='daily', station_ids=['6414'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        with patch('weatherdownload.providers.be.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='BE', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), EXPECTED_BE_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_mean'])
        self.assertEqual(observations.iloc[0]['flag'], EXPECTED_DAILY_RAW_QC_FLAG)
        self.assertEqual(str(observations['quality'].dtype), 'Int64')

    def test_download_hourly_observations_country_be_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            if url == RMI_AWS_WFS_URL:
                self.assertIn('code = 6414', params['cql_filter'])
                self.assertIn("timestamp >= '2024-01-01T01:00:00Z'", params['cql_filter'])
                self.assertEqual(params['typeName'], 'aws:aws_1hour')
                return _MockResponse(SAMPLE_HOURLY_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.providers.be.hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='BE', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), EXPECTED_BE_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['pressure', 'temp_dry_shelter_avg'])
        self.assertEqual(observations.iloc[0]['flag'], EXPECTED_HOURLY_RAW_QC_FLAG)
        self.assertEqual(str(observations.iloc[0]['timestamp']), '2024-01-01 01:00:00+00:00')

    def test_download_tenmin_observations_country_be_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            if url == RMI_AWS_WFS_URL:
                self.assertIn('code = 6414', params['cql_filter'])
                self.assertIn("timestamp >= '2024-01-01T00:10:00Z'", params['cql_filter'])
                self.assertEqual(params['typeName'], 'aws:aws_10min')
                return _MockResponse(SAMPLE_TENMIN_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.providers.be.tenmin.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='BE', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), EXPECTED_BE_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean'])
        self.assertEqual(observations.iloc[0]['flag'], EXPECTED_TENMIN_RAW_QC_FLAG)
        self.assertEqual(str(observations.iloc[0]['timestamp']), '2024-01-01 00:10:00+00:00')

    def test_be_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='daily', station_ids=['6414'], start_date='2024-01-01', end_date='2024-01-02', elements=list(EXPECTED_BE_DAILY_CANONICAL_MAPPING.keys()))
        with patch('weatherdownload.providers.be.daily.requests.get', return_value=_MockResponse(SAMPLE_DAILY_TEXT)):
            observations = download_observations(query, country='BE', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_BE_DAILY_CANONICAL_MAPPING)
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01').date())]), 4.2)

    def test_be_hourly_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=list(EXPECTED_BE_SUBDAILY_CANONICAL_MAPPING.keys()))
        with patch('weatherdownload.providers.be.hourly.requests.get', return_value=_MockResponse(SAMPLE_HOURLY_TEXT)):
            observations = download_observations(query, country='BE', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_BE_SUBDAILY_CANONICAL_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01T01:00:00Z'))]), 4.1)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2024-01-01T02:00:00Z'))]), 1008.1)

    def test_be_tenmin_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=list(EXPECTED_BE_SUBDAILY_CANONICAL_MAPPING.keys()))
        with patch('weatherdownload.providers.be.tenmin.requests.get', return_value=_MockResponse(SAMPLE_TENMIN_TEXT)):
            observations = download_observations(query, country='BE', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_BE_SUBDAILY_CANONICAL_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01T00:10:00Z'))]), 4.15)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2024-01-01T00:20:00Z'))]), 1008.3)


if __name__ == '__main__':
    unittest.main()

