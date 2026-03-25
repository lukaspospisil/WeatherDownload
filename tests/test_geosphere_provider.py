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
from weatherdownload.geosphere_daily import GEOSPHERE_DAILY_API_URL
from weatherdownload.geosphere_hourly import GEOSPHERE_HOURLY_API_URL
from weatherdownload.geosphere_tenmin import GEOSPHERE_TENMIN_API_URL

SAMPLE_METADATA_PATH = Path('tests/data/sample_geosphere_klima_v2_1d_metadata.json')
SAMPLE_METADATA_TEXT = SAMPLE_METADATA_PATH.read_text(encoding='utf-8')
SAMPLE_HOURLY_METADATA_PATH = Path('tests/data/sample_geosphere_klima_v2_1h_metadata.json')
SAMPLE_HOURLY_METADATA_TEXT = SAMPLE_HOURLY_METADATA_PATH.read_text(encoding='utf-8')
SAMPLE_TENMIN_METADATA_PATH = Path('tests/data/sample_geosphere_klima_v2_10min_metadata.json')
SAMPLE_TENMIN_METADATA_TEXT = SAMPLE_TENMIN_METADATA_PATH.read_text(encoding='utf-8')
SAMPLE_CSV_PATH = Path('tests/data/sample_geosphere_klima_v2_1d.csv')
SAMPLE_CSV_TEXT = SAMPLE_CSV_PATH.read_text(encoding='utf-8')
SAMPLE_HOURLY_CSV_PATH = Path('tests/data/sample_geosphere_klima_v2_1h.csv')
SAMPLE_HOURLY_CSV_TEXT = SAMPLE_HOURLY_CSV_PATH.read_text(encoding='utf-8')
SAMPLE_TENMIN_CSV_PATH = Path('tests/data/sample_geosphere_klima_v2_10min.csv')
SAMPLE_TENMIN_CSV_TEXT = SAMPLE_TENMIN_CSV_PATH.read_text(encoding='utf-8')
EXPECTED_AT_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
EXPECTED_AT_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
EXPECTED_AT_CANONICAL_MAPPING = {
    'tas_mean': 'tl_mittel',
    'tas_max': 'tlmax',
    'tas_min': 'tlmin',
    'precipitation': 'rr',
    'sunshine_duration': 'so_h',
    'wind_speed': 'vv_mittel',
    'pressure': 'p_mittel',
    'relative_humidity': 'rf_mittel',
}
EXPECTED_AT_HOURLY_MAPPING = {
    'tas_mean': 'tl',
    'precipitation': 'rr',
    'wind_speed': 'ff',
    'relative_humidity': 'rf',
    'pressure': 'p',
    'sunshine_duration': 'so',
}
EXPECTED_AT_TENMIN_MAPPING = {
    'tas_mean': 'tl',
    'precipitation': 'rr',
    'wind_speed': 'ff',
    'relative_humidity': 'rf',
    'pressure': 'p',
    'sunshine_duration': 'so',
}


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class GeosphereProviderTests(unittest.TestCase):
    def test_supported_countries_include_at(self) -> None:
        self.assertIn('AT', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='AT'), ['historical'])
        self.assertEqual(list_resolutions(country='AT', dataset_scope='historical'), ['10min', '1hour', 'daily'])

    def test_read_station_metadata_country_at_from_sample(self) -> None:
        with patch('weatherdownload.geosphere_metadata.requests.get', return_value=_MockResponse(SAMPLE_METADATA_TEXT)):
            stations = read_station_metadata(country='AT')
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['1', '2'])
        self.assertEqual(stations.iloc[0]['full_name'], 'Aflenz')
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(stations.iloc[0]['begin_date'], '1983-05-01T00:00Z')

    def test_read_station_observation_metadata_country_at_from_samples(self) -> None:
        def fake_get(url, timeout=60):
            if url.endswith('klima-v2-1d/metadata'):
                return _MockResponse(SAMPLE_METADATA_TEXT)
            if url.endswith('klima-v2-1h/metadata'):
                return _MockResponse(SAMPLE_HOURLY_METADATA_TEXT)
            if url.endswith('klima-v2-10min/metadata'):
                return _MockResponse(SAMPLE_TENMIN_METADATA_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        with patch('weatherdownload.geosphere_metadata.requests.get', side_effect=fake_get):
            observation_metadata = read_station_observation_metadata(country='AT')
        self.assertEqual(list(observation_metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertIn('tl_mittel', observation_metadata['element'].tolist())
        self.assertIn('tl', observation_metadata['element'].tolist())
        self.assertIn('HISTORICAL_DAILY', observation_metadata['obs_type'].tolist())
        self.assertIn('HISTORICAL_HOURLY', observation_metadata['obs_type'].tolist())
        self.assertIn('HISTORICAL_10MIN', observation_metadata['obs_type'].tolist())
        self.assertTrue(observation_metadata['height'].isna().all())

    def test_discovery_country_at_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='AT', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration', 'wind_speed', 'pressure', 'relative_humidity'],
        )
        self.assertEqual(
            list_supported_elements(country='AT', dataset_scope='historical', resolution='daily', provider_raw=True),
            ['tl_mittel', 'tlmax', 'tlmin', 'rr', 'so_h', 'vv_mittel', 'p_mittel', 'rf_mittel'],
        )
        self.assertEqual(
            list_supported_elements(country='AT', dataset_scope='historical', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='AT', dataset_scope='historical', resolution='1hour', provider_raw=True),
            ['tl', 'rr', 'ff', 'rf', 'p', 'so'],
        )
        self.assertEqual(
            list_supported_elements(country='AT', dataset_scope='historical', resolution='10min'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='AT', dataset_scope='historical', resolution='10min', provider_raw=True),
            ['tl', 'rr', 'ff', 'rf', 'p', 'so'],
        )

    def test_at_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='AT', dataset_scope='historical', resolution='daily', station_ids=['1'], start_date='2024-01-01', end_date='2024-01-03', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='AT', dataset_scope='historical', resolution='daily', station_ids=['1'], start_date='2024-01-01', end_date='2024-01-03', elements=['tl_mittel', 'rr'])
        self.assertEqual(canonical_query.elements, ['tl_mittel', 'rr'])
        self.assertEqual(raw_query.elements, ['tl_mittel', 'rr'])

    def test_at_hourly_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='AT', dataset_scope='historical', resolution='1hour', station_ids=['1'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        raw_query = ObservationQuery(country='AT', dataset_scope='historical', resolution='1hour', station_ids=['1'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['tl', 'p'])
        self.assertEqual(canonical_query.elements, ['tl', 'p'])
        self.assertEqual(raw_query.elements, ['tl', 'p'])

    def test_at_tenmin_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='AT', dataset_scope='historical', resolution='10min', station_ids=['1'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        raw_query = ObservationQuery(country='AT', dataset_scope='historical', resolution='10min', station_ids=['1'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tl', 'p'])
        self.assertEqual(canonical_query.elements, ['tl', 'p'])
        self.assertEqual(raw_query.elements, ['tl', 'p'])

    def test_download_daily_observations_country_at_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))

        def fake_get(url, params=None, timeout=60):
            if url == GEOSPHERE_DAILY_API_URL:
                self.assertIn(('parameters', 'tl_mittel'), params)
                self.assertIn(('parameters', 'tl_mittel_flag'), params)
                self.assertIn(('parameters', 'rr'), params)
                self.assertIn(('parameters', 'rr_flag'), params)
                return _MockResponse(SAMPLE_CSV_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='daily', station_ids=['1'], start_date='2024-01-01', end_date='2024-01-03', elements=['tas_mean', 'precipitation'])
        with patch('weatherdownload.geosphere_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='AT', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), EXPECTED_AT_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['rr', 'tl_mittel'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertEqual(observations['quality'].dropna().astype(int).tolist(), [20, 21, 22, 20, 21, 22])
        self.assertEqual(observations.iloc[0]['observation_date'].isoformat(), '2024-01-01')

    def test_download_hourly_observations_country_at_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))

        def fake_get(url, params=None, timeout=60):
            if url == GEOSPHERE_HOURLY_API_URL:
                self.assertIn(('parameters', 'tl'), params)
                self.assertIn(('parameters', 'tl_flag'), params)
                self.assertIn(('parameters', 'p'), params)
                self.assertIn(('parameters', 'p_flag'), params)
                return _MockResponse(SAMPLE_HOURLY_CSV_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='1hour', station_ids=['1'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.geosphere_hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='AT', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), EXPECTED_AT_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['p', 'tl'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertEqual(observations['flag'].dropna().tolist(), ['20', '21', '22', '20', '21', '22'])
        self.assertEqual(observations.iloc[0]['timestamp'].isoformat(), '2024-01-01T00:00:00+00:00')

    def test_download_tenmin_observations_country_at_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))

        def fake_get(url, params=None, timeout=60):
            if url == GEOSPHERE_TENMIN_API_URL:
                self.assertIn(('parameters', 'tl'), params)
                self.assertIn(('parameters', 'tl_flag'), params)
                self.assertIn(('parameters', 'p'), params)
                self.assertIn(('parameters', 'p_flag'), params)
                return _MockResponse(SAMPLE_TENMIN_CSV_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='10min', station_ids=['1'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.geosphere_tenmin.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='AT', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), EXPECTED_AT_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['p', 'tl'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertEqual(observations['flag'].dropna().tolist(), ['12', '12', '12', '12'])
        self.assertEqual(observations.iloc[0]['timestamp'].isoformat(), '2024-01-01T00:10:00+00:00')

    def test_at_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='daily', station_ids=['1'], start_date='2024-01-01', end_date='2024-01-03', elements=list(EXPECTED_AT_CANONICAL_MAPPING.keys()))
        with patch('weatherdownload.geosphere_daily.requests.get', return_value=_MockResponse(SAMPLE_CSV_TEXT)):
            observations = download_observations(query, country='AT', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_AT_CANONICAL_MAPPING)
        self.assertEqual(len(observations), 24)
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01').date())]), 2.2)
        self.assertAlmostEqual(float(lookup[('tas_max', pd.Timestamp('2024-01-03').date())]), 6.8)
        self.assertTrue(pd.isna(lookup[('sunshine_duration', pd.Timestamp('2024-01-02').date())]))

    def test_at_hourly_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='1hour', station_ids=['1'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=list(EXPECTED_AT_HOURLY_MAPPING.keys()))
        with patch('weatherdownload.geosphere_hourly.requests.get', return_value=_MockResponse(SAMPLE_HOURLY_CSV_TEXT)):
            observations = download_observations(query, country='AT', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_AT_HOURLY_MAPPING)
        self.assertEqual(len(observations), 18)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01T00:00:00Z'))]), 2.1)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2024-01-01T02:00:00Z'))]), 1011.8)
        self.assertTrue(pd.isna(lookup[('sunshine_duration', pd.Timestamp('2024-01-01T01:00:00Z'))]))

    def test_at_tenmin_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='10min', station_ids=['1'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:30:00Z', elements=list(EXPECTED_AT_TENMIN_MAPPING.keys()))
        with patch('weatherdownload.geosphere_tenmin.requests.get', return_value=_MockResponse(SAMPLE_TENMIN_CSV_TEXT)):
            observations = download_observations(query, country='AT', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_AT_TENMIN_MAPPING)
        self.assertEqual(len(observations), 18)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01T00:10:00Z'))]), 0.1)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2024-01-01T00:30:00Z'))]), 919.9)
        self.assertTrue(pd.isna(lookup[('sunshine_duration', pd.Timestamp('2024-01-01T00:20:00Z'))]))

    def test_at_daily_all_history_uses_station_coverage(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='daily', station_ids=['1'], all_history=True, elements=['tas_mean'])
        captured: dict[str, object] = {}

        def fake_get(url, params=None, timeout=60):
            captured['params'] = params
            return _MockResponse(SAMPLE_CSV_TEXT)

        with patch('weatherdownload.geosphere_daily.requests.get', side_effect=fake_get):
            download_observations(query, country='AT', station_metadata=station_metadata)
        self.assertIn(('start', '1983-05-01'), captured['params'])
        self.assertIn(('end', '2100-12-31'), captured['params'])

    def test_at_hourly_all_history_uses_station_coverage(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='1hour', station_ids=['1'], all_history=True, elements=['tas_mean'])
        captured: dict[str, object] = {}

        def fake_get(url, params=None, timeout=60):
            captured['params'] = params
            return _MockResponse(SAMPLE_HOURLY_CSV_TEXT)

        with patch('weatherdownload.geosphere_hourly.requests.get', side_effect=fake_get):
            download_observations(query, country='AT', station_metadata=station_metadata)
        start_value = dict(captured['params'])['start']
        end_value = dict(captured['params'])['end']
        self.assertTrue(str(start_value).startswith('1983-05-01T00:00:00'))
        self.assertTrue(str(end_value).startswith('2100-12-31T00:00:00'))

    def test_at_tenmin_all_history_uses_station_coverage(self) -> None:
        station_metadata = read_station_metadata(country='AT', source_url=str(SAMPLE_METADATA_PATH))
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='10min', station_ids=['1'], all_history=True, elements=['tas_mean'])
        captured: dict[str, object] = {}

        def fake_get(url, params=None, timeout=60):
            captured['params'] = params
            return _MockResponse(SAMPLE_TENMIN_CSV_TEXT)

        with patch('weatherdownload.geosphere_tenmin.requests.get', side_effect=fake_get):
            download_observations(query, country='AT', station_metadata=station_metadata)
        start_value = dict(captured['params'])['start']
        end_value = dict(captured['params'])['end']
        self.assertTrue(str(start_value).startswith('1983-05-01T00:00:00'))
        self.assertTrue(str(end_value).startswith('2100-12-31T00:00:00'))


if __name__ == '__main__':
    unittest.main()
