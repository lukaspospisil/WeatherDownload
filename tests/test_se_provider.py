import unittest
from pathlib import Path

import pandas as pd
from unittest.mock import patch

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

FIXTURE_DIR = Path('tests/data/smhi_se')
EXPECTED_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
EXPECTED_MAPPING = {
    'tas_mean': '2',
    'tas_max': '20',
    'tas_min': '19',
    'precipitation': '5',
}


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class SwedenProviderTests(unittest.TestCase):
    def test_supported_countries_include_se(self) -> None:
        self.assertIn('SE', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='SE'), ['historical'])
        self.assertEqual(list_resolutions(country='SE', dataset_scope='historical'), ['daily'])

    def test_read_station_metadata_country_se_from_local_fixture_dir(self) -> None:
        stations = read_station_metadata(country='SE', source_url=str(FIXTURE_DIR))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['98230'])
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_se_from_local_fixture_dir(self) -> None:
        metadata = read_station_observation_metadata(country='SE', source_url=str(FIXTURE_DIR))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(sorted(metadata['element'].tolist()), ['19', '2', '20', '5'])
        self.assertTrue(metadata['height'].isna().all())
        self.assertTrue(metadata['obs_type'].eq('HISTORICAL_DAILY').all())

    def test_discovery_country_se_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='SE', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='SE', dataset_scope='historical', resolution='daily', provider_raw=True),
            ['2', '20', '19', '5'],
        )

    def test_se_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='SE', dataset_scope='historical', resolution='daily', station_ids=['98230'], start_date='1996-10-01', end_date='1996-10-03', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='SE', dataset_scope='historical', resolution='daily', station_ids=['98230'], start_date='1996-10-01', end_date='1996-10-03', elements=['2', '5'])
        self.assertEqual(canonical_query.elements, ['2', '5'])
        self.assertEqual(raw_query.elements, ['2', '5'])

    def test_download_daily_observations_country_se_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='SE', source_url=str(FIXTURE_DIR))

        def fake_get(url, timeout=60):
            if '/parameter/2/' in url:
                return _MockResponse((FIXTURE_DIR / 'daily_parameter_2.csv').read_text(encoding='utf-8'))
            if '/parameter/5/' in url:
                return _MockResponse((FIXTURE_DIR / 'daily_parameter_5.csv').read_text(encoding='utf-8'))
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='SE', dataset_scope='historical', resolution='daily', station_ids=['98230'], start_date='1996-10-01', end_date='1996-10-02', elements=['tas_mean', 'precipitation'])
        with patch('weatherdownload.se_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='SE', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), EXPECTED_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['2', '5'])
        self.assertEqual(sorted(observations['flag'].dropna().unique().tolist()), ['G', 'Y'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertEqual(str(observations['quality'].dtype), 'Int64')

    def test_se_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='SE', source_url=str(FIXTURE_DIR))

        def fake_get(url, timeout=60):
            if '/parameter/2/' in url:
                return _MockResponse((FIXTURE_DIR / 'daily_parameter_2.csv').read_text(encoding='utf-8'))
            if '/parameter/5/' in url:
                return _MockResponse((FIXTURE_DIR / 'daily_parameter_5.csv').read_text(encoding='utf-8'))
            if '/parameter/19/' in url:
                return _MockResponse((FIXTURE_DIR / 'daily_parameter_19.csv').read_text(encoding='utf-8'))
            if '/parameter/20/' in url:
                return _MockResponse((FIXTURE_DIR / 'daily_parameter_20.csv').read_text(encoding='utf-8'))
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(country='SE', dataset_scope='historical', resolution='daily', station_ids=['98230'], start_date='1996-10-01', end_date='1996-10-03', elements=list(EXPECTED_MAPPING.keys()))
        with patch('weatherdownload.se_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='SE', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_MAPPING)
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('1996-10-01').date())]), 11.1)
        self.assertAlmostEqual(float(lookup[('tas_max', pd.Timestamp('1996-10-02').date())]), 11.1)


if __name__ == '__main__':
    unittest.main()

