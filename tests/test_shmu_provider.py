import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    QueryValidationError,
    download_observations,
    get_provider,
    list_dataset_scopes,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.errors import UnsupportedQueryError
from weatherdownload.providers.sk.observations import build_recent_daily_month_urls, resolve_latest_recent_daily_data_url
from weatherdownload.providers.sk.metadata import discover_recent_daily_stations_shmu
from weatherdownload.providers.sk.registry import get_dataset_spec

SAMPLE_PAYLOAD_PATH = Path('tests/data/sample_shmu_kli_inter_2025-01.json')
SAMPLE_METADATA_PATH = Path('tests/data/sample_shmu_kli_inter_metadata.json')
SAMPLE_INDEX_HTML = Path('tests/data/sample_shmu_recent_daily_index.html').read_text(encoding='utf-8')
SAMPLE_MONTH_INDEX_HTML = Path('tests/data/sample_shmu_recent_daily_month_index.html').read_text(encoding='utf-8')
SAMPLE_PAYLOAD_TEXT = SAMPLE_PAYLOAD_PATH.read_text(encoding='utf-8')
SAMPLE_METADATA_TEXT = SAMPLE_METADATA_PATH.read_text(encoding='utf-8')
SAMPLE_STATION_RANGES_PATH = Path('tests/data/sample_shmu_kli_inter_station_ranges.json')
EXPECTED_SK_DAILY_COLUMNS = [
    'station_id',
    'gh_id',
    'element',
    'element_raw',
    'observation_date',
    'time_function',
    'value',
    'flag',
    'quality',
    'dataset_scope',
    'resolution',
]
EXPECTED_SK_CANONICAL_MAPPING = {
    'tas_max': 't_max',
    'tas_min': 't_min',
    'sunshine_duration': 'sln_svit',
    'precipitation': 'zra_uhrn',
    'open_water_evaporation': 'voda_vypar',
}


class ShmuProviderTests(unittest.TestCase):
    def test_supported_countries_include_experimental_sk(self) -> None:
        self.assertIn('SK', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='SK'), ['recent'])
        self.assertEqual(list_resolutions(country='SK', dataset_scope='recent'), ['daily'])

    def test_provider_capability_metadata_is_explicit(self) -> None:
        provider = get_provider('SK')
        self.assertEqual(provider.supported_country_codes, ('SK',))
        self.assertEqual(provider.supported_dataset_scopes, ('recent',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(provider.supported_canonical_elements, ('tas_max', 'tas_min', 'sunshine_duration', 'precipitation', 'open_water_evaporation'))
        self.assertTrue(provider.experimental)

    def test_discovery_country_sk_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='SK', dataset_scope='recent', resolution='daily'),
            ['tas_max', 'tas_min', 'sunshine_duration', 'precipitation', 'open_water_evaporation'],
        )
        self.assertEqual(
            list_supported_elements(country='SK', dataset_scope='recent', resolution='daily', provider_raw=True),
            ['t_max', 't_min', 'sln_svit', 'zra_uhrn', 'voda_vypar'],
        )

    def test_invalid_dataset_scope_for_sk_fails_clearly(self) -> None:
        with self.assertRaisesRegex(
            QueryValidationError,
            r"Unsupported provider 'historical' for country 'SK'.*dataset_scope remains accepted",
        ):
            ObservationQuery(
                country='SK',
                dataset_scope='historical',
                resolution='daily',
                station_ids=['11800'],
                start_date='2025-01-01',
                end_date='2025-01-01',
                elements=['tas_max'],
            )

    def test_invalid_resolution_for_sk_fails_clearly(self) -> None:
        with self.assertRaisesRegex(
            QueryValidationError,
            r"Unsupported resolution '1hour' for provider 'recent'.*dataset_scope remains accepted",
        ):
            ObservationQuery(
                country='SK',
                dataset_scope='recent',
                resolution='1hour',
                station_ids=['11800'],
                start='2025-01-01T00:00:00Z',
                end='2025-01-01T01:00:00Z',
                elements=['tas_max'],
            )

    def test_invalid_element_for_sk_fails_clearly(self) -> None:
        with self.assertRaisesRegex(
            QueryValidationError,
            r"Unsupported elements for provider 'recent' and resolution 'daily': \['tas_mean'\].*dataset_scope remains accepted",
        ):
            ObservationQuery(
                country='SK',
                dataset_scope='recent',
                resolution='daily',
                station_ids=['11800'],
                start_date='2025-01-01',
                end_date='2025-01-01',
                elements=['tas_mean'],
            )

    def test_provider_guard_rejects_non_sk_query_deterministically(self) -> None:
        provider = get_provider('SK')
        query = SimpleNamespace(country='DE', dataset_scope='recent', resolution='daily')
        with self.assertRaisesRegex(UnsupportedQueryError, r"Experimental SHMU provider supports only country='SK'\."):
            provider.download_observations(query, 60, None)

    def test_provider_guard_rejects_non_daily_query_deterministically(self) -> None:
        provider = get_provider('SK')
        query = SimpleNamespace(country='SK', dataset_scope='recent', resolution='10min')
        with self.assertRaisesRegex(UnsupportedQueryError, r"Experimental SHMU provider supports only resolution='daily'\."):
            provider.download_observations(query, 60, None)

    def test_read_station_metadata_country_sk_from_sample_payload(self) -> None:
        stations = read_station_metadata(country='SK', source_url=str(SAMPLE_PAYLOAD_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['11800', '11999'])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertTrue(stations['full_name'].isna().all())
        self.assertTrue(stations['longitude'].isna().all())
        self.assertEqual(stations.iloc[0]['begin_date'], '2025-01-01T00:00Z')
        self.assertEqual(stations.iloc[0]['end_date'], '2025-01-02T00:00Z')

    def test_sk_station_metadata_missing_identity_fields_remain_null(self) -> None:
        stations = read_station_metadata(country='SK', source_url=str(SAMPLE_PAYLOAD_PATH))
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertTrue(stations['full_name'].isna().all())
        self.assertTrue(stations['longitude'].isna().all())
        self.assertTrue(stations['latitude'].isna().all())
        self.assertTrue(stations['elevation_m'].isna().all())

    def test_discover_recent_daily_stations_shmu_returns_minimal_station_table(self) -> None:
        stations = discover_recent_daily_stations_shmu(source_url=str(SAMPLE_PAYLOAD_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['11800', '11999'])
        self.assertEqual(stations['begin_date'].tolist(), ['2025-01-01T00:00Z', '2025-01-01T00:00Z'])
        self.assertEqual(stations['end_date'].tolist(), ['2025-01-02T00:00Z', '2025-01-02T00:00Z'])
        self.assertTrue(stations[['gh_id', 'full_name', 'longitude', 'latitude', 'elevation_m']].isna().all().all())

    def test_discover_recent_daily_stations_shmu_computes_begin_end_per_station(self) -> None:
        stations = discover_recent_daily_stations_shmu(source_url=str(SAMPLE_STATION_RANGES_PATH))
        by_station = stations.set_index('station_id')
        self.assertEqual(by_station.loc['11800', 'begin_date'], '2025-01-01T00:00Z')
        self.assertEqual(by_station.loc['11800', 'end_date'], '2025-01-03T00:00Z')
        self.assertEqual(by_station.loc['11999', 'begin_date'], '2025-01-02T00:00Z')
        self.assertEqual(by_station.loc['11999', 'end_date'], '2025-01-03T00:00Z')
        self.assertTrue(by_station[['gh_id', 'full_name', 'longitude', 'latitude', 'elevation_m']].isna().all().all())
    def test_discover_recent_daily_stations_shmu_handles_empty_payload_conservatively(self) -> None:
        with patch('weatherdownload.providers.sk.metadata._load_recent_daily_payload_text', return_value='{"id": "kli_inter", "data": []}'):
            stations = discover_recent_daily_stations_shmu(timeout=60)
        self.assertTrue(stations.empty)
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
    def test_read_station_observation_metadata_country_sk_from_sample_payload(self) -> None:
        def fake_read_text(source: str, timeout: int) -> str:
            if source == str(SAMPLE_PAYLOAD_PATH):
                return SAMPLE_PAYLOAD_TEXT
            if source.endswith('kli_inter_metadata.json'):
                return SAMPLE_METADATA_TEXT
            raise AssertionError(f'unexpected source: {source}')

        with patch('weatherdownload.providers.sk.metadata._read_text', side_effect=fake_read_text):
            observation_metadata = read_station_observation_metadata(country='SK', source_url=str(SAMPLE_PAYLOAD_PATH))

        self.assertEqual(
            list(observation_metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertIn('t_max', observation_metadata['element'].tolist())
        self.assertIn('zra_uhrn', observation_metadata['element'].tolist())
        self.assertIn('voda_vypar', observation_metadata['element'].tolist())
        self.assertTrue(observation_metadata['description'].fillna('').str.contains('Maximum air temperature').any())
        self.assertTrue(observation_metadata['description'].fillna('').str.contains('Water evaporation').any())
        self.assertTrue(observation_metadata['height'].isna().all())

    def test_query_normalizes_canonical_and_raw_shmu_elements(self) -> None:
        canonical_query = ObservationQuery(
            country='SK',
            dataset_scope='recent',
            resolution='daily',
            station_ids=['11800'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_max', 'precipitation', 'open_water_evaporation'],
        )
        raw_query = ObservationQuery(
            country='SK',
            dataset_scope='recent',
            resolution='daily',
            station_ids=['11800'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['t_max', 'zra_uhrn', 'voda_vypar'],
        )
        self.assertEqual(canonical_query.elements, ['t_max', 'zra_uhrn', 'voda_vypar'])
        self.assertEqual(raw_query.elements, ['t_max', 'zra_uhrn', 'voda_vypar'])

    def test_resolve_latest_recent_daily_data_url_from_sample_indexes(self) -> None:
        def fake_read_text(source: str, timeout: int) -> str:
            if source.endswith('/recent/data/daily/'):
                return SAMPLE_INDEX_HTML
            if source.endswith('/recent/data/daily/2025-02/'):
                return '<html><body><a href="kli-inter - 2025-02.json">kli-inter - 2025-02.json</a></body></html>'
            raise AssertionError(f'unexpected source: {source}')

        with patch('weatherdownload.providers.sk.observations._read_text', side_effect=fake_read_text):
            latest_url = resolve_latest_recent_daily_data_url(timeout=60)

        self.assertTrue(latest_url.endswith('/recent/data/daily/2025-02/kli-inter - 2025-02.json'))

    def test_build_recent_daily_month_urls_from_sample_index(self) -> None:
        query = ObservationQuery(
            country='SK',
            dataset_scope='recent',
            resolution='daily',
            station_ids=['11800'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_max'],
        )

        def fake_read_text(source: str, timeout: int) -> str:
            if source.endswith('/recent/data/daily/'):
                return SAMPLE_INDEX_HTML
            if source.endswith('/recent/data/daily/2025-01/'):
                return SAMPLE_MONTH_INDEX_HTML
            raise AssertionError(f'unexpected source: {source}')

        with patch('weatherdownload.providers.sk.observations._read_text', side_effect=fake_read_text):
            urls = build_recent_daily_month_urls(query, spec=get_dataset_spec('recent', 'daily'), timeout=60)

        self.assertEqual(urls, ['https://opendata.shmu.sk/meteorology/climate/recent/data/daily/2025-01/kli-inter - 2025-01.json'])

    def test_sk_recent_daily_output_columns_are_exact(self) -> None:
        observations = self._download_all_supported_sample_observations()
        self.assertEqual(list(observations.columns), EXPECTED_SK_DAILY_COLUMNS)

    def test_sk_recent_daily_raw_to_canonical_mapping_is_stable(self) -> None:
        observations = self._download_all_supported_sample_observations()
        mapping = (
            observations[['element', 'element_raw']]
            .drop_duplicates()
            .sort_values(['element', 'element_raw'])
        )
        self.assertEqual(
            {
                row.element: row.element_raw
                for row in mapping.itertuples(index=False)
            },
            EXPECTED_SK_CANONICAL_MAPPING,
        )

    def test_download_daily_observations_country_sk_with_canonical_elements(self) -> None:
        def fake_read_text(source: str, timeout: int) -> str:
            if source.endswith('/recent/data/daily/'):
                return SAMPLE_INDEX_HTML
            if source.endswith('/recent/data/daily/2025-01/'):
                return SAMPLE_MONTH_INDEX_HTML
            if source.endswith('kli-inter - 2025-01.json'):
                return SAMPLE_PAYLOAD_TEXT
            raise AssertionError(f'unexpected source: {source}')

        query = ObservationQuery(
            country='SK',
            dataset_scope='recent',
            resolution='daily',
            station_ids=['11800'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_max', 'precipitation', 'open_water_evaporation'],
        )
        station_metadata = read_station_metadata(country='SK', source_url=str(SAMPLE_PAYLOAD_PATH))

        with patch('weatherdownload.providers.sk.observations._read_text', side_effect=fake_read_text):
            observations = download_observations(query, country='SK', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), EXPECTED_SK_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['open_water_evaporation', 'precipitation', 'tas_max'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['t_max', 'voda_vypar', 'zra_uhrn'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(observations.iloc[0]['observation_date'].isoformat(), '2025-01-01')
        self.assertAlmostEqual(float(observations[observations['element'] == 'tas_max'].iloc[0]['value']), 5.2)
        self.assertAlmostEqual(float(observations[observations['element'] == 'precipitation'].iloc[1]['value']), 2.5)
        self.assertAlmostEqual(float(observations[observations['element'] == 'open_water_evaporation'].iloc[0]['value']), 0.1)

    def test_sk_recent_daily_regression_fixture_record_count_and_key_values_are_stable(self) -> None:
        observations = self._download_all_supported_sample_observations()
        self.assertEqual(len(observations), 10)
        self.assertEqual(observations['station_id'].nunique(), 1)
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_max', pd.Timestamp('2025-01-01').date())]), 5.2)
        self.assertAlmostEqual(float(lookup[('tas_min', pd.Timestamp('2025-01-01').date())]), -2.1)
        self.assertAlmostEqual(float(lookup[('precipitation', pd.Timestamp('2025-01-02').date())]), 2.5)
        self.assertAlmostEqual(float(lookup[('open_water_evaporation', pd.Timestamp('2025-01-02').date())]), 0.3)
        self.assertTrue(pd.isna(lookup[('sunshine_duration', pd.Timestamp('2025-01-02').date())]))

    def test_download_daily_observations_country_sk_accepts_raw_codes(self) -> None:
        def fake_read_text(source: str, timeout: int) -> str:
            if source.endswith('/recent/data/daily/'):
                return SAMPLE_INDEX_HTML
            if source.endswith('/recent/data/daily/2025-01/'):
                return SAMPLE_MONTH_INDEX_HTML
            if source.endswith('kli-inter - 2025-01.json'):
                return SAMPLE_PAYLOAD_TEXT
            raise AssertionError(f'unexpected source: {source}')

        query = ObservationQuery(
            country='SK',
            dataset_scope='recent',
            resolution='daily',
            station_ids=['11800'],
            start_date='2025-01-01',
            end_date='2025-01-01',
            elements=['t_max'],
        )

        with patch('weatherdownload.providers.sk.observations._read_text', side_effect=fake_read_text):
            observations = download_observations(query, country='SK')

        self.assertEqual(list(observations['element'].unique()), ['tas_max'])
        self.assertEqual(list(observations['element_raw'].unique()), ['t_max'])

    def _download_all_supported_sample_observations(self) -> pd.DataFrame:
        def fake_read_text(source: str, timeout: int) -> str:
            if source.endswith('/recent/data/daily/'):
                return SAMPLE_INDEX_HTML
            if source.endswith('/recent/data/daily/2025-01/'):
                return SAMPLE_MONTH_INDEX_HTML
            if source.endswith('kli-inter - 2025-01.json'):
                return SAMPLE_PAYLOAD_TEXT
            raise AssertionError(f'unexpected source: {source}')

        query = ObservationQuery(
            country='SK',
            dataset_scope='recent',
            resolution='daily',
            station_ids=['11800'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=list(EXPECTED_SK_CANONICAL_MAPPING.keys()),
        )
        station_metadata = read_station_metadata(country='SK', source_url=str(SAMPLE_PAYLOAD_PATH))

        with patch('weatherdownload.providers.sk.observations._read_text', side_effect=fake_read_text):
            return download_observations(query, country='SK', station_metadata=station_metadata)


if __name__ == '__main__':
    unittest.main()





