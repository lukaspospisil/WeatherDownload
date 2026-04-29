import json
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    QueryValidationError,
    download_observations,
    list_dataset_scopes,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.ch.parser import (
    CH_NORMALIZED_DAILY_COLUMNS,
    CH_NORMALIZED_SUBDAILY_COLUMNS,
    parse_ch_observation_csv,
)
from weatherdownload.providers.ch.registry import CH_ITEM_URL_TEMPLATE

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ch_meta_stations.csv')
SAMPLE_PARAMETERS_PATH = Path('tests/data/sample_ch_meta_parameters.csv')
SAMPLE_INVENTORY_PATH = Path('tests/data/sample_ch_meta_datainventory.csv')
SAMPLE_ITEM_PATH = Path('tests/data/sample_ch_aig_item.json')
SAMPLE_DAILY_HISTORICAL_PATH = Path('tests/data/sample_ch_aig_d_historical.csv')
SAMPLE_DAILY_RECENT_PATH = Path('tests/data/sample_ch_aig_d_recent.csv')
SAMPLE_HOURLY_HISTORICAL_PATH = Path('tests/data/sample_ch_aig_h_historical_2020_2029.csv')
SAMPLE_HOURLY_RECENT_PATH = Path('tests/data/sample_ch_aig_h_recent.csv')
SAMPLE_TENMIN_HISTORICAL_PATH = Path('tests/data/sample_ch_aig_t_historical_2020_2029.csv')
SAMPLE_TENMIN_RECENT_PATH = Path('tests/data/sample_ch_aig_t_recent.csv')
SAMPLE_ITEM_ASSETS = json.loads(SAMPLE_ITEM_PATH.read_text(encoding='utf-8'))['assets']
EXPECTED_CH_DAILY_MAPPING = {
    'tas_mean': 'tre200d0',
    'tas_max': 'tre200dx',
    'tas_min': 'tre200dn',
    'precipitation': 'rre150d0',
    'wind_speed': 'fkl010d0',
    'wind_speed_max': 'fkl010d1',
    'relative_humidity': 'ure200d0',
    'vapour_pressure': 'pva200d0',
    'pressure': 'prestad0',
    'sunshine_duration': 'sre000d0',
}
EXPECTED_CH_HOURLY_MAPPING = {
    'tas_mean': 'tre200h0',
    'precipitation': 'rre150h0',
    'wind_speed': 'fkl010h0',
    'wind_speed_max': 'fkl010h1',
    'relative_humidity': 'ure200h0',
    'vapour_pressure': 'pva200h0',
    'pressure': 'prestah0',
    'sunshine_duration': 'sre000h0',
}
EXPECTED_CH_TENMIN_MAPPING = {
    'tas_mean': 'tre200s0',
    'precipitation': 'rre150z0',
    'wind_speed': 'fkl010z0',
    'wind_speed_max': 'fkl010z1',
    'relative_humidity': 'ure200s0',
    'vapour_pressure': 'pva200s0',
    'pressure': 'prestas0',
    'sunshine_duration': 'sre000z0',
}


class _MockResponse:
    def __init__(self, text: str | None = None, status_code: int = 200, content: bytes | None = None) -> None:
        self.text = text or ''
        self.status_code = status_code
        self.encoding = 'utf-8'
        self.content = content if content is not None else self.text.encode('utf-8')

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class SwitzerlandProviderTests(unittest.TestCase):
    def test_supported_countries_include_ch(self) -> None:
        self.assertIn('CH', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='CH'), ['ghcnd', 'historical'])
        self.assertEqual(list_resolutions(country='CH', dataset_scope='ghcnd'), ['daily'])
        self.assertEqual(list_resolutions(country='CH', dataset_scope='historical'), ['10min', '1hour', 'daily'])

    def test_read_station_metadata_country_ch_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='CH', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations.iloc[0]['station_id'], 'ABO')
        self.assertEqual(stations.iloc[0]['gh_id'], '0-20000-0-06735')
        self.assertEqual(stations.iloc[2]['full_name'], 'Aigle')

    def test_read_station_observation_metadata_country_ch_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='CH', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertIn('HISTORICAL_DAILY', set(metadata['obs_type']))
        self.assertIn('HISTORICAL_HOURLY', set(metadata['obs_type']))
        self.assertIn('HISTORICAL_10MIN', set(metadata['obs_type']))
        aig_daily = metadata[(metadata['station_id'] == 'AIG') & (metadata['obs_type'] == 'HISTORICAL_DAILY')]
        self.assertIn('tre200d0', aig_daily['element'].tolist())
        self.assertTrue(aig_daily['height'].isna().all())

    def test_discovery_country_ch_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='CH', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='CH', dataset_scope='historical', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='CH', dataset_scope='historical', resolution='10min'),
            ['tas_mean', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='CH', dataset_scope='historical', resolution='10min', provider_raw=True),
            ['tre200s0', 'rre150z0', 'fkl010z0', 'fkl010z1', 'ure200s0', 'pva200s0', 'prestas0', 'sre000z0'],
        )

    def test_ch_queries_accept_canonical_and_raw_codes(self) -> None:
        daily_query = ObservationQuery(country='CH', dataset_scope='historical', resolution='daily', station_ids=['AIG'], start_date='2025-12-31', end_date='2026-01-02', elements=['tas_mean', 'precipitation'])
        hourly_query = ObservationQuery(country='CH', dataset_scope='historical', resolution='1hour', station_ids=['AIG'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='CH', dataset_scope='historical', resolution='10min', station_ids=['AIG'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(daily_query.elements, ['tre200d0', 'rre150d0'])
        self.assertEqual(hourly_query.elements, ['tre200h0', 'prestah0'])
        self.assertEqual(tenmin_query.elements, ['tre200s0', 'prestas0'])

    def test_ch_query_rejects_unsupported_resolution_and_ambiguous_element(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='CH', dataset_scope='historical', resolution='monthly', station_ids=['AIG'], start='2026-01-01T00:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='CH', dataset_scope='historical', resolution='10min', station_ids=['AIG'], start='2026-01-01T00:00:00Z', end='2026-01-01T00:10:00Z', elements=['gre000z0'])

    def test_parse_ch_observation_csv_keeps_source_columns(self) -> None:
        parsed = parse_ch_observation_csv(SAMPLE_TENMIN_HISTORICAL_PATH.read_text(encoding='cp1252'))
        self.assertIn('station_abbr', parsed.columns)
        self.assertIn('reference_timestamp', parsed.columns)
        self.assertIn('tre200s0', parsed.columns)
        self.assertEqual(parsed.iloc[0]['station_abbr'], 'AIG')

    def test_download_daily_observations_country_ch_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='CH', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, timeout=60):
            if url == CH_ITEM_URL_TEMPLATE.format(station_id='aig'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_d_historical.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_d_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='CH', dataset_scope='historical', resolution='daily', station_ids=['AIG'], start_date='2025-12-31', end_date='2026-01-02', elements=['tas_mean', 'pressure', 'precipitation'])
        with patch('weatherdownload.providers.ch.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='CH', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), CH_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'pressure', 'tas_mean'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2025-12-31').date())]), -2.2)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2026-01-02').date())]), 963.4)

    def test_download_hourly_observations_country_ch_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='CH', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, timeout=60):
            if url == CH_ITEM_URL_TEMPLATE.format(station_id='aig'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_h_historical_2020-2029.csv']['href']:
                return _MockResponse(content=SAMPLE_HOURLY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_h_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_HOURLY_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='CH', dataset_scope='historical', resolution='1hour', station_ids=['AIG'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean', 'pressure', 'vapour_pressure'])
        with patch('weatherdownload.providers.ch.hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='CH', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), CH_NORMALIZED_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean', 'vapour_pressure'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['1hour'])
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2025-12-31T23:00:00Z'))]), -3.4)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2026-01-01T01:00:00Z'))]), 975.4)
        self.assertAlmostEqual(float(lookup[('vapour_pressure', pd.Timestamp('2026-01-01T00:00:00Z'))]), 3.9)

    def test_download_tenmin_observations_country_ch_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='CH', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, timeout=60):
            if url == CH_ITEM_URL_TEMPLATE.format(station_id='aig'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_t_historical_2020-2029.csv']['href']:
                return _MockResponse(content=SAMPLE_TENMIN_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_t_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_TENMIN_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='CH', dataset_scope='historical', resolution='10min', station_ids=['AIG'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['tas_mean', 'pressure', 'wind_speed_max'])
        with patch('weatherdownload.providers.ch.tenmin.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='CH', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), CH_NORMALIZED_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean', 'wind_speed_max'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['10min'])
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2025-12-31T23:50:00Z'))]), -3.7)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2026-01-01T00:00:00Z'))]), 975.9)
        self.assertAlmostEqual(float(lookup[('wind_speed_max', pd.Timestamp('2026-01-01T00:10:00Z'))]), 2.8)

    def test_ch_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='CH', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_daily_get(url, timeout=60):
            if url == CH_ITEM_URL_TEMPLATE.format(station_id='aig'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_d_historical.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_d_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        daily_query = ObservationQuery(country='CH', dataset_scope='historical', resolution='daily', station_ids=['AIG'], start_date='2025-12-31', end_date='2026-01-02', elements=list(EXPECTED_CH_DAILY_MAPPING.keys()))
        with patch('weatherdownload.providers.ch.daily.requests.get', side_effect=fake_daily_get):
            daily = download_observations(daily_query, country='CH', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in daily[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_CH_DAILY_MAPPING)

        def fake_hourly_get(url, timeout=60):
            if url == CH_ITEM_URL_TEMPLATE.format(station_id='aig'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_h_historical_2020-2029.csv']['href']:
                return _MockResponse(content=SAMPLE_HOURLY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_h_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_HOURLY_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        hourly_query = ObservationQuery(country='CH', dataset_scope='historical', resolution='1hour', station_ids=['AIG'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=list(EXPECTED_CH_HOURLY_MAPPING.keys()))
        with patch('weatherdownload.providers.ch.hourly.requests.get', side_effect=fake_hourly_get):
            hourly = download_observations(hourly_query, country='CH', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in hourly[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_CH_HOURLY_MAPPING)

        def fake_tenmin_get(url, timeout=60):
            if url == CH_ITEM_URL_TEMPLATE.format(station_id='aig'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_t_historical_2020-2029.csv']['href']:
                return _MockResponse(content=SAMPLE_TENMIN_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_aig_t_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_TENMIN_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        tenmin_query = ObservationQuery(country='CH', dataset_scope='historical', resolution='10min', station_ids=['AIG'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=list(EXPECTED_CH_TENMIN_MAPPING.keys()))
        with patch('weatherdownload.providers.ch.tenmin.requests.get', side_effect=fake_tenmin_get):
            tenmin = download_observations(tenmin_query, country='CH', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in tenmin[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_CH_TENMIN_MAPPING)


if __name__ == '__main__':
    unittest.main()

