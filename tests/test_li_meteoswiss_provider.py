import json
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    QueryValidationError,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.ch.parser import CH_NORMALIZED_DAILY_COLUMNS


SAMPLE_STATIONS_PATH = Path('tests/data/sample_ch_meta_stations.csv')
SAMPLE_ITEM_PATH = Path('tests/data/sample_ch_vad_item.json')
SAMPLE_ITEM_ASSETS = json.loads(SAMPLE_ITEM_PATH.read_text(encoding='utf-8'))['assets']
SAMPLE_DAILY_HISTORICAL_PATH = Path('tests/data/sample_ch_vad_d_historical.csv')
SAMPLE_DAILY_RECENT_PATH = Path('tests/data/sample_ch_vad_d_recent.csv')
EXPECTED_LI_DAILY_MAPPING = {
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
    'solar_radiation': 'gre000d0',
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


class LiechtensteinMeteoSwissProviderTests(unittest.TestCase):
    def test_supported_countries_include_li(self) -> None:
        self.assertIn('LI', list_supported_countries())
        self.assertEqual(list_providers(country='LI'), ['meteoswiss'])
        self.assertEqual(list_resolutions(country='LI', provider='meteoswiss'), ['daily'])

    def test_read_station_metadata_country_li_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='LI', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['VAD'])
        self.assertEqual(stations.iloc[0]['gh_id'], '0-20000-0-06990')
        self.assertEqual(stations.iloc[0]['full_name'], 'Vaduz')

    def test_read_station_observation_metadata_country_li_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='LI', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(set(metadata['station_id']), {'VAD'})
        self.assertEqual(set(metadata['obs_type']), {'HISTORICAL_DAILY'})
        self.assertIn('gre000d0', metadata['element'].tolist())

    def test_discovery_country_li_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='LI', provider='meteoswiss', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure', 'sunshine_duration', 'solar_radiation'],
        )
        self.assertEqual(
            list_supported_elements(country='LI', provider='meteoswiss', resolution='daily', provider_raw=True),
            ['tre200d0', 'tre200dx', 'tre200dn', 'rre150d0', 'fkl010d0', 'fkl010d1', 'ure200d0', 'pva200d0', 'prestad0', 'sre000d0', 'gre000d0'],
        )

    def test_li_queries_accept_canonical_and_raw_codes(self) -> None:
        daily_query = ObservationQuery(country='LI', provider='meteoswiss', resolution='daily', station_ids=['VAD'], start_date='2025-12-31', end_date='2026-01-02', elements=['tas_mean', 'solar_radiation'])
        self.assertEqual(daily_query.elements, ['tre200d0', 'gre000d0'])

    def test_li_query_rejects_unsupported_resolution_and_unmapped_field(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='LI', provider='meteoswiss', resolution='1hour', station_ids=['VAD'], start='2026-01-01T00:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='LI', provider='meteoswiss', resolution='daily', station_ids=['VAD'], start_date='2026-01-01', end_date='2026-01-02', elements=['snow_depth'])

    def test_download_daily_observations_country_li_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='LI', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, timeout=60):
            if url.endswith('/items/vad'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_vad_d_historical.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_vad_d_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='LI', provider='meteoswiss', resolution='daily', station_ids=['VAD'], start_date='2025-12-31', end_date='2026-01-02', elements=['tas_mean', 'pressure', 'solar_radiation'])
        with patch('weatherdownload.providers.li.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='LI', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), CH_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'solar_radiation', 'tas_mean'])
        self.assertEqual(observations['provider'].unique().tolist(), ['meteoswiss'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2025-12-31').date())]), -1.7)
        self.assertAlmostEqual(float(lookup[('solar_radiation', pd.Timestamp('2026-01-02').date())]), 88.0)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2026-01-01').date())]), 960.9)

    def test_li_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='LI', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, timeout=60):
            if url.endswith('/items/vad'):
                return _MockResponse(content=SAMPLE_ITEM_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_vad_d_historical.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_ITEM_ASSETS['ogd-smn_vad_d_recent.csv']['href']:
                return _MockResponse(content=SAMPLE_DAILY_RECENT_PATH.read_bytes())
            return _MockResponse(status_code=404)

        daily_query = ObservationQuery(country='LI', provider='meteoswiss', resolution='daily', station_ids=['VAD'], start_date='2025-12-31', end_date='2026-01-02', elements=list(EXPECTED_LI_DAILY_MAPPING.keys()))
        with patch('weatherdownload.providers.li.daily.requests.get', side_effect=fake_get):
            daily = download_observations(daily_query, country='LI', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in daily[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_LI_DAILY_MAPPING)


if __name__ == '__main__':
    unittest.main()
