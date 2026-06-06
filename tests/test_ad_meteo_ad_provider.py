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
from weatherdownload.providers.ad.parser import AD_NORMALIZED_DAILY_COLUMNS


SAMPLE_CLIMATOLOGY_PATH = Path('tests/data/sample_ad_meteo_ad_climatologia.html')
SAMPLE_DAILY_WORKBOOK_PATH = Path('tests/data/sample_ad_meteo_ad_daily_rich.xls')
EXPECTED_AD_DAILY_MAPPING = {
    'tas_mean': 'temp_mitjana',
    'tas_max': 'temp_max',
    'tas_min': 'temp_min',
    'precipitation': 'prec_total',
    'wind_speed': 'vel_vent_mitjana',
    'wind_speed_max': 'vel_vent_max',
    'relative_humidity': 'hum_mitjana',
    'sunshine_duration': 'insolacio_total',
    'solar_radiation': 'irradiacio_total',
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


class AndorraMeteoAdProviderTests(unittest.TestCase):
    def test_supported_countries_include_ad(self) -> None:
        self.assertIn('AD', list_supported_countries())
        self.assertEqual(list_providers(country='AD'), ['meteo_ad'])
        self.assertEqual(list_resolutions(country='AD', provider='meteo_ad'), ['daily'])

    def test_read_station_metadata_country_ad_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='AD', source_url=str(SAMPLE_CLIMATOLOGY_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['99130011'])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(stations.iloc[0]['full_name'], 'Aixàs')
        self.assertAlmostEqual(float(stations.iloc[0]['longitude']), 1.4771)
        self.assertAlmostEqual(float(stations.iloc[0]['latitude']), 42.4837)

    def test_read_station_observation_metadata_country_ad_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='AD', source_url=str(SAMPLE_CLIMATOLOGY_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(set(metadata['station_id']), {'99130011'})
        self.assertEqual(set(metadata['obs_type']), {'HISTORICAL_DAILY'})
        self.assertEqual(set(metadata['element']), set(EXPECTED_AD_DAILY_MAPPING.values()))

    def test_discovery_country_ad_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='AD', provider='meteo_ad', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'sunshine_duration', 'solar_radiation'],
        )
        self.assertEqual(
            list_supported_elements(country='AD', provider='meteo_ad', resolution='daily', provider_raw=True),
            ['temp_mitjana', 'temp_max', 'temp_min', 'prec_total', 'vel_vent_mitjana', 'vel_vent_max', 'hum_mitjana', 'insolacio_total', 'irradiacio_total'],
        )

    def test_ad_queries_accept_canonical_and_raw_codes(self) -> None:
        daily_query = ObservationQuery(country='AD', provider='meteo_ad', resolution='daily', station_ids=['99130011'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'solar_radiation'])
        self.assertEqual(daily_query.elements, ['temp_mitjana', 'irradiacio_total'])

    def test_ad_query_rejects_unsupported_resolution_and_unmapped_fields(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='AD', provider='meteo_ad', resolution='1hour', station_ids=['99130011'], start='2024-01-01T00:00:00Z', end='2024-01-01T01:00:00Z', elements=['tas_mean'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='AD', provider='meteo_ad', resolution='daily', station_ids=['99130011'], start_date='2024-01-01', end_date='2024-01-02', elements=['pressure'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='AD', provider='meteo_ad', resolution='daily', station_ids=['99130011'], start_date='2024-01-01', end_date='2024-01-02', elements=['snow_depth'])

    def test_download_daily_observations_country_ad_uses_public_workbook_export(self) -> None:
        station_metadata = read_station_metadata(country='AD', source_url=str(SAMPLE_CLIMATOLOGY_PATH))

        def fake_get(url, timeout=60):
            del timeout
            self.assertIn('/climatologia/list2xls?', url)
            self.assertIn('estacio=99130011', url)
            self.assertIn('mesura=0', url)
            self.assertIn('dades=temp_mitjana', url)
            self.assertIn('dades=prec_total', url)
            self.assertIn('dades=irradiacio_total', url)
            return _MockResponse(content=SAMPLE_DAILY_WORKBOOK_PATH.read_bytes())

        query = ObservationQuery(country='AD', provider='meteo_ad', resolution='daily', station_ids=['99130011'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation', 'solar_radiation'])
        with patch('weatherdownload.providers.ad.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='AD', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), AD_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'solar_radiation', 'tas_mean'])
        self.assertEqual(observations['provider'].unique().tolist(), ['meteo_ad'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2024-01-01').date())]), 1.6)
        self.assertAlmostEqual(float(lookup[('precipitation', pd.Timestamp('2024-01-02').date())]), 0.0)
        self.assertAlmostEqual(float(lookup[('solar_radiation', pd.Timestamp('2024-01-01').date())]), 7.357786)

    def test_ad_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='AD', source_url=str(SAMPLE_CLIMATOLOGY_PATH))

        def fake_get(url, timeout=60):
            del timeout
            return _MockResponse(content=SAMPLE_DAILY_WORKBOOK_PATH.read_bytes())

        daily_query = ObservationQuery(country='AD', provider='meteo_ad', resolution='daily', station_ids=['99130011'], start_date='2024-01-01', end_date='2024-01-02', elements=list(EXPECTED_AD_DAILY_MAPPING.keys()))
        with patch('weatherdownload.providers.ad.daily.requests.get', side_effect=fake_get):
            daily = download_observations(daily_query, country='AD', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in daily[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_AD_DAILY_MAPPING)


if __name__ == '__main__':
    unittest.main()
