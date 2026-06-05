import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)


FIXTURE_DIR = Path('tests/data/lt_meteo_lt')
DAILY_FIXTURE_TEXT = (FIXTURE_DIR / 'vilniaus-ams_2024-01-01.json').read_text(encoding='utf-8')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            http_error = requests.HTTPError(f'HTTP {self.status_code}')
            http_error.response = self
            raise http_error


def _meteo_lt_daily_response(url: str, timeout: int = 60) -> _MockTextResponse:
    del timeout
    if url.endswith('/stations/vilniaus-ams/observations/2024-01-01'):
        return _MockTextResponse(DAILY_FIXTURE_TEXT)
    if url.endswith('/stations/vilniaus-ams/observations/2024-01-02'):
        return _MockTextResponse('{"message": "Not found"}', status_code=404)
    raise AssertionError(f'unexpected Meteo.lt observation URL: {url}')


class LithuaniaMeteoLtProviderTests(unittest.TestCase):
    def test_lt_discovery_exposes_meteo_lt_and_ghcnd_daily(self) -> None:
        self.assertEqual(list_providers(country='LT'), ['ghcnd', 'meteo_lt'])
        self.assertEqual(list_resolutions(country='LT', provider='meteo_lt'), ['daily'])
        self.assertEqual(list_resolutions(country='LT', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='LT', provider='meteo_lt', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'pressure', 'snow_depth', 'cloud_cover'],
        )
        self.assertEqual(
            list_supported_elements(country='LT', provider='meteo_lt', resolution='daily', provider_raw=True),
            ['airTemperature_mean', 'airTemperature_max', 'airTemperature_min', 'precipitation', 'windSpeed', 'windGust', 'relativeHumidity', 'seaLevelPressure', 'snowDepth', 'cloudCover'],
        )
        self.assertEqual(
            list_supported_elements(country='LT', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )

    def test_read_station_metadata_country_lt_from_fixture(self) -> None:
        stations = read_station_metadata(country='LT', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['kauno-ams', 'vilniaus-ams'])
        self.assertEqual(stations['full_name'].tolist(), ['Kauno AMS', 'Vilniaus AMS'])
        self.assertEqual(stations['longitude'].tolist(), [23.8333, 25.107064])
        self.assertEqual(stations['latitude'].tolist(), [54.8833, 54.625992])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('meteo_lt', 'daily')]['vilniaus-ams'],
            ['airTemperature_mean', 'airTemperature_max', 'airTemperature_min', 'precipitation', 'windSpeed', 'windGust', 'relativeHumidity', 'seaLevelPressure', 'snowDepth', 'cloudCover'],
        )

    def test_read_station_observation_metadata_country_lt_from_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='LT', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['station_id'].unique().tolist()), ['kauno-ams', 'vilniaus-ams'])
        self.assertEqual(
            sorted(metadata['element'].unique().tolist()),
            ['airTemperature_max', 'airTemperature_mean', 'airTemperature_min', 'cloudCover', 'precipitation', 'relativeHumidity', 'seaLevelPressure', 'snowDepth', 'windGust', 'windSpeed'],
        )

    def test_download_daily_observations_lt_meteo_lt_aggregates_fixture_payload(self) -> None:
        station_metadata = read_station_metadata(country='LT', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='LT',
            provider='meteo_lt',
            resolution='daily',
            station_ids=['vilniaus-ams'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=[
                'tas_mean',
                'tas_max',
                'tas_min',
                'precipitation',
                'wind_speed',
                'wind_speed_max',
                'relative_humidity',
                'pressure',
                'snow_depth',
                'cloud_cover',
            ],
        )
        with patch('weatherdownload.providers.lt.observations.requests.get', side_effect=_meteo_lt_daily_response):
            observations = download_observations(query, country='LT', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['cloud_cover', 'precipitation', 'pressure', 'relative_humidity', 'snow_depth', 'tas_max', 'tas_mean', 'tas_min', 'wind_speed', 'wind_speed_max'],
        )
        self.assertEqual(observations['provider'].unique().tolist(), ['meteo_lt'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())

        lookup = {(row.element, row.observation_date.isoformat()): round(float(row.value), 4) for row in observations.itertuples(index=False)}
        self.assertEqual(lookup[('tas_mean', '2024-01-01')], round((-1.0 - 2.0 + 1.0) / 3.0, 4))
        self.assertEqual(lookup[('tas_max', '2024-01-01')], 1.0)
        self.assertEqual(lookup[('tas_min', '2024-01-01')], -2.0)
        self.assertEqual(lookup[('precipitation', '2024-01-01')], 1.6)
        self.assertEqual(lookup[('wind_speed', '2024-01-01')], 3.0)
        self.assertEqual(lookup[('wind_speed_max', '2024-01-01')], 8.0)
        self.assertEqual(lookup[('relative_humidity', '2024-01-01')], round((92.0 + 88.0 + 86.0) / 3.0, 4))
        self.assertEqual(lookup[('pressure', '2024-01-01')], round((1015.0 + 1014.0 + 1012.0) / 3.0, 4))
        self.assertEqual(lookup[('snow_depth', '2024-01-01')], 5.0)
        self.assertEqual(lookup[('cloud_cover', '2024-01-01')], 80.0)

    def test_download_daily_observations_lt_meteo_lt_treats_404_as_no_data(self) -> None:
        station_metadata = read_station_metadata(country='LT', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='LT',
            provider='meteo_lt',
            resolution='daily',
            station_ids=['vilniaus-ams'],
            start_date='2024-01-02',
            end_date='2024-01-02',
            elements=['tas_mean'],
        )
        with patch('weatherdownload.providers.lt.observations.requests.get', side_effect=_meteo_lt_daily_response):
            with self.assertRaisesRegex(Exception, 'No observations found'):
                download_observations(query, country='LT', station_metadata=station_metadata)


if __name__ == '__main__':
    unittest.main()
