import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)


FIXTURE_DIR = Path('tests/data/si_arso')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


def _arso_observation_response(url: str, timeout: int = 60) -> _MockTextResponse:
    del timeout
    if 'id=1895' not in url:
        raise AssertionError(f'unexpected ARSO observation URL: {url}')
    return _MockTextResponse((FIXTURE_DIR / 'obs_1895_2024-01-01_2024-01-03.xml').read_text(encoding='utf-8'))


class SloveniaArsoProviderTests(unittest.TestCase):
    def test_si_discovery_exposes_both_arso_and_ghcnd_daily(self) -> None:
        self.assertEqual(list_providers(country='SI'), ['arso', 'ghcnd'])
        self.assertEqual(list_resolutions(country='SI', provider='arso'), ['daily'])
        self.assertEqual(list_resolutions(country='SI', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='SI', provider='arso', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='SI', provider='arso', resolution='daily', provider_raw=True),
            ['t2m_klima', 'tmax', 'tmin', 'padavine_klima', 'sneg_skupni', 'trajanje_so'],
        )
        self.assertEqual(
            list_supported_elements(country='SI', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )

    def test_read_station_metadata_country_si_arso_from_fixture(self) -> None:
        stations = read_station_metadata(country='SI', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['1895', '3049'])
        self.assertEqual(stations['full_name'].tolist(), ['Ljubljana Bežigrad', 'Letališče Jožeta Pučnika Ljubljana'])
        self.assertEqual(stations['longitude'].tolist(), [14.512359, 14.457611])
        self.assertEqual(stations['latitude'].tolist(), [46.065505, 46.223889])
        self.assertEqual(stations['elevation_m'].tolist(), [299.0, 364.0])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('arso', 'daily')]['1895'],
            ['t2m_klima', 'tmax', 'tmin', 'padavine_klima', 'sneg_skupni', 'trajanje_so'],
        )

    def test_read_station_observation_metadata_country_si_arso_from_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='SI', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['station_id'].unique().tolist()), ['1895', '3049'])
        self.assertEqual(
            sorted(metadata['element'].unique().tolist()),
            ['padavine_klima', 'sneg_skupni', 't2m_klima', 'tmax', 'tmin', 'trajanje_so'],
        )

    def test_download_daily_observations_si_arso_normalizes_fixture_payloads(self) -> None:
        station_metadata = read_station_metadata(country='SI', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='SI',
            provider='arso',
            resolution='daily',
            station_ids=['1895'],
            start_date='2024-01-01',
            end_date='2024-01-03',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth', 'sunshine_duration'],
        )
        with patch('weatherdownload.providers.si.arso_daily.requests.get', side_effect=_arso_observation_response):
            observations = download_observations(query, country='SI', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['precipitation', 'snow_depth', 'sunshine_duration', 'tas_max', 'tas_mean', 'tas_min'],
        )
        self.assertEqual(
            sorted(observations['element_raw'].unique().tolist()),
            ['padavine_klima', 'sneg_skupni', 't2m_klima', 'tmax', 'tmin', 'trajanje_so'],
        )
        self.assertEqual(observations['provider'].unique().tolist(), ['arso'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())

        expected_values = {
            ('tas_mean', '2024-01-01'): 5.8,
            ('tas_mean', '2024-01-02'): 6.2,
            ('tas_max', '2024-01-01'): 12.8,
            ('tas_max', '2024-01-02'): 11.0,
            ('tas_max', '2024-01-03'): 9.2,
            ('tas_min', '2024-01-01'): 4.7,
            ('tas_min', '2024-01-02'): 2.9,
            ('tas_min', '2024-01-03'): 1.0,
            ('precipitation', '2024-01-01'): 15.7,
            ('precipitation', '2024-01-02'): 0.0,
            ('precipitation', '2024-01-03'): 2.5,
            ('snow_depth', '2024-01-01'): 0.0,
            ('snow_depth', '2024-01-02'): 10.0,
            ('sunshine_duration', '2024-01-01'): 0.3,
            ('sunshine_duration', '2024-01-02'): 0.0,
            ('sunshine_duration', '2024-01-03'): 7.1,
        }
        actual_values = {
            (row.element, row.observation_date.isoformat()): round(row.value, 1)
            for row in observations.itertuples(index=False)
        }
        self.assertEqual(actual_values, expected_values)
        self.assertNotIn(('tas_mean', '2024-01-03'), actual_values)
        self.assertNotIn(('snow_depth', '2024-01-03'), actual_values)

    def test_si_arso_supported_elements_remain_conservative(self) -> None:
        supported = set(list_supported_elements(country='SI', provider='arso', resolution='daily'))
        self.assertEqual(
            supported,
            {'tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth', 'sunshine_duration'},
        )
        self.assertTrue(
            {
                'wind_speed',
                'relative_humidity',
                'pressure',
                'solar_radiation',
                'vapour_pressure',
                'open_water_evaporation',
            }.isdisjoint(supported)
        )


if __name__ == '__main__':
    unittest.main()
