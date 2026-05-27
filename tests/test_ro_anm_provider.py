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


FIXTURE_DIR = Path('tests/data/ro_anm')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


def _anm_observation_response(url: str, timeout: int = 60) -> _MockTextResponse:
    del timeout
    filename = url.rsplit('/', 1)[-1]
    station_id = filename.split('.')[2]
    raw_code = filename.split('.')[3]
    fixture_path = FIXTURE_DIR / f'obs_{station_id}_{raw_code}.xml'
    if not fixture_path.exists():
        raise AssertionError(f'unexpected ANM observation URL: {url}')
    return _MockTextResponse(fixture_path.read_text(encoding='utf-8'))


class RomaniaAnmProviderTests(unittest.TestCase):
    def test_ro_discovery_exposes_both_anm_and_ghcnd_daily(self) -> None:
        self.assertEqual(list_providers(country='RO'), ['anm', 'ghcnd'])
        self.assertEqual(list_resolutions(country='RO', provider='anm'), ['daily'])
        self.assertEqual(list_resolutions(country='RO', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='RO', provider='anm', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='RO', provider='anm', resolution='daily', provider_raw=True),
            ['TemperatureAverageDailyCLIMAT', 'TemperatureMaximumDailyCLIMAT', 'TemperatureMinimumDailyCLIMAT', 'TotalPrecipitationCLIMAT'],
        )
        self.assertEqual(
            list_supported_elements(country='RO', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )

    def test_read_station_metadata_country_ro_anm_from_fixture(self) -> None:
        stations = read_station_metadata(country='RO', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['15015', '15420'])
        self.assertEqual(stations['full_name'].tolist(), ['Ocna Șugatag', 'București Băneasa'])
        self.assertEqual(stations['longitude'].tolist(), [23.9405, 26.0782])
        self.assertEqual(stations['latitude'].tolist(), [47.7771, 44.5104])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertTrue(stations['elevation_m'].isna().all())
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('anm', 'daily')]['15015'],
            [
                'TemperatureAverageDailyCLIMAT',
                'TemperatureMaximumDailyCLIMAT',
                'TemperatureMinimumDailyCLIMAT',
                'TotalPrecipitationCLIMAT',
            ],
        )

    def test_read_station_observation_metadata_country_ro_anm_from_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='RO', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['station_id'].unique().tolist()), ['15015', '15420'])
        self.assertEqual(
            sorted(metadata['element'].unique().tolist()),
            [
                'TemperatureAverageDailyCLIMAT',
                'TemperatureMaximumDailyCLIMAT',
                'TemperatureMinimumDailyCLIMAT',
                'TotalPrecipitationCLIMAT',
            ],
        )

    def test_download_daily_observations_ro_anm_normalizes_fixture_payloads(self) -> None:
        station_metadata = read_station_metadata(country='RO', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='RO',
            provider='anm',
            resolution='daily',
            station_ids=['15015'],
            start_date='1961-01-01',
            end_date='1961-01-03',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        with patch('weatherdownload.providers.ro.anm_daily.requests.get', side_effect=_anm_observation_response):
            observations = download_observations(query, country='RO', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max', 'tas_mean', 'tas_min'])
        self.assertEqual(
            sorted(observations['element_raw'].unique().tolist()),
            [
                'TemperatureAverageDailyCLIMAT',
                'TemperatureMaximumDailyCLIMAT',
                'TemperatureMinimumDailyCLIMAT',
                'TotalPrecipitationCLIMAT',
            ],
        )
        self.assertEqual(observations['provider'].unique().tolist(), ['anm'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())

        expected_values = {
            ('tas_mean', '1961-01-01'): -0.1,
            ('tas_mean', '1961-01-02'): 1.2,
            ('tas_mean', '1961-01-03'): 4.0,
            ('tas_max', '1961-01-01'): 1.0,
            ('tas_max', '1961-01-02'): 4.2,
            ('tas_max', '1961-01-03'): 7.2,
            ('tas_min', '1961-01-01'): -1.3,
            ('tas_min', '1961-01-02'): -1.3,
            ('tas_min', '1961-01-03'): 1.2,
            ('precipitation', '1961-01-01'): 0.0,
            ('precipitation', '1961-01-02'): 0.3,
            ('precipitation', '1961-01-03'): 0.5,
        }
        actual_values = {
            (row.element, row.observation_date.isoformat()): round(row.value, 1)
            for row in observations.itertuples(index=False)
        }
        self.assertEqual(actual_values, expected_values)

    def test_ro_anm_supported_elements_remain_conservative(self) -> None:
        supported = set(list_supported_elements(country='RO', provider='anm', resolution='daily'))
        self.assertEqual(supported, {'tas_mean', 'tas_max', 'tas_min', 'precipitation'})
        self.assertTrue(
            {
                'snow_depth',
                'wind_speed',
                'relative_humidity',
                'pressure',
                'solar_radiation',
                'sunshine_duration',
                'vapour_pressure',
                'open_water_evaporation',
            }.isdisjoint(supported)
        )


if __name__ == '__main__':
    unittest.main()
