import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import find_stations_with_elements, list_station_elements, list_station_paths, read_station_metadata, read_station_observation_metadata, station_availability, station_supports


SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')
SAMPLE_GHCND_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class StationAvailabilityTests(unittest.TestCase):
    def _read_stations(self):
        return read_station_metadata(source_url='tests/data/sample_meta1.csv')

    def test_station_availability_lists_implemented_paths(self) -> None:
        stations = self._read_stations()
        availability = station_availability(stations, station_ids=['0-20000-0-11406'])
        self.assertEqual(
            set(tuple(item) for item in availability[['dataset_scope', 'resolution']].to_records(index=False)),
            {('historical_csv', '10min'), ('historical_csv', '1hour'), ('historical_csv', 'daily')},
        )

    def test_station_supports_checks_registry_and_active_date(self) -> None:
        stations = self._read_stations()
        self.assertTrue(station_supports(stations, '0-20000-0-11406', 'historical_csv', 'daily', active_on='2024-01-01'))
        self.assertTrue(station_supports(stations, '0-20000-0-11406', 'historical_csv', '10min', active_on='2024-01-01'))
        self.assertFalse(station_supports(stations, '0-20000-0-11414', 'historical_csv', 'daily', active_on='2024-01-01'))
        self.assertFalse(station_supports(stations, '0-20000-0-11406', 'now', '10min'))

    def test_list_station_paths_can_include_canonical_elements(self) -> None:
        stations = self._read_stations()
        paths = list_station_paths(stations, '0-20000-0-11406', include_elements=True)
        self.assertIn('supported_elements', paths.columns)
        self.assertEqual(paths.iloc[0]['station_id'], '0-20000-0-11406')
        self.assertIn('tas_mean', paths.iloc[0]['supported_elements'])

    def test_list_station_paths_can_include_mapping(self) -> None:
        stations = self._read_stations()
        paths = list_station_paths(stations, '0-20000-0-11406', include_element_mapping=True)
        self.assertIn('supported_element_mapping', paths.columns)
        self.assertEqual(paths.iloc[0]['supported_element_mapping']['tas_mean'], ['T'])

    def test_list_station_elements_for_implemented_path_returns_canonical_names_by_default(self) -> None:
        stations = self._read_stations()
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'historical_csv', '10min'), ['tas_mean', 'tas_max', 'tas_min', 'tas_period_max', 'soil_temperature_10cm', 'soil_temperature_100cm', 'sunshine_duration'])
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'historical_csv', '1hour'), ['vapour_pressure', 'pressure', 'cloud_cover', 'past_weather_1', 'past_weather_2', 'sunshine_duration'])
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'historical_csv', 'daily'), ['open_water_evaporation', 'vapour_pressure', 'wind_speed', 'snow_depth', 'pressure', 'relative_humidity', 'precipitation', 'sunshine_duration', 'tas_mean', 'tas_max', 'tas_min', 'wind_from_direction'])
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'now', '10min'), [])

    def test_list_station_elements_can_return_provider_raw_codes(self) -> None:
        stations = self._read_stations()
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'historical_csv', '10min', provider_raw=True), ['T', 'TMA', 'TMI', 'TPM', 'T10', 'T100', 'SSV10M'])

    def test_list_station_elements_can_return_mapping_table(self) -> None:
        stations = self._read_stations()
        mapping = list_station_elements(stations, '0-20000-0-11406', 'historical_csv', 'daily', include_mapping=True)
        self.assertEqual(list(mapping.columns), ['station_id', 'dataset_scope', 'resolution', 'element', 'element_raw', 'raw_elements'])
        evaporation = mapping[mapping['element'] == 'open_water_evaporation'].iloc[0]
        self.assertEqual(evaporation['element_raw'], 'VY')
        self.assertEqual(evaporation['raw_elements'], ['VY'])
        wind_speed = mapping[mapping['element'] == 'wind_speed'].iloc[0]
        self.assertEqual(wind_speed['element_raw'], 'F')
        self.assertEqual(wind_speed['raw_elements'], ['F', 'WSPD'])

    def test_find_stations_with_required_elements_for_cz_historical_csv_uses_observation_metadata(self) -> None:
        stations = self._read_stations()
        observation_metadata = pd.DataFrame([
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11406', 'begin_date': '1863-10-01T00:00Z', 'end_date': '1960-12-31T23:59Z', 'element': 'T', 'schedule': '', 'name': '', 'description': '', 'height': 2},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11406', 'begin_date': '1865-06-01T00:00Z', 'end_date': '1960-12-31T23:59Z', 'element': 'TMA', 'schedule': '', 'name': '', 'description': '', 'height': 2},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11406', 'begin_date': '1865-06-01T00:00Z', 'end_date': '1960-12-31T23:59Z', 'element': 'TMI', 'schedule': '', 'name': '', 'description': '', 'height': 2},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11406', 'begin_date': '1955-01-01T00:00Z', 'end_date': '1960-12-31T23:59Z', 'element': 'F', 'schedule': '', 'name': '', 'description': '', 'height': 10},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11406', 'begin_date': '1955-01-01T00:00Z', 'end_date': '2000-12-31T23:59Z', 'element': 'E', 'schedule': '', 'name': '', 'description': '', 'height': 2},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11406', 'begin_date': '1955-01-01T00:00Z', 'end_date': '2000-12-31T23:59Z', 'element': 'SSV', 'schedule': '', 'name': '', 'description': '', 'height': 2},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11406', 'begin_date': '1957-01-01T00:00Z', 'end_date': '1960-12-31T23:59Z', 'element': 'VY', 'schedule': '', 'name': '', 'description': '', 'height': 2},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11414', 'begin_date': '1955-01-01T00:00Z', 'end_date': '1958-07-31T23:59Z', 'element': 'T', 'schedule': '', 'name': '', 'description': '', 'height': 2},
            {'obs_type': 'DLY', 'station_id': '0-20000-0-11414', 'begin_date': '1955-01-01T00:00Z', 'end_date': '1958-07-31T23:59Z', 'element': 'TMA', 'schedule': '', 'name': '', 'description': '', 'height': 2},
        ])
        matches = find_stations_with_elements(
            country='CZ',
            provider='historical_csv',
            resolution='daily',
            elements=['tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration', 'open_water_evaporation'],
            stations=stations,
            observation_metadata=observation_metadata,
        )
        self.assertEqual(matches['station_id'].tolist(), ['0-20000-0-11406'])
        self.assertEqual(
            matches.iloc[0]['matched_elements'],
            ['tas_mean', 'tas_max', 'tas_min', 'wind_speed', 'vapour_pressure', 'sunshine_duration', 'open_water_evaporation'],
        )
        self.assertEqual(matches.iloc[0]['provider'], 'historical_csv')
        self.assertEqual(matches.iloc[0]['dataset_scope'], 'historical_csv')
        self.assertEqual(matches.iloc[0]['matching_begin_date'], '1957-01-01')
        self.assertEqual(matches.iloc[0]['matching_end_date'], '1960-12-31')

    def test_find_stations_with_required_elements_for_us_ghcnd_is_inventory_driven(self) -> None:
        stations = read_station_metadata(country='US', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        observation_metadata = read_station_observation_metadata(country='US', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        matches = find_stations_with_elements(
            country='US',
            provider='ghcnd',
            resolution='daily',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
            stations=stations,
            observation_metadata=observation_metadata,
        )
        self.assertEqual(matches['station_id'].tolist(), ['USC00000001'])
        self.assertEqual(
            matches.iloc[0]['matched_elements'],
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(matches.iloc[0]['provider'], 'ghcnd')

    def test_find_stations_with_required_elements_accepts_dataset_scope_alias(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        observation_metadata = read_station_observation_metadata(country='CA', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        matches = find_stations_with_elements(
            country='CA',
            dataset_scope='ghcnd',
            resolution='daily',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
            stations=stations,
            observation_metadata=observation_metadata,
        )
        self.assertEqual(matches['station_id'].tolist(), ['CA000000001'])

    def test_find_stations_with_required_elements_rejects_provider_level_unsupported_element(self) -> None:
        stations = read_station_metadata(country='US', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        observation_metadata = read_station_observation_metadata(country='US', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        with self.assertRaisesRegex(ValueError, "Unsupported elements for provider='ghcnd' and resolution='daily': wind_speed"):
            find_stations_with_elements(
                country='US',
                provider='ghcnd',
                resolution='daily',
                elements=['tas_mean', 'wind_speed'],
                stations=stations,
                observation_metadata=observation_metadata,
            )

    def test_find_stations_with_required_elements_requires_explicit_provider_when_country_is_ambiguous(self) -> None:
        stations = self._read_stations()
        with self.assertRaisesRegex(ValueError, "Multiple providers support country='CZ' and resolution='daily'"):
            find_stations_with_elements(
                country='CZ',
                resolution='daily',
                elements=['tas_mean'],
                stations=stations,
                observation_metadata=pd.DataFrame(columns=['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height']),
            )

    def test_ca_station_availability_and_elements_are_inventory_driven(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        availability = station_availability(stations, station_ids=['CA000000002'], country='CA')
        self.assertEqual(
            availability[['dataset_scope', 'resolution', 'supported_elements']].to_dict('records'),
            [{'dataset_scope': 'ghcnd', 'resolution': 'daily', 'supported_elements': ['precipitation']}],
        )
        self.assertTrue(station_supports(stations, 'CA000000002', 'ghcnd', 'daily', country='CA'))
        self.assertTrue(station_supports(stations, 'CA000000002', None, 'daily', country='CA', provider='ghcnd'))
        self.assertEqual(list_station_elements(stations, 'CA000000001', 'ghcnd', 'daily', country='CA'), ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'])
        self.assertEqual(list_station_elements(stations, 'CA000000001', None, 'daily', country='CA', provider='ghcnd'), ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'])
        self.assertEqual(list_station_elements(stations, 'CA000000002', 'ghcnd', 'daily', country='CA'), ['precipitation'])

    def test_cz_ghcnd_station_availability_and_elements_are_inventory_driven(self) -> None:
        stations = read_station_metadata(country='CZ', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        availability = station_availability(stations, station_ids=['EZM00011520'], country='CZ')
        self.assertEqual(
            availability[['dataset_scope', 'resolution', 'supported_elements']].to_dict('records'),
            [{'dataset_scope': 'ghcnd', 'resolution': 'daily', 'supported_elements': ['precipitation']}],
        )
        self.assertTrue(station_supports(stations, 'EZM00011520', 'ghcnd', 'daily', country='CZ'))
        self.assertTrue(station_supports(stations, 'EZM00011520', None, 'daily', country='CZ', provider='ghcnd'))
        self.assertEqual(list_station_elements(stations, 'EZM00011406', 'ghcnd', 'daily', country='CZ'), ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'])
        self.assertEqual(list_station_elements(stations, 'EZM00011406', None, 'daily', country='CZ', provider='ghcnd'), ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'])
        self.assertEqual(list_station_elements(stations, 'EZM00011520', 'ghcnd', 'daily', country='CZ'), ['precipitation'])

    def test_de_ghcnd_station_availability_and_elements_are_inventory_driven(self) -> None:
        stations = read_station_metadata(country='DE', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        availability = station_availability(stations, station_ids=['GM000000002'], country='DE')
        self.assertEqual(
            availability[['dataset_scope', 'resolution', 'supported_elements']].to_dict('records'),
            [{'dataset_scope': 'ghcnd', 'resolution': 'daily', 'supported_elements': ['precipitation']}],
        )
        self.assertTrue(station_supports(stations, 'GM000000002', 'ghcnd', 'daily', country='DE'))
        self.assertTrue(station_supports(stations, 'GM000000002', None, 'daily', country='DE', provider='ghcnd'))
        self.assertEqual(list_station_elements(stations, 'GM000000001', 'ghcnd', 'daily', country='DE'), ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'])
        self.assertEqual(list_station_elements(stations, 'GM000000001', None, 'daily', country='DE', provider='ghcnd'), ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'])
        self.assertEqual(list_station_elements(stations, 'GM000000002', 'ghcnd', 'daily', country='DE'), ['precipitation'])


if __name__ == '__main__':
    unittest.main()
