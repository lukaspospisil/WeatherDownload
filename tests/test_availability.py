import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import list_station_elements, list_station_paths, read_station_metadata, station_availability, station_supports


SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')


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
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)):
            return read_station_metadata()

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

    def test_list_station_paths_can_include_elements(self) -> None:
        stations = self._read_stations()
        paths = list_station_paths(stations, '0-20000-0-11406', include_elements=True)
        self.assertIn('supported_elements', paths.columns)
        self.assertEqual(paths.iloc[0]['station_id'], '0-20000-0-11406')

    def test_list_station_elements_for_implemented_path(self) -> None:
        stations = self._read_stations()
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'historical_csv', '10min'), ['T', 'TMA', 'TMI', 'TPM', 'T10', 'T100', 'SSV10M'])
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'historical_csv', '1hour'), ['E', 'P', 'N', 'W1', 'W2', 'SSV1H'])
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'now', '10min'), [])


if __name__ == '__main__':
    unittest.main()
