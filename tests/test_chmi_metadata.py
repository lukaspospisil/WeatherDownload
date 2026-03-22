import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import filter_stations, read_station_metadata, read_station_observation_metadata


SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')
SAMPLE_META2 = Path('tests/data/sample_meta2.csv').read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class StationMetadataTests(unittest.TestCase):
    def test_read_station_metadata(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)):
            stations = read_station_metadata()
        self.assertEqual(len(stations), 2)
        self.assertEqual(stations.iloc[0]['station_id'], '0-20000-0-11406')
        self.assertEqual(stations.iloc[0]['gh_id'], 'L3CHEB01')

    def test_read_station_observation_metadata(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META2)):
            observation_metadata = read_station_observation_metadata()
        self.assertEqual(list(observation_metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(observation_metadata.iloc[0]['obs_type'], 'DLY')
        self.assertEqual(observation_metadata.iloc[0]['station_id'], '0-20000-0-11406')
        self.assertEqual(observation_metadata.iloc[0]['element'], 'TMA')
        self.assertEqual(observation_metadata.iloc[1]['schedule'], 'AVG,07:00,14:00,21:00')

    def test_filter_stations_by_station_id(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)):
            stations = read_station_metadata()
        filtered = filter_stations(stations, station_ids=['0-20000-0-11406'])
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]['station_id'], '0-20000-0-11406')

    def test_filter_stations_by_gh_id(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)):
            stations = read_station_metadata()
        filtered = filter_stations(stations, gh_ids=['L3KVAL01'])
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]['gh_id'], 'L3KVAL01')

    def test_filter_stations_by_name_substring(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)):
            stations = read_station_metadata()
        filtered = filter_stations(stations, name_contains='olsova')
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]['station_id'], '0-20000-0-11414')

    def test_filter_stations_by_bbox(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)):
            stations = read_station_metadata()
        filtered = filter_stations(stations, bbox=(12.8, 50.1, 13.0, 50.3))
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]['station_id'], '0-20000-0-11414')

    def test_filter_stations_by_active_on(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)):
            stations = read_station_metadata()
        filtered = filter_stations(stations, active_on='2024-01-01')
        self.assertEqual(filtered['station_id'].tolist(), ['0-20000-0-11406'])


if __name__ == '__main__':
    unittest.main()
