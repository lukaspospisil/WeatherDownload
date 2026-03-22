import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import ObservationQuery, download_observations, filter_stations, read_station_metadata


SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')
SAMPLE_DAILY_TMA = Path('tests/data/sample_daily_tma.csv').read_text(encoding='utf-8')
SAMPLE_TENMIN_T = Path('tests/data/sample_10min_t_202401.csv').read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class DailyWorkflowIntegrationTests(unittest.TestCase):
    def test_metadata_selection_and_daily_download_workflow(self) -> None:
        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/metadata/meta1.csv'):
                return _MockResponse(SAMPLE_META1)
            if url.endswith('/temperature/dly-0-20000-0-11406-TMA.csv'):
                return _MockResponse(SAMPLE_DAILY_TMA)
            return _MockResponse('', status_code=404)

        with patch('weatherdownload.metadata.requests.get', side_effect=fake_get):
            stations = read_station_metadata()
        selected = filter_stations(stations, station_ids=['0-20000-0-11406'])
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=selected['station_id'].tolist(), start_date='1865-06-01', end_date='1865-06-03', elements=['TMA'])
        with patch('weatherdownload.chmi_daily.requests.get', side_effect=fake_get):
            result = download_observations(query, station_metadata=selected)
        self.assertEqual(list(result['station_id'].unique()), ['0-20000-0-11406'])
        self.assertEqual(list(result['gh_id'].unique()), ['L3CHEB01'])
        self.assertEqual(list(result['element'].unique()), ['TMA'])
        self.assertEqual(str(result.iloc[0]['observation_date']), '1865-06-01')

    def test_metadata_selection_and_tenmin_download_workflow(self) -> None:
        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/metadata/meta1.csv'):
                return _MockResponse(SAMPLE_META1)
            if url.endswith('/temperature/2024/10m-0-20000-0-11406-T-202401.csv'):
                return _MockResponse(SAMPLE_TENMIN_T)
            return _MockResponse('', status_code=404)

        with patch('weatherdownload.metadata.requests.get', side_effect=fake_get):
            stations = read_station_metadata()
        selected = filter_stations(stations, station_ids=['0-20000-0-11406'])
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=selected['station_id'].tolist(), start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['T'])
        with patch('weatherdownload.chmi_tenmin.requests.get', side_effect=fake_get):
            result = download_observations(query, station_metadata=selected)
        self.assertEqual(list(result['station_id'].unique()), ['0-20000-0-11406'])
        self.assertEqual(list(result['gh_id'].unique()), ['L3CHEB01'])
        self.assertEqual(list(result['element'].unique()), ['T'])
        self.assertEqual(str(result.iloc[0]['timestamp']), '2024-01-01 00:00:00+00:00')


if __name__ == '__main__':
    unittest.main()
