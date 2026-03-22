import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_dataset_scopes,
    list_supported_countries,
    normalize_country_code,
    read_station_metadata,
)

SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class ProviderTests(unittest.TestCase):
    def test_supported_countries_and_normalization(self) -> None:
        self.assertEqual(list_supported_countries(), ['CZ', 'DE'])
        self.assertEqual(normalize_country_code('de'), 'DE')
        self.assertEqual(normalize_country_code(None), 'CZ')

    def test_read_station_metadata_country_de_not_implemented(self) -> None:
        with self.assertRaises(NotImplementedError):
            read_station_metadata(country='DE')

    def test_list_dataset_scopes_country_de_is_empty_placeholder(self) -> None:
        self.assertEqual(list_dataset_scopes(country='DE'), [])

    def test_download_observations_country_de_not_implemented(self) -> None:
        query = ObservationQuery(
            dataset_scope='historical_csv',
            resolution='daily',
            station_ids=['station-1'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['T'],
        )
        with self.assertRaises(NotImplementedError):
            download_observations(query, country='DE')

    def test_read_station_metadata_preserves_positional_source_url_compatibility(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)) as mock_get:
            stations = read_station_metadata('https://example.test/meta1.csv')
        mock_get.assert_called_once_with('https://example.test/meta1.csv', timeout=60)
        self.assertEqual(stations.iloc[0]['station_id'], '0-20000-0-11406')


if __name__ == '__main__':
    unittest.main()
