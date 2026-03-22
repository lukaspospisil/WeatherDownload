import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_dataset_scopes,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    normalize_country_code,
    read_station_metadata,
    read_station_observation_metadata,
)

SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')
SAMPLE_DWD_STATIONS = '''Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe
----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------
00003 18910101 20241231 202 50.7827 6.0941 Aachen Baden-W\xfcrttemberg Frei
00044 20070401 20241231 79 52.9336 8.2370 Alfhausen Niedersachsen Frei
'''.encode('latin-1')


class _MockResponse:
    def __init__(self, text: str | None = None, status_code: int = 200, content: bytes | None = None) -> None:
        self.text = text or ''
        self.status_code = status_code
        self.encoding = 'utf-8'
        self.content = content if content is not None else self.text.encode('utf-8')

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class ProviderTests(unittest.TestCase):
    def test_supported_countries_and_normalization(self) -> None:
        self.assertEqual(list_supported_countries(), ['CZ', 'DE'])
        self.assertEqual(normalize_country_code('de'), 'DE')
        self.assertEqual(normalize_country_code(None), 'CZ')

    def test_read_station_metadata_country_de(self) -> None:
        with patch('weatherdownload.dwd_metadata.requests.get', return_value=_MockResponse(content=SAMPLE_DWD_STATIONS)):
            stations = read_station_metadata(country='DE')
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations.iloc[0]['station_id'], '00003')
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertIn('W\u00fcrttemberg', stations.iloc[0]['full_name'])
        self.assertNotIn('\ufffd', stations.iloc[0]['full_name'])

    def test_read_station_observation_metadata_country_de(self) -> None:
        with patch('weatherdownload.dwd_metadata.requests.get', return_value=_MockResponse(content=SAMPLE_DWD_STATIONS)):
            observation_metadata = read_station_observation_metadata(country='DE')
        self.assertEqual(list(observation_metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertIn('TMK', observation_metadata['element'].tolist())
        self.assertIn('TT_TU', observation_metadata['element'].tolist())
        self.assertIn('RWS_10', observation_metadata['element'].tolist())
        self.assertTrue(observation_metadata['station_id'].str.len().eq(5).all())

    def test_discovery_country_de_returns_canonical_names_by_default(self) -> None:
        self.assertEqual(list_dataset_scopes(country='DE'), ['historical'])
        self.assertEqual(list_resolutions(country='DE', dataset_scope='historical'), ['10min', '1hour', 'daily'])
        hourly_elements = list_supported_elements(country='DE', dataset_scope='historical', resolution='1hour')
        self.assertIn('tas_mean', hourly_elements)
        self.assertIn('wind_speed', hourly_elements)
        self.assertIn('precipitation', hourly_elements)

    def test_discovery_country_de_can_return_raw_codes(self) -> None:
        hourly_elements = list_supported_elements(country='DE', dataset_scope='historical', resolution='1hour', provider_raw=True)
        self.assertIn('TT_TU', hourly_elements)
        self.assertIn('FF', hourly_elements)
        self.assertIn('R1', hourly_elements)

    def test_download_observations_country_de_hourly_not_implemented(self) -> None:
        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='1hour',
            station_ids=['00003'],
            start='2024-01-01T00:00:00Z',
            end='2024-01-01T01:00:00Z',
            elements=['tas_mean'],
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
