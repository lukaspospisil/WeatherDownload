import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    get_provider,
    list_providers,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
)
from weatherdownload.providers.pt.ipma_parser import IPMA_NORMALIZED_SUBDAILY_COLUMNS

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_IPMA_STATIONS_PATH = Path('tests/data/sample_pt_ipma_stations.json')
SAMPLE_IPMA_OBSERVATIONS_PATH = Path('tests/data/sample_pt_ipma_observations.json')
SAMPLE_IPMA_STATIONS_TEXT = SAMPLE_IPMA_STATIONS_PATH.read_text(encoding='utf-8')
SAMPLE_IPMA_OBSERVATIONS_TEXT = SAMPLE_IPMA_OBSERVATIONS_PATH.read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        return None


class PortugalProviderTests(unittest.TestCase):
    def test_provider_capability_metadata_includes_ghcnd_and_ipma(self) -> None:
        provider = get_provider('PT')
        self.assertEqual(provider.supported_country_codes, ('PT',))
        self.assertEqual(provider.supported_providers, ('ghcnd', 'ipma'))
        self.assertEqual(provider.supported_resolutions, ('daily', '1hour'))

    def test_supported_countries_include_pt(self) -> None:
        self.assertIn('PT', list_supported_countries())

    def test_discovery_country_pt_returns_ghcnd_daily_and_ipma_hourly_without_evap(self) -> None:
        self.assertEqual(list_providers(country='PT'), ['ghcnd', 'ipma'])
        self.assertIn('ghcnd', list_providers(country='PT'))
        self.assertIn('ipma', list_providers(country='PT'))
        self.assertIn('daily', list_resolutions(country='PT', provider='ghcnd'))
        self.assertIn('1hour', list_resolutions(country='PT', provider='ipma'))
        self.assertEqual(
            list_supported_elements(country='PT', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='PT', provider='ipma', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity'],
        )
        self.assertNotIn(
            'open_water_evaporation',
            list_supported_elements(country='PT', provider='ghcnd', resolution='daily'),
        )
        self.assertNotIn(
            'open_water_evaporation',
            list_supported_elements(country='PT', provider='ipma', resolution='1hour'),
        )
        self.assertNotIn(
            'pressure',
            list_supported_elements(country='PT', provider='ipma', resolution='1hour'),
        )

    def test_station_metadata_reader_filters_ghcnd_stations_by_po_prefix(self) -> None:
        stations = read_station_metadata(country='PT', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['PO000000001', 'PO000000002'])

    def test_station_metadata_reader_reads_ipma_station_fields(self) -> None:
        stations = read_station_metadata(country='PT', source_url=str(SAMPLE_IPMA_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['1234567', '2345678', '3456789'])
        self.assertEqual(stations.iloc[0]['full_name'], 'Porto')
        self.assertAlmostEqual(float(stations.iloc[0]['longitude']), -8.6133)
        self.assertAlmostEqual(float(stations.iloc[0]['latitude']), 41.1413)
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertTrue(stations['elevation_m'].isna().all())

    def test_download_observations_country_pt_ipma_hourly_from_sample_payload(self) -> None:
        def fake_get(url: str, timeout: int = 60) -> _MockResponse:
            if url.endswith('/stations.json'):
                return _MockResponse(SAMPLE_IPMA_STATIONS_TEXT)
            if url.endswith('/observations.json'):
                return _MockResponse(SAMPLE_IPMA_OBSERVATIONS_TEXT)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='PT',
            provider='ipma',
            resolution='1hour',
            station_ids=['1234567', '2345678'],
            start='2026-05-25T04:00:00',
            end='2026-05-25T05:00:00',
            elements=['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity'],
        )
        with patch('weatherdownload.providers.pt.ipma_parser.requests.get', side_effect=fake_get):
            stations = read_station_metadata(country='PT', source_url=str(SAMPLE_IPMA_STATIONS_PATH))
        with patch('weatherdownload.providers.pt.ipma_parser.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PT', station_metadata=stations)

        self.assertEqual(list(observations.columns), IPMA_NORMALIZED_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['station_id'].unique().tolist()), ['1234567', '2345678'])
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['precipitation', 'relative_humidity', 'tas_mean', 'wind_speed'],
        )
        self.assertEqual(
            sorted(observations['element_raw'].unique().tolist()),
            ['humidade', 'intensidadeVento', 'precAcumulada', 'temperatura'],
        )
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertTrue(all(value.tzinfo is None for value in observations['timestamp']))
        self.assertTrue(pd.isna(observations[(observations['station_id'] == '2345678') & (observations['element'] == 'precipitation')].iloc[0]['value']))
        self.assertTrue(
            pd.isna(
                observations[
                    (observations['station_id'] == '1234567')
                    & (observations['timestamp'] == pd.Timestamp('2026-05-25T05:00:00'))
                    & (observations['element'] == 'wind_speed')
                ].iloc[0]['value']
            )
        )
        self.assertFalse((observations['station_id'] == '3456789').any())
        self.assertEqual(observations['timestamp'].min(), pd.Timestamp('2026-05-25T04:00:00'))
        self.assertEqual(observations['timestamp'].max(), pd.Timestamp('2026-05-25T05:00:00'))


if __name__ == '__main__':
    unittest.main()
