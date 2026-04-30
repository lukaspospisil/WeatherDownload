import unittest
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    get_provider,
    list_providers,
    list_resolutions,
    list_station_elements,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.ca.eccc_parser import CA_ECCC_NORMALIZED_DAILY_COLUMNS, CA_ECCC_NORMALIZED_HOURLY_COLUMNS
from weatherdownload.providers.ca.observations import _build_eccc_daily_params, _iter_month_chunks
from weatherdownload.providers.ca.metadata import read_station_metadata_eccc


SAMPLE_ECCC_DAILY_PATH = Path('tests/data/sample_ca_eccc_daily.json')
SAMPLE_ECCC_HOURLY_PATH = Path('tests/data/sample_ca_eccc_hourly.json')
SAMPLE_GHCND_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')


class CanadaEcccProviderTests(unittest.TestCase):
    def test_provider_capability_metadata_includes_eccc_and_ghcnd(self) -> None:
        provider = get_provider('CA')
        self.assertEqual(provider.supported_country_codes, ('CA',))
        self.assertEqual(provider.supported_providers, ('eccc', 'ghcnd'))
        self.assertEqual(provider.supported_resolutions, ('daily', '1hour'))

    def test_discovery_country_ca_includes_eccc_daily_and_hourly(self) -> None:
        self.assertEqual(list_providers(country='CA'), ['eccc', 'ghcnd'])
        self.assertEqual(list_resolutions(country='CA', provider='eccc'), ['1hour', 'daily'])
        self.assertEqual(
            list_supported_elements(country='CA', provider='eccc', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='CA', provider='eccc', resolution='daily', provider_raw=True),
            ['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'],
        )
        mapping = list_supported_elements(country='CA', provider='eccc', resolution='daily', include_mapping=True)
        self.assertEqual(
            mapping[['element', 'element_raw']].to_dict('records'),
            [
                {'element': 'tas_mean', 'element_raw': 'MEAN_TEMPERATURE'},
                {'element': 'tas_max', 'element_raw': 'MAX_TEMPERATURE'},
                {'element': 'tas_min', 'element_raw': 'MIN_TEMPERATURE'},
                {'element': 'precipitation', 'element_raw': 'TOTAL_PRECIPITATION'},
            ],
        )
        self.assertEqual(
            list_supported_elements(country='CA', provider='eccc', resolution='1hour'),
            ['tas_mean', 'relative_humidity', 'wind_speed', 'precipitation', 'pressure'],
        )
        self.assertEqual(
            list_supported_elements(country='CA', provider='eccc', resolution='1hour', provider_raw=True),
            ['TEMP', 'RELATIVE_HUMIDITY', 'WIND_SPEED', 'PRECIP_AMOUNT', 'STATION_PRESSURE'],
        )

    def test_read_station_metadata_and_observation_metadata_from_eccc_fixture(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['1021330'])
        self.assertEqual(stations.iloc[0]['begin_date'], '2025-01-01T00:00Z')
        self.assertEqual(stations.iloc[0]['end_date'], '2025-01-03T23:59Z')
        self.assertEqual(stations.iloc[0]['full_name'], 'TEST STATION')
        self.assertTrue(stations['gh_id'].isna().all())

        metadata = read_station_observation_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        self.assertEqual(
            metadata[['station_id', 'element']].to_dict('records'),
            [
                {'station_id': '1021330', 'element': 'MAX_TEMPERATURE'},
                {'station_id': '1021330', 'element': 'MEAN_TEMPERATURE'},
                {'station_id': '1021330', 'element': 'MIN_TEMPERATURE'},
                {'station_id': '1021330', 'element': 'TOTAL_PRECIPITATION'},
            ],
        )

    def test_eccc_query_normalizes_canonical_and_raw_elements(self) -> None:
        canonical_query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='daily',
            station_ids=['1021330'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        raw_query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='daily',
            station_ids=['1021330'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'],
        )
        self.assertEqual(canonical_query.elements, ['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'])
        self.assertEqual(raw_query.elements, ['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'])

    def test_eccc_hourly_query_normalizes_canonical_and_raw_elements(self) -> None:
        canonical_query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='1hour',
            station_ids=['1017101'],
            start='2024-10-02T09:00:00Z',
            end='2024-10-02T10:00:00Z',
            elements=['tas_mean', 'relative_humidity'],
        )
        raw_query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='1hour',
            station_ids=['1017101'],
            start='2024-10-02T09:00:00Z',
            end='2024-10-02T10:00:00Z',
            elements=['TEMP', 'RELATIVE_HUMIDITY'],
        )
        self.assertEqual(canonical_query.elements, ['TEMP', 'RELATIVE_HUMIDITY'])
        self.assertEqual(raw_query.elements, ['TEMP', 'RELATIVE_HUMIDITY'])

    def test_download_observations_reads_local_eccc_fixture_via_station_metadata_source(self) -> None:
        station_metadata = read_station_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='daily',
            station_ids=['1021330'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        observations = download_observations(query, country='CA', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), CA_ECCC_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max', 'tas_mean', 'tas_min'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['MAX_TEMPERATURE', 'MEAN_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'])
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', query.start_date)]), 5.4)
        self.assertAlmostEqual(float(lookup[('tas_min', query.start_date)]), 1.1)
        self.assertAlmostEqual(float(lookup[('tas_max', query.end_date)]), 3.2)
        self.assertAlmostEqual(float(lookup[('precipitation', query.end_date)]), 0.0)
        self.assertNotIn(('tas_mean', query.end_date), lookup.index)

    def test_station_elements_for_eccc_fixture_are_mapping_driven(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        self.assertEqual(
            list_station_elements(stations, '1021330', 'eccc', 'daily', country='CA'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )

    def test_station_elements_for_eccc_hourly_are_conservative_when_not_marked_hourly(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        self.assertEqual(
            list_station_elements(stations, '1021330', 'eccc', '1hour', country='CA'),
            [],
        )

    def test_ca_ghcnd_metadata_path_remains_available(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['CA000000001', 'CA000000002'])

    def test_eccc_live_month_chunking_and_params_are_stable(self) -> None:
        chunks = _iter_month_chunks(date(2025, 1, 31), date(2025, 2, 2))
        self.assertEqual(chunks, [(date(2025, 1, 31), date(2025, 1, 31)), (date(2025, 2, 1), date(2025, 2, 2))])

        params = _build_eccc_daily_params(
            station_id='1021330',
            chunk_start=date(2025, 1, 1),
            chunk_end=date(2025, 1, 2),
            limit=5000,
        )
        self.assertEqual(params['f'], 'json')
        self.assertEqual(params['CLIMATE_IDENTIFIER'], '1021330')
        self.assertEqual(params['datetime'], '2025-01-01/2025-01-02')
        self.assertIn('LOCAL_DATE', params['properties'])
        self.assertEqual(params['limit'], '5000')

    def test_download_observations_can_fetch_live_eccc_with_pagination(self) -> None:
        fixture_payload = json.loads(SAMPLE_ECCC_DAILY_PATH.read_text(encoding='utf-8'))
        features = fixture_payload['features']
        page1 = {
            'type': 'FeatureCollection',
            'features': features[:2],
            'links': [
                {
                    'rel': 'next',
                    'href': 'https://api.weather.gc.ca/collections/climate-daily/items?offset=2',
                }
            ],
        }
        page2 = {'type': 'FeatureCollection', 'features': features[2:], 'links': []}

        class _MockResponse:
            def __init__(self, text: str) -> None:
                self.text = text
                self.status_code = 200
                self.encoding = 'utf-8'

            def raise_for_status(self) -> None:
                return None

        base_url = 'https://api.weather.gc.ca/collections/climate-daily/items'
        next_url = 'https://api.weather.gc.ca/collections/climate-daily/items?offset=2'

        def _mock_get(url: str, params=None, timeout: int = 60):
            if url == base_url and params is not None:
                return _MockResponse(json.dumps(page1))
            if url == next_url and params is None:
                return _MockResponse(json.dumps(page2))
            raise AssertionError(f'unexpected request: url={url!r} params={params!r}')

        station_metadata = pd.DataFrame.from_records([{'station_id': '1021330'}])
        query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='daily',
            station_ids=['1021330'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )

        with patch('weatherdownload.providers.ca.observations.requests.get', side_effect=_mock_get) as mock_get:
            observations = download_observations(query, country='CA', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), CA_ECCC_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(observations['station_id'].unique().tolist(), ['1021330'])
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max', 'tas_mean', 'tas_min'])
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', query.start_date)]), 5.4)
        self.assertAlmostEqual(float(lookup[('tas_min', query.start_date)]), 1.1)
        self.assertAlmostEqual(float(lookup[('tas_max', query.end_date)]), 3.2)
        self.assertAlmostEqual(float(lookup[('precipitation', query.end_date)]), 0.0)
        self.assertNotIn(('tas_mean', query.end_date), lookup.index)
        self.assertEqual(mock_get.call_count, 2)

    def test_read_station_metadata_eccc_can_fetch_live_with_pagination(self) -> None:
        page1 = {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': [-123.1, 49.3]},
                    'properties': {
                        'CLIMATE_IDENTIFIER': '1021330',
                        'STATION_NAME': 'TEST STATION A',
                        'DLY_FIRST_DATE': '1990-01-01T00:00Z',
                        'DLY_LAST_DATE': '1990-12-31T23:59Z',
                        'ELEVATION': 12.5,
                    },
                }
            ],
            'links': [{'rel': 'next', 'href': 'https://api.weather.gc.ca/collections/climate-stations/items?offset=1'}],
        }
        page2 = {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': [-75.7, 45.4]},
                    'properties': {
                        'CLIMATE_IDENTIFIER': '1022430',
                        'STATION_NAME': 'TEST STATION B',
                        'DLY_FIRST_DATE': '1991-05-04T00:00Z',
                        'DLY_LAST_DATE': '1991-05-05T23:59Z',
                        'ELEVATION': 70,
                    },
                }
            ],
            'links': [],
        }

        class _MockResponse:
            def __init__(self, text: str) -> None:
                self.text = text
                self.status_code = 200
                self.encoding = 'utf-8'

            def raise_for_status(self) -> None:
                return None

        base_url = 'https://api.weather.gc.ca/collections/climate-stations/items'
        next_url = 'https://api.weather.gc.ca/collections/climate-stations/items?offset=1'

        def _mock_get(url: str, params=None, timeout: int = 60):
            if url == base_url and params is not None:
                self.assertEqual(params.get('f'), 'json')
                self.assertEqual(params.get('limit'), '5000')
                self.assertIn('CLIMATE_IDENTIFIER', params.get('properties', ''))
                return _MockResponse(json.dumps(page1))
            if url == next_url and params is None:
                return _MockResponse(json.dumps(page2))
            raise AssertionError(f'unexpected request: url={url!r} params={params!r}')

        with patch('weatherdownload.providers.ca.metadata.requests.get', side_effect=_mock_get) as mock_get:
            stations = read_station_metadata_eccc(source_url=None, timeout=60)

        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['1021330', '1022430'])
        self.assertEqual(stations.iloc[0]['full_name'], 'TEST STATION A')
        self.assertEqual(stations.iloc[0]['begin_date'], '1990-01-01T00:00Z')
        self.assertEqual(stations.iloc[0]['end_date'], '1990-12-31T23:59Z')
        self.assertAlmostEqual(float(stations.iloc[0]['longitude']), -123.1)
        self.assertAlmostEqual(float(stations.iloc[0]['latitude']), 49.3)
        self.assertAlmostEqual(float(stations.iloc[0]['elevation_m']), 12.5)
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(mock_get.call_count, 2)

    def test_download_observations_can_fetch_live_hourly_eccc_with_pagination_and_empty_next_termination(self) -> None:
        fixture_payload = json.loads(SAMPLE_ECCC_HOURLY_PATH.read_text(encoding='utf-8'))
        features = fixture_payload['features']
        page1 = {
            'type': 'FeatureCollection',
            'features': features,
            'links': [
                {
                    'rel': 'next',
                    'href': 'https://api.weather.gc.ca/collections/climate-hourly/items?offset=2',
                }
            ],
        }
        page2 = {'type': 'FeatureCollection', 'features': [], 'links': []}

        class _MockResponse:
            def __init__(self, text: str) -> None:
                self.text = text
                self.status_code = 200
                self.encoding = 'utf-8'

            def raise_for_status(self) -> None:
                return None

        base_url = 'https://api.weather.gc.ca/collections/climate-hourly/items'
        next_url = 'https://api.weather.gc.ca/collections/climate-hourly/items?offset=2'

        def _mock_get(url: str, params=None, timeout: int = 60):
            if url == base_url and params is not None:
                self.assertEqual(params.get('f'), 'json')
                self.assertIn('CLIMATE_IDENTIFIER', params)
                self.assertIn('datetime', params)
                self.assertIn('properties', params)
                return _MockResponse(json.dumps(page1))
            if url == next_url and params is None:
                return _MockResponse(json.dumps(page2))
            raise AssertionError(f'unexpected request: url={url!r} params={params!r}')

        station_metadata = pd.DataFrame.from_records([{'station_id': '1017101'}])
        query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='1hour',
            station_ids=['1017101'],
            start='2024-10-02T09:00:00Z',
            end='2024-10-02T10:00:00Z',
            elements=['tas_mean', 'relative_humidity', 'wind_speed', 'pressure', 'precipitation'],
        )

        with patch('weatherdownload.providers.ca.hourly.requests.get', side_effect=_mock_get) as mock_get:
            observations = download_observations(query, country='CA', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), CA_ECCC_NORMALIZED_HOURLY_COLUMNS)
        self.assertEqual(observations['station_id'].unique().tolist(), ['1017101'])
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['precipitation', 'pressure', 'relative_humidity', 'tas_mean', 'wind_speed'],
        )
        self.assertEqual(mock_get.call_count, 2)


if __name__ == '__main__':
    unittest.main()
