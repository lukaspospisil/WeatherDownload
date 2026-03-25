import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_dataset_scopes,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_knmi_station_metadata.csv')
SAMPLE_FILES_JSON = Path('tests/data/sample_knmi_files.json').read_text(encoding='utf-8')
SAMPLE_HOURLY_FILES_JSON = json.dumps(
    {
        'files': [
            {'filename': 'hourly-observations-20240101-01.nc'},
            {'filename': 'hourly-observations-20240101-02.nc'},
        ]
    }
)


class KnmiProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self._environment = patch.dict(os.environ, {'WEATHERDOWNLOAD_KNMI_API_KEY': 'test-key'}, clear=False)
        self._environment.start()

    def tearDown(self) -> None:
        self._environment.stop()

    def test_supported_countries_include_nl(self) -> None:
        self.assertIn('NL', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='NL'), ['historical'])
        self.assertEqual(list_resolutions(country='NL', dataset_scope='historical'), ['1hour', 'daily'])

    def test_read_station_metadata_country_nl_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='NL', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['0-20000-0-06260', '0-20000-0-06310'])
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_nl_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='NL', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertIn('TG', metadata['element'].tolist())
        self.assertIn('T', metadata['element'].tolist())
        self.assertIn('Daily mean air temperature', metadata['name'].tolist())
        self.assertIn('Hourly air temperature', metadata['name'].tolist())

    def test_discovery_country_nl_returns_daily_and_hourly_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='NL', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration', 'wind_speed', 'pressure', 'relative_humidity'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', dataset_scope='historical', resolution='daily', provider_raw=True),
            ['TG', 'TX', 'TN', 'RH', 'SQ', 'FG', 'PG', 'UG'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', dataset_scope='historical', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', dataset_scope='historical', resolution='1hour', provider_raw=True),
            ['T', 'RH', 'FH', 'U', 'P', 'SQ'],
        )

    def test_nl_queries_accept_canonical_and_raw_codes(self) -> None:
        daily_query = ObservationQuery(
            country='NL',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['0-20000-0-06260'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation'],
        )
        hourly_query = ObservationQuery(
            country='NL',
            dataset_scope='historical',
            resolution='1hour',
            station_ids=['0-20000-0-06260'],
            start='2024-01-01T01:00:00Z',
            end='2024-01-01T02:00:00Z',
            elements=['tas_mean', 'pressure'],
        )
        raw_query = ObservationQuery(
            country='NL',
            dataset_scope='historical',
            resolution='1hour',
            station_ids=['0-20000-0-06260'],
            start='2024-01-01T01:00:00Z',
            end='2024-01-01T02:00:00Z',
            elements=['T', 'P'],
        )
        self.assertEqual(daily_query.elements, ['TG', 'RH'])
        self.assertEqual(hourly_query.elements, ['T', 'P'])
        self.assertEqual(raw_query.elements, ['T', 'P'])

    def test_nl_daily_download_uses_api_listing_and_normalizes_output(self) -> None:
        station_metadata = read_station_metadata(country='NL', source_url=str(SAMPLE_STATIONS_PATH))
        parsed_payloads = {
            'daily-observations-20240101.nc': {
                'observation_date': pd.Timestamp('2024-01-01').date(),
                'stations': pd.DataFrame([
                    {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                    {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
                ]),
                'variables': {
                    'TG': pd.Series([3.4, 5.6]),
                    'RH': pd.Series([1.2, 0.0]),
                },
            },
            'daily-observations-20240102.nc': {
                'observation_date': pd.Timestamp('2024-01-02').date(),
                'stations': pd.DataFrame([
                    {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                    {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
                ]),
                'variables': {
                    'TG': pd.Series([4.1, 6.2]),
                    'RH': pd.Series([0.5, 0.1]),
                },
            },
        }
        filename_order = iter(parsed_payloads.keys())

        with patch('weatherdownload.knmi_daily.list_knmi_files', return_value=json.loads(SAMPLE_FILES_JSON)):
            with patch('weatherdownload.knmi_daily.download_knmi_file_bytes', side_effect=lambda **kwargs: next(filename_order).encode('utf-8')):
                with patch('weatherdownload.knmi_daily.parse_knmi_daily_netcdf_bytes', side_effect=lambda payload: parsed_payloads[payload.decode('utf-8')]):
                    query = ObservationQuery(
                        country='NL',
                        dataset_scope='historical',
                        resolution='daily',
                        station_ids=['0-20000-0-06260'],
                        start_date='2024-01-01',
                        end_date='2024-01-02',
                        elements=['tas_mean', 'precipitation'],
                    )
                    observations = download_observations(query, country='NL', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'dataset_scope', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['RH', 'TG'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(observations.iloc[0]['observation_date'].isoformat(), '2024-01-01')

    def test_nl_hourly_download_uses_validated_dataset_and_normalizes_output(self) -> None:
        station_metadata = read_station_metadata(country='NL', source_url=str(SAMPLE_STATIONS_PATH))
        parsed_payloads = {
            'hourly-observations-20240101-01.nc': {
                'timestamp': pd.Timestamp('2024-01-01T01:00:00Z'),
                'stations': pd.DataFrame([
                    {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                    {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
                ]),
                'variables': {
                    'T': pd.Series([3.1, 4.2]),
                    'P': pd.Series([1012.1, 1011.2]),
                },
            },
            'hourly-observations-20240101-02.nc': {
                'timestamp': pd.Timestamp('2024-01-01T02:00:00Z'),
                'stations': pd.DataFrame([
                    {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                    {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
                ]),
                'variables': {
                    'T': pd.Series([3.8, 4.9]),
                    'P': pd.Series([1011.7, 1010.8]),
                },
            },
        }
        filename_order = iter(parsed_payloads.keys())

        with patch('weatherdownload.knmi_hourly.list_knmi_files', return_value=json.loads(SAMPLE_HOURLY_FILES_JSON)):
            with patch('weatherdownload.knmi_hourly.download_knmi_file_bytes', side_effect=lambda **kwargs: next(filename_order).encode('utf-8')):
                with patch('weatherdownload.knmi_hourly.parse_knmi_hourly_netcdf_bytes', side_effect=lambda payload: parsed_payloads[payload.decode('utf-8')]):
                    query = ObservationQuery(
                        country='NL',
                        dataset_scope='historical',
                        resolution='1hour',
                        station_ids=['0-20000-0-06260'],
                        start='2024-01-01T01:00:00Z',
                        end='2024-01-01T02:00:00Z',
                        elements=['tas_mean', 'pressure'],
                    )
                    observations = download_observations(query, country='NL', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'dataset_scope', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['P', 'T'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(observations.iloc[0]['timestamp'].isoformat(), '2024-01-01T01:00:00+00:00')

    def test_nl_provider_fails_early_when_api_key_is_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, r'KNMI Open Data API key is required'):
                read_station_metadata(country='NL')


if __name__ == '__main__':
    unittest.main()
