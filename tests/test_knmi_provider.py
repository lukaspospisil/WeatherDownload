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


class KnmiProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self._environment = patch.dict(os.environ, {'WEATHERDOWNLOAD_KNMI_API_KEY': 'test-key'}, clear=False)
        self._environment.start()

    def tearDown(self) -> None:
        self._environment.stop()

    def test_supported_countries_include_nl(self) -> None:
        self.assertIn('NL', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='NL'), ['historical'])
        self.assertEqual(list_resolutions(country='NL', dataset_scope='historical'), ['daily'])

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
        self.assertIn('SQ', metadata['element'].tolist())

    def test_discovery_country_nl_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='NL', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration', 'wind_speed', 'pressure', 'relative_humidity'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', dataset_scope='historical', resolution='daily', provider_raw=True),
            ['TG', 'TX', 'TN', 'RH', 'SQ', 'FG', 'PG', 'UG'],
        )

    def test_nl_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(
            country='NL',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['0-20000-0-06260'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation'],
        )
        raw_query = ObservationQuery(
            country='NL',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['0-20000-0-06260'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['TG', 'RH'],
        )
        self.assertEqual(canonical_query.elements, ['TG', 'RH'])
        self.assertEqual(raw_query.elements, ['TG', 'RH'])

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

    def test_nl_provider_fails_early_when_api_key_is_missing(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, r'KNMI Open Data API key is required'):
                read_station_metadata(country='NL')


if __name__ == '__main__':
    unittest.main()

