import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.es.parser import (
    parse_aemet_coordinate,
    parse_aemet_daily_data_json,
    parse_aemet_numeric,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_es_aemet_stations.json')
SAMPLE_DAILY_TEXT = Path('tests/data/sample_es_aemet_daily.json').read_text(encoding='utf-8')


class EsProviderTests(unittest.TestCase):
    def test_supported_countries_include_es(self) -> None:
        self.assertIn('ES', list_supported_countries())
        self.assertEqual(list_providers(country='ES'), ['aemet'])
        self.assertEqual(list_resolutions(country='ES', provider='aemet'), ['daily'])
        self.assertNotIn('1hour', list_resolutions(country='ES', provider='aemet'))
        self.assertNotIn('10min', list_resolutions(country='ES', provider='aemet'))

    def test_discovery_country_es_returns_daily_only_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='ES', provider='aemet', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='ES', provider='aemet', resolution='daily', provider_raw=True),
            ['tmed', 'tmax', 'tmin', 'prec', 'velmedia', 'hrMedia', 'sol'],
        )
        self.assertNotIn('vapour_pressure', list_supported_elements(country='ES', provider='aemet', resolution='daily'))

    def test_es_queries_accept_canonical_and_raw_codes(self) -> None:
        daily_query = ObservationQuery(
            country='ES',
            provider='aemet',
            resolution='daily',
            station_ids=['3195'],
            start_date='2024-03-01',
            end_date='2024-03-02',
            elements=['tas_mean', 'wind_speed', 'relative_humidity'],
        )
        raw_query = ObservationQuery(
            country='ES',
            provider='aemet',
            resolution='daily',
            station_ids=['3195'],
            start_date='2024-03-01',
            end_date='2024-03-02',
            elements=['tmed', 'velmedia', 'hrMedia'],
        )
        self.assertEqual(daily_query.elements, ['tmed', 'velmedia', 'hrMedia'])
        self.assertEqual(raw_query.elements, ['tmed', 'velmedia', 'hrMedia'])

    def test_read_station_metadata_country_es_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['0076', '3195'])
        self.assertTrue(stations['gh_id'].isna().all())
        madrid = stations[stations['station_id'] == '3195'].iloc[0]
        self.assertEqual(madrid['full_name'], 'MADRID, RETIRO, MADRID')
        self.assertAlmostEqual(madrid['latitude'], 40.414444, places=5)
        self.assertAlmostEqual(madrid['longitude'], -3.683611, places=5)
        self.assertEqual(madrid['elevation_m'], 667.0)

    def test_read_station_observation_metadata_country_es_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertIn('tmed', metadata['element'].tolist())
        self.assertIn('velmedia', metadata['element'].tolist())
        self.assertIn('hrMedia', metadata['element'].tolist())
        self.assertIn('Daily mean air temperature', metadata['name'].tolist())
        self.assertIn('Daily mean wind speed', metadata['name'].tolist())
        self.assertIn('Daily mean relative humidity', metadata['name'].tolist())

    def test_aemet_parser_helpers_cover_coordinates_and_numeric_rules(self) -> None:
        self.assertAlmostEqual(parse_aemet_coordinate('402452N'), 40.414444, places=5)
        self.assertAlmostEqual(parse_aemet_coordinate('034101W'), -3.683611, places=5)
        self.assertEqual(parse_aemet_numeric('prec', 'Ip'), 0.0)
        self.assertAlmostEqual(parse_aemet_numeric('tmed', '7,7'), 7.7, places=6)
        self.assertAlmostEqual(parse_aemet_numeric('velmedia', '7,2'), 2.0, places=6)
        self.assertAlmostEqual(parse_aemet_numeric('hrMedia', '63,5'), 63.5, places=6)
        self.assertTrue(pd.isna(parse_aemet_numeric('hrMedia', '')))

    def test_es_daily_download_normalizes_output_from_fixtures(self) -> None:
        station_metadata = read_station_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='ES',
            provider='aemet',
            resolution='daily',
            station_ids=['3195'],
            start_date='2024-03-01',
            end_date='2024-03-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'sunshine_duration'],
        )
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_AEMET_API_KEY': 'test-key'}, clear=False):
            with patch('weatherdownload.providers.es.daily.download_aemet_dataset_text', return_value=SAMPLE_DAILY_TEXT):
                observations = download_observations(query, country='ES', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'relative_humidity', 'sunshine_duration', 'tas_max', 'tas_mean', 'tas_min', 'wind_speed'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['hrMedia', 'prec', 'sol', 'tmax', 'tmed', 'tmin', 'velmedia'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(observations['provider'].unique().tolist(), ['aemet'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])

        values = {
            (row.observation_date.isoformat(), row.element): row.value
            for row in observations.itertuples(index=False)
        }
        self.assertAlmostEqual(values[('2024-03-01', 'tas_mean')], 10.5, places=6)
        self.assertAlmostEqual(values[('2024-03-01', 'tas_max')], 17.0, places=6)
        self.assertAlmostEqual(values[('2024-03-01', 'tas_min')], 4.0, places=6)
        self.assertAlmostEqual(values[('2024-03-01', 'precipitation')], 0.4, places=6)
        self.assertAlmostEqual(values[('2024-03-01', 'wind_speed')], 2.0, places=6)
        self.assertAlmostEqual(values[('2024-03-01', 'relative_humidity')], 63.5, places=6)
        self.assertAlmostEqual(values[('2024-03-01', 'sunshine_duration')], 8.5, places=6)
        self.assertEqual(values[('2024-03-02', 'precipitation')], 0.0)
        self.assertAlmostEqual(values[('2024-03-02', 'wind_speed')], 1.0, places=6)
        self.assertTrue(pd.isna(values[('2024-03-02', 'relative_humidity')]))
        self.assertTrue(json.dumps(parse_aemet_daily_data_json(SAMPLE_DAILY_TEXT).to_dict(orient='records')))

    def test_es_daily_missing_optional_raw_fields_do_not_crash(self) -> None:
        station_metadata = read_station_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='ES',
            provider='aemet',
            resolution='daily',
            station_ids=['3195'],
            start_date='2024-03-02',
            end_date='2024-03-02',
            elements=['sunshine_duration'],
        )
        payload = '[{"fecha":"2024-03-02","indicativo":"3195"}]'
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_AEMET_API_KEY': 'test-key'}, clear=False):
            with patch('weatherdownload.providers.es.daily.download_aemet_dataset_text', return_value=payload):
                observations = download_observations(query, country='ES', station_metadata=station_metadata)
        self.assertEqual(len(observations.index), 1)
        self.assertTrue(observations['value'].isna().all())

    def test_es_daily_missing_hrmedia_column_does_not_crash(self) -> None:
        station_metadata = read_station_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='ES',
            provider='aemet',
            resolution='daily',
            station_ids=['3195'],
            start_date='2024-03-02',
            end_date='2024-03-02',
            elements=['relative_humidity'],
        )
        payload = '[{"fecha":"2024-03-02","indicativo":"3195"}]'
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_AEMET_API_KEY': 'test-key'}, clear=False):
            with patch('weatherdownload.providers.es.daily.download_aemet_dataset_text', return_value=payload):
                observations = download_observations(query, country='ES', station_metadata=station_metadata)
        self.assertEqual(len(observations.index), 1)
        self.assertEqual(observations.iloc[0]['element'], 'relative_humidity')
        self.assertTrue(observations['value'].isna().all())

    def test_es_provider_fails_early_when_api_key_is_missing(self) -> None:
        station_metadata = read_station_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='ES',
            provider='aemet',
            resolution='daily',
            station_ids=['3195'],
            start_date='2024-03-01',
            end_date='2024-03-02',
            elements=['tas_mean'],
        )
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, r'AEMET OpenData API key is required'):
                download_observations(query, country='ES', station_metadata=station_metadata)

    def test_fixture_metadata_parsing_does_not_require_api_key(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            stations = read_station_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['0076', '3195'])

    def test_es_all_history_is_not_implemented(self) -> None:
        station_metadata = read_station_metadata(country='ES', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='ES',
            provider='aemet',
            resolution='daily',
            station_ids=['3195'],
            all_history=True,
            elements=['tas_mean'],
        )
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_AEMET_API_KEY': 'test-key'}, clear=False):
            with self.assertRaisesRegex(NotImplementedError, r'all_history'):
                download_observations(query, country='ES', station_metadata=station_metadata)


if __name__ == '__main__':
    unittest.main()
