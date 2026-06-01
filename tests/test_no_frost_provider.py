import os
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
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
from weatherdownload.providers.no.daily import download_daily_observations_frost, normalize_daily_observations_frost
from weatherdownload.providers.no.metadata import resolve_frost_client_id
from weatherdownload.providers.no.parser import parse_frost_observations_json


SAMPLE_STATIONS_PATH = Path('tests/data/sample_no_frost_stations.json')
SAMPLE_DAILY_PATH = Path('tests/data/sample_no_frost_daily.json')
SAMPLE_DAILY_TEXT = SAMPLE_DAILY_PATH.read_text(encoding='utf-8')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class NoFrostProviderTests(unittest.TestCase):
    def test_supported_countries_include_no(self) -> None:
        self.assertIn('NO', list_supported_countries())
        self.assertEqual(list_providers(country='NO'), ['frost', 'ghcnd'])
        self.assertEqual(list_resolutions(country='NO', provider='frost'), ['daily'])
        self.assertEqual(list_resolutions(country='NO', provider='ghcnd'), ['daily'])

    def test_discovery_country_no_returns_frost_and_ghcnd_daily_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='NO', provider='frost', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='NO', provider='frost', resolution='daily', provider_raw=True),
            [
                'mean(air_temperature P1D)',
                'max(air_temperature P1D)',
                'min(air_temperature P1D)',
                'sum(precipitation_amount P1D)',
                'mean(wind_speed P1D)',
                'surface_snow_thickness',
            ],
        )
        self.assertNotIn('relative_humidity', list_supported_elements(country='NO', provider='frost', resolution='daily'))
        self.assertNotIn('pressure', list_supported_elements(country='NO', provider='frost', resolution='daily'))
        self.assertNotIn('sunshine_duration', list_supported_elements(country='NO', provider='frost', resolution='daily'))
        self.assertNotIn('solar_radiation', list_supported_elements(country='NO', provider='frost', resolution='daily'))

    def test_no_frost_queries_accept_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['sn18700'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation', 'snow_depth'],
        )
        raw_query = ObservationQuery(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['SN18700'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['mean(air_temperature P1D)', 'sum(precipitation_amount P1D)', 'surface_snow_thickness'],
        )
        self.assertEqual(canonical_query.station_ids, ['SN18700'])
        self.assertEqual(canonical_query.elements, ['mean(air_temperature P1D)', 'sum(precipitation_amount P1D)', 'surface_snow_thickness'])
        self.assertEqual(raw_query.elements, ['mean(air_temperature P1D)', 'sum(precipitation_amount P1D)', 'surface_snow_thickness'])

    def test_read_station_metadata_country_no_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='NO', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['SN18700', 'SN90450'])
        self.assertTrue(stations['gh_id'].isna().all())
        blindern = stations[stations['station_id'] == 'SN18700'].iloc[0]
        self.assertEqual(blindern['full_name'], 'Oslo - Blindern')
        self.assertAlmostEqual(blindern['longitude'], 10.72, places=6)
        self.assertAlmostEqual(blindern['latitude'], 59.94, places=6)
        self.assertEqual(blindern['elevation_m'], 94.0)

    def test_read_station_observation_metadata_country_no_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='NO', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertIn('mean(air_temperature P1D)', metadata['element'].tolist())
        self.assertIn('sum(precipitation_amount P1D)', metadata['element'].tolist())
        self.assertIn('surface_snow_thickness', metadata['element'].tolist())
        self.assertIn('Daily mean air temperature', metadata['name'].tolist())
        self.assertIn('Surface snow thickness', metadata['name'].tolist())

    def test_parse_frost_observations_uses_reference_date_not_timeoffset_end(self) -> None:
        parsed = parse_frost_observations_json(SAMPLE_DAILY_TEXT)
        self.assertEqual(sorted(parsed['station_id'].unique().tolist()), ['SN18700', 'SN90450'])
        self.assertEqual(parsed.iloc[0]['observation_date'].isoformat(), '2024-01-01')
        offsets = {
            row.element_raw: row.time_offset
            for row in parsed[parsed['station_id'] == 'SN18700'].itertuples(index=False)
            if row.observation_date.isoformat() == '2024-01-01'
        }
        self.assertEqual(offsets['max(air_temperature P1D)'], 'PT18H')
        self.assertEqual(offsets['sum(precipitation_amount P1D)'], 'PT6H')

    def test_no_frost_daily_download_normalizes_units_and_special_codes_from_fixture(self) -> None:
        station_metadata = read_station_metadata(country='NO', source_url=str(SAMPLE_STATIONS_PATH))
        station_metadata.attrs['observations_source_url'] = str(SAMPLE_DAILY_PATH)
        query = ObservationQuery(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['SN18700'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'snow_depth'],
        )
        observations = download_observations(query, country='NO', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'snow_depth', 'tas_max', 'tas_mean', 'tas_min', 'wind_speed'])
        self.assertEqual(observations['provider'].unique().tolist(), ['frost'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())
        self.assertEqual(str(observations['quality'].dtype), 'Int64')

        values = {
            (row.observation_date.isoformat(), row.element): row.value
            for row in observations.itertuples(index=False)
        }
        self.assertAlmostEqual(values[('2024-01-01', 'tas_mean')], 3.2, places=6)
        self.assertAlmostEqual(values[('2024-01-01', 'tas_max')], 5.1, places=6)
        self.assertAlmostEqual(values[('2024-01-01', 'tas_min')], -0.8, places=6)
        self.assertEqual(values[('2024-01-01', 'precipitation')], 0.0)
        self.assertAlmostEqual(values[('2024-01-01', 'wind_speed')], 4.5, places=6)
        self.assertAlmostEqual(values[('2024-01-01', 'snow_depth')], 120.0, places=6)
        self.assertTrue(pd.isna(values[('2024-01-02', 'snow_depth')]))

        snow_row = observations[(observations['observation_date'] == pd.Timestamp('2024-01-02').date()) & (observations['element'] == 'snow_depth')].iloc[0]
        self.assertEqual(int(snow_row['quality']), 5)
        self.assertIn('coded_value_description', snow_row['flag'])
        self.assertIn('Zero snow depth or partial snow cover', snow_row['flag'])

    def test_no_frost_daily_precipitation_code_minus_one_maps_to_zero_mm_with_coded_flag(self) -> None:
        station_metadata = read_station_metadata(country='NO', source_url=str(SAMPLE_STATIONS_PATH))
        station_metadata.attrs['observations_source_url'] = str(SAMPLE_DAILY_PATH)
        query = ObservationQuery(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['SN18700'],
            start_date='2024-01-01',
            end_date='2024-01-01',
            elements=['precipitation'],
        )

        observations = download_observations(query, country='NO', station_metadata=station_metadata)

        precipitation_row = observations.iloc[0]
        self.assertEqual(precipitation_row['element_raw'], 'sum(precipitation_amount P1D)')
        self.assertEqual(precipitation_row['value'], 0.0)
        self.assertIn('coded_value', precipitation_row['flag'])
        self.assertIn('-1.0', precipitation_row['flag'])
        self.assertIn('coded_value_description', precipitation_row['flag'])
        self.assertIn('No precipitation', precipitation_row['flag'])

    def test_no_frost_daily_normalizer_rejects_unexpected_units(self) -> None:
        payload = parse_frost_observations_json(SAMPLE_DAILY_TEXT)
        payload.loc[payload['element_raw'] == 'mean(wind_speed P1D)', 'unit'] = 'km/h'
        query = ObservationQuery(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['SN18700'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['wind_speed'],
        )
        with self.assertRaisesRegex(ValueError, r"Unexpected Frost unit"):
            normalize_daily_observations_frost(payload, query=query)

    def test_no_frost_daily_normalizer_rejects_unsupported_elements(self) -> None:
        payload = parse_frost_observations_json(SAMPLE_DAILY_TEXT)
        payload.loc[payload.index[0], 'element_raw'] = 'mean(relative_humidity P1D)'
        query = SimpleNamespace(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['SN18700'],
            start_date=date.fromisoformat('2024-01-01'),
            end_date=date.fromisoformat('2024-01-01'),
            elements=['mean(relative_humidity P1D)'],
        )
        with self.assertRaisesRegex(ValueError, r'Unsupported Frost daily element'):
            normalize_daily_observations_frost(payload, query=query)

    def test_no_frost_provider_fails_early_when_client_id_is_missing_for_live_downloads(self) -> None:
        station_metadata = read_station_metadata(country='NO', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['SN18700'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean'],
        )
        with patch.dict(os.environ, {}, clear=True):
            with patch('weatherdownload.providers.no.daily.requests.get') as mock_get:
                with self.assertRaisesRegex(ValueError, r'Frost client ID is required'):
                    download_observations(query, country='NO', station_metadata=station_metadata)
        mock_get.assert_not_called()

    def test_resolve_frost_client_id_accepts_project_env_var(self) -> None:
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_FROST_CLIENT_ID': 'test-client'}, clear=True):
            self.assertEqual(resolve_frost_client_id(), 'test-client')

    def test_no_frost_live_download_builds_expected_request_shape(self) -> None:
        station_metadata = read_station_metadata(country='NO', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='NO',
            provider='frost',
            resolution='daily',
            station_ids=['SN18700'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation'],
        )
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_FROST_CLIENT_ID': 'test-client'}, clear=False):
            with patch('weatherdownload.providers.no.daily.requests.get', return_value=_MockTextResponse(SAMPLE_DAILY_TEXT)) as mock_get:
                observations = download_daily_observations_frost(query, station_metadata=station_metadata)

        self.assertFalse(observations.empty)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['auth'], ('test-client', ''))
        self.assertEqual(kwargs['params']['sources'], 'SN18700')
        self.assertEqual(kwargs['params']['referencetime'], '2024-01-01/2024-01-02')
        self.assertEqual(kwargs['params']['elements'], 'mean(air_temperature P1D),sum(precipitation_amount P1D)')
        self.assertEqual(kwargs['params']['timeresolutions'], 'P1D')
        self.assertEqual(kwargs['params']['levels'], 'default')
        self.assertEqual(kwargs['params']['timeoffsets'], 'default')


if __name__ == '__main__':
    unittest.main()
