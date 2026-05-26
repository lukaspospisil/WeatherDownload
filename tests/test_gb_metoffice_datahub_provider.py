import os
import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.core.queries import QueryValidationError
from weatherdownload.providers.gb.metadata import resolve_metoffice_datahub_api_key


SAMPLE_STATIONS_PATH = Path('tests/data/sample_gb_metoffice_datahub_stations.json')
SAMPLE_HOURLY_TEXT = Path('tests/data/sample_gb_metoffice_datahub_hourly.json').read_text(encoding='utf-8')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class GbMetOfficeDataHubProviderTests(unittest.TestCase):
    def test_gb_discovery_includes_metoffice_datahub_hourly(self) -> None:
        self.assertEqual(list_providers(country='GB'), ['ghcnd', 'metoffice_datahub'])
        self.assertEqual(list_resolutions(country='GB', provider='metoffice_datahub'), ['1hour'])
        self.assertEqual(
            list_supported_elements(country='GB', provider='metoffice_datahub', resolution='1hour'),
            ['tas_mean', 'relative_humidity', 'wind_speed', 'pressure'],
        )
        self.assertEqual(
            list_supported_elements(country='GB', provider='metoffice_datahub', resolution='1hour', provider_raw=True),
            ['temperature', 'humidity', 'wind_speed', 'mslp'],
        )

    def test_gb_hourly_queries_accept_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(
            country='GB',
            provider='metoffice_datahub',
            resolution='1hour',
            station_ids=['gcj8ds'],
            start='2026-05-25T08:00:00Z',
            end='2026-05-25T09:00:00Z',
            elements=['tas_mean', 'pressure'],
        )
        raw_query = ObservationQuery(
            country='GB',
            provider='metoffice_datahub',
            resolution='1hour',
            station_ids=['gcj8ds'],
            start='2026-05-25T08:00:00Z',
            end='2026-05-25T09:00:00Z',
            elements=['temperature', 'mslp'],
        )
        self.assertEqual(canonical_query.station_ids, ['GCJ8DS'])
        self.assertEqual(canonical_query.elements, ['temperature', 'mslp'])
        self.assertEqual(raw_query.elements, ['temperature', 'mslp'])

    def test_read_station_metadata_country_gb_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['GCJ8DS', 'GCPVJ0'])
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_gb_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['element'].unique().tolist()), ['humidity', 'mslp', 'temperature', 'wind_speed'])

    def test_download_hourly_observations_gb_normalizes_output_from_fixture(self) -> None:
        station_metadata = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='GB',
            provider='metoffice_datahub',
            resolution='1hour',
            station_ids=['gcj8ds'],
            start='2026-05-25T08:00:00Z',
            end='2026-05-25T09:00:00Z',
            elements=['tas_mean', 'relative_humidity', 'wind_speed', 'pressure'],
        )
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY': 'test-key'}, clear=False):
            with patch('weatherdownload.providers.gb.observations.requests.get', return_value=_MockTextResponse(SAMPLE_HOURLY_TEXT)):
                with patch('weatherdownload.providers.gb.observations._current_utc', return_value='2026-05-26T00:00:00Z'):
                    observations = download_observations(query, country='GB', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'relative_humidity', 'tas_mean', 'wind_speed'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['humidity', 'mslp', 'temperature', 'wind_speed'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertEqual(observations['provider'].unique().tolist(), ['metoffice_datahub'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['1hour'])

    def test_gb_metoffice_datahub_requires_api_key_for_live_downloads(self) -> None:
        station_metadata = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='GB',
            provider='metoffice_datahub',
            resolution='1hour',
            station_ids=['gcj8ds'],
            start='2026-05-25T08:00:00Z',
            end='2026-05-25T09:00:00Z',
            elements=['tas_mean'],
        )
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, r'Met Office Weather DataHub API key is required'):
                download_observations(query, country='GB', station_metadata=station_metadata)

    def test_resolve_metoffice_datahub_api_key_accepts_project_env_var(self) -> None:
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY': 'test-key'}, clear=True):
            self.assertEqual(resolve_metoffice_datahub_api_key(), 'test-key')

    def test_gb_metoffice_datahub_rejects_outside_last_48_hours(self) -> None:
        station_metadata = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='GB',
            provider='metoffice_datahub',
            resolution='1hour',
            station_ids=['gcj8ds'],
            start='2026-05-20T08:00:00Z',
            end='2026-05-20T09:00:00Z',
            elements=['tas_mean'],
        )
        with patch.dict(os.environ, {'WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY': 'test-key'}, clear=False):
            with patch('weatherdownload.providers.gb.observations._current_utc', return_value='2026-05-26T00:00:00Z'):
                with self.assertRaisesRegex(Exception, r'last 48 hours'):
                    download_observations(query, country='GB', station_metadata=station_metadata)

    def test_gb_metoffice_datahub_does_not_accept_daily_or_tenmin_queries(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(
                country='GB',
                provider='metoffice_datahub',
                resolution='daily',
                station_ids=['gcj8ds'],
                start_date='2026-05-25',
                end_date='2026-05-26',
                elements=['tas_mean'],
            )
        with self.assertRaises(QueryValidationError):
            ObservationQuery(
                country='GB',
                provider='metoffice_datahub',
                resolution='10min',
                station_ids=['gcj8ds'],
                start='2026-05-25T08:00:00Z',
                end='2026-05-25T08:10:00Z',
                elements=['tas_mean'],
            )


if __name__ == '__main__':
    unittest.main()
