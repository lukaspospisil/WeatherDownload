import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import requests

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)


FIXTURE_DIR = Path('tests/data/is_vedur')
SYNOP_DAILY_FIXTURE_TEXT = (FIXTURE_DIR / 'synop_day_1_2024-01-01_2024-01-03.json').read_text(encoding='utf-8')
AWS_DAILY_FIXTURE_TEXT = (FIXTURE_DIR / 'aws_day_1475_2024-01-01_2024-01-03.json').read_text(encoding='utf-8')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            http_error = requests.HTTPError(f'HTTP {self.status_code}')
            http_error.response = self
            raise http_error


def _vedur_daily_response(url: str, params=None, timeout: int = 60) -> _MockTextResponse:
    del timeout
    params = params or {}
    station_id = str(params.get('station_id'))
    day_from = params.get('day_from')
    day_to = params.get('day_to')
    if url.endswith('/observations/synop/day') and station_id == '1' and day_from == '2024-01-01' and day_to == '2024-01-03':
        return _MockTextResponse(SYNOP_DAILY_FIXTURE_TEXT)
    if url.endswith('/observations/aws/day') and station_id == '1475' and day_from == '2024-01-01' and day_to == '2024-01-03':
        return _MockTextResponse(AWS_DAILY_FIXTURE_TEXT)
    return _MockTextResponse('{"message":"No data found."}', status_code=404)


class IcelandVedurProviderTests(unittest.TestCase):
    def test_is_discovery_exposes_vedur_and_ghcnd_daily(self) -> None:
        self.assertEqual(list_providers(country='IS'), ['ghcnd', 'vedur'])
        self.assertEqual(list_resolutions(country='IS', provider='vedur'), ['daily'])
        self.assertEqual(list_resolutions(country='IS', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='IS', provider='vedur', resolution='daily'),
            [
                'tas_mean',
                'tas_max',
                'tas_min',
                'precipitation',
                'wind_speed',
                'wind_speed_max',
                'relative_humidity',
                'pressure',
                'vapour_pressure',
                'snow_depth',
                'sunshine_duration',
            ],
        )
        self.assertEqual(
            list_supported_elements(country='IS', provider='vedur', resolution='daily', provider_raw=True),
            ['t', 'txx', 'tx', 'tnn', 'tn', 'r', 'f', 'fg', 'rh', 'p', 'vp', 'snd', 'sun', 'rsun'],
        )

    def test_read_station_metadata_country_is_from_fixture(self) -> None:
        stations = read_station_metadata(country='IS', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['1', '1475'])
        self.assertEqual(stations['full_name'].tolist(), ['Reykjavik', 'Reykjavik - Bustadavegur'])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('vedur', 'daily')]['1'],
            ['t', 'txx', 'tnn', 'r', 'f', 'fg', 'rh', 'p', 'vp', 'snd', 'sun'],
        )
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('vedur', 'daily')]['1475'],
            ['t', 'tx', 'tn', 'r', 'f', 'fg', 'rh', 'p', 'vp', 'rsun'],
        )

    def test_read_station_observation_metadata_country_is_from_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='IS', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertIn('snd', metadata[metadata['station_id'] == '1']['element'].tolist())
        self.assertNotIn('snd', metadata[metadata['station_id'] == '1475']['element'].tolist())
        self.assertIn('rsun', metadata[metadata['station_id'] == '1475']['element'].tolist())

    def test_download_daily_observations_is_vedur_for_synop_station(self) -> None:
        station_metadata = read_station_metadata(country='IS', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='IS',
            provider='vedur',
            resolution='daily',
            station_ids=['1'],
            start_date='2024-01-01',
            end_date='2024-01-03',
            elements=[
                'tas_mean',
                'tas_max',
                'tas_min',
                'precipitation',
                'wind_speed',
                'wind_speed_max',
                'relative_humidity',
                'pressure',
                'vapour_pressure',
                'snow_depth',
                'sunshine_duration',
            ],
        )
        with patch('weatherdownload.providers.is.daily.requests.get', side_effect=_vedur_daily_response):
            observations = download_observations(query, country='IS', station_metadata=station_metadata)

        self.assertEqual(observations['provider'].unique().tolist(), ['vedur'])
        lookup = {(row.element, row.observation_date.isoformat()): row.value for row in observations.itertuples(index=False)}
        self.assertEqual(float(lookup[('tas_mean', '2024-01-01')]), 1.7)
        self.assertEqual(float(lookup[('tas_max', '2024-01-01')]), 4.5)
        self.assertEqual(float(lookup[('tas_min', '2024-01-01')]), -5.5)
        self.assertTrue(pd.isna(lookup[('precipitation', '2024-01-01')]))
        self.assertEqual(float(lookup[('snow_depth', '2024-01-02')]), 14.0)
        self.assertEqual(float(lookup[('sunshine_duration', '2024-01-03')]), 0.0)

    def test_download_daily_observations_is_vedur_for_aws_station_uses_alternative_raw_codes(self) -> None:
        station_metadata = read_station_metadata(country='IS', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='IS',
            provider='vedur',
            resolution='daily',
            station_ids=['1475'],
            start_date='2024-01-01',
            end_date='2024-01-03',
            elements=[
                'tas_mean',
                'tas_max',
                'tas_min',
                'precipitation',
                'wind_speed',
                'wind_speed_max',
                'relative_humidity',
                'pressure',
                'vapour_pressure',
                'sunshine_duration',
            ],
        )
        with patch('weatherdownload.providers.is.daily.requests.get', side_effect=_vedur_daily_response):
            observations = download_observations(query, country='IS', station_metadata=station_metadata)

        self.assertEqual(observations['provider'].unique().tolist(), ['vedur'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['f', 'fg', 'p', 'r', 'rh', 'rsun', 't', 'tn', 'tx', 'vp'])
        lookup = {(row.element, row.observation_date.isoformat()): row.value for row in observations.itertuples(index=False)}
        self.assertEqual(float(lookup[('tas_max', '2024-01-02')]), 4.7)
        self.assertEqual(float(lookup[('tas_min', '2024-01-03')]), 0.6)
        self.assertEqual(float(lookup[('precipitation', '2024-01-03')]), 1.2)
        self.assertEqual(float(lookup[('sunshine_duration', '2024-01-02')]), 1.5)

    def test_download_daily_observations_is_vedur_rejects_station_specific_unsupported_element(self) -> None:
        station_metadata = read_station_metadata(country='IS', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='IS',
            provider='vedur',
            resolution='daily',
            station_ids=['1475'],
            start_date='2024-01-01',
            end_date='2024-01-01',
            elements=['snow_depth'],
        )
        with self.assertRaisesRegex(Exception, 'does not support requested raw elements'):
            with patch('weatherdownload.providers.is.daily.requests.get', side_effect=_vedur_daily_response):
                download_observations(query, country='IS', station_metadata=station_metadata)


if __name__ == '__main__':
    unittest.main()
