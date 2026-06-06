import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
)
from weatherdownload.providers.be.daily import RMI_AWS_WFS_URL

SAMPLE_STATIONS_PATH = Path('tests/data/sample_be_aws_station.json')
SAMPLE_DAILY_TEXT = Path('tests/data/sample_be_aws_1day.json').read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class BelgiumRmiProviderTests(unittest.TestCase):
    def test_rmi_daily_discovery_is_exposed(self) -> None:
        self.assertEqual(list_resolutions(country='BE', provider='rmi'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='BE', provider='rmi', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )

    def test_rmi_daily_query_maps_canonical_elements(self) -> None:
        query = ObservationQuery(
            country='BE',
            provider='rmi',
            resolution='daily',
            station_ids=['6414'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'wind_speed_max', 'sunshine_duration'],
        )
        self.assertEqual(query.elements, ['temp_avg', 'wind_gusts_speed', 'sun_duration'])

    def test_rmi_daily_download_normalizes_sunshine_to_hours(self) -> None:
        station_metadata = read_station_metadata(country='BE', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url, params=None, timeout=60):
            if url == RMI_AWS_WFS_URL:
                self.assertEqual(params['typeNames'], 'aws:aws_1day')
                return _MockResponse(SAMPLE_DAILY_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(
            country='BE',
            provider='rmi',
            resolution='daily',
            station_ids=['6414'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['sunshine_duration', 'wind_speed_max'],
        )
        with patch('weatherdownload.providers.be.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='BE', station_metadata=station_metadata)

        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('sunshine_duration', pd.Timestamp('2024-01-01').date())]), 380.92 / 60.0)
        self.assertAlmostEqual(float(lookup[('wind_speed_max', pd.Timestamp('2024-01-01').date())]), 8.2)


if __name__ == '__main__':
    unittest.main()
