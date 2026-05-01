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
    list_supported_elements,
    read_station_metadata,
)
from weatherdownload.providers.fi.fmi_parser import FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS


SAMPLE_FMI_XML = Path('tests/data/sample_fmi_timevaluepair_real_fmisid.xml')
SAMPLE_FMI_DAILY_XML = Path('tests/data/sample_fmi_daily_timevaluepair_real.xml')
SAMPLE_FMI_STATIONS_XML = Path('tests/data/sample_fmi_stations.xml')


class FinlandFmiProviderTests(unittest.TestCase):
    def test_provider_capability_metadata_includes_fmi_and_ghcnd(self) -> None:
        provider = get_provider('FI')
        self.assertEqual(provider.supported_country_codes, ('FI',))
        self.assertEqual(provider.supported_providers, ('fmi', 'ghcnd'))
        self.assertEqual(provider.supported_resolutions, ('daily', '1hour'))

    def test_discovery_country_fi_includes_fmi_hourly_and_daily(self) -> None:
        self.assertEqual(list_providers(country='FI'), ['fmi', 'ghcnd'])
        self.assertEqual(list_resolutions(country='FI', provider='fmi'), ['1hour', 'daily'])
        self.assertEqual(
            list_supported_elements(country='FI', provider='fmi', resolution='1hour'),
            ['tas_mean', 'wind_speed', 'relative_humidity', 'pressure', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='FI', provider='fmi', resolution='1hour', provider_raw=True),
            ['t2m', 'ws_10min', 'rh', 'p_sea', 'r_1h'],
        )
        self.assertEqual(
            list_supported_elements(country='FI', provider='fmi', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='FI', provider='fmi', resolution='daily', provider_raw=True),
            ['tday', 'tmax', 'tmin', 'rrday'],
        )

    def test_download_observations_reads_local_fmi_fixture_via_station_metadata_source(self) -> None:
        stations = read_station_metadata(country='FI', source_url=str(SAMPLE_FMI_XML))
        query = ObservationQuery(
            country='FI',
            provider='fmi',
            resolution='1hour',
            station_ids=['100971'],
            start='2026-04-30T00:00:00Z',
            end='2026-04-30T02:00:00Z',
            elements=['tas_mean', 'wind_speed', 'relative_humidity', 'pressure', 'precipitation'],
        )
        # Ensure we never hit the network in this test.
        with patch('weatherdownload.providers.fi.hourly_fmi.requests.get', side_effect=AssertionError('unexpected network')):
            observations = download_observations(query, country='FI', station_metadata=stations)

        self.assertEqual(list(observations.columns), FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)
        self.assertEqual(observations['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), sorted(['t2m', 'ws_10min', 'rh', 'p_sea', 'r_1h']))
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            sorted(['tas_mean', 'wind_speed', 'relative_humidity', 'pressure', 'precipitation']),
        )
        self.assertTrue(isinstance(observations['timestamp'].dtype, pd.DatetimeTZDtype))

    def test_download_daily_observations_reads_local_fmi_daily_fixture(self) -> None:
        stations = read_station_metadata(country='FI', source_url=str(SAMPLE_FMI_DAILY_XML))
        query = ObservationQuery(
            country='FI',
            provider='fmi',
            resolution='daily',
            station_ids=['100971'],
            start_date='2026-04-01',
            end_date='2026-04-03',
            elements=['tas_mean', 'tas_min', 'tas_max', 'precipitation'],
        )
        with patch('weatherdownload.providers.fi.daily_fmi.requests.get', side_effect=AssertionError('unexpected network')):
            observations = download_observations(query, country='FI', station_metadata=stations)

        self.assertEqual(list(observations.columns), FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)
        self.assertEqual(observations['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), sorted(['tday', 'tmin', 'tmax', 'rrday']))
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            sorted(['tas_mean', 'tas_min', 'tas_max', 'precipitation']),
        )

    def test_read_station_metadata_fmi_from_station_fixture(self) -> None:
        stations = read_station_metadata(country='FI', source_url=str(SAMPLE_FMI_STATIONS_XML))
        self.assertFalse(stations.empty)
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertIn('station_provider_raw_elements_by_path', stations.attrs)
        self.assertIn(('fmi', '1hour'), stations.attrs['station_provider_raw_elements_by_path'])
        self.assertIn(('fmi', 'daily'), stations.attrs['station_provider_raw_elements_by_path'])


if __name__ == '__main__':
    unittest.main()
