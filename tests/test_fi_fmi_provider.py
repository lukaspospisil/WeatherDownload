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


class FinlandFmiProviderTests(unittest.TestCase):
    def test_provider_capability_metadata_includes_fmi_and_ghcnd(self) -> None:
        provider = get_provider('FI')
        self.assertEqual(provider.supported_country_codes, ('FI',))
        self.assertEqual(provider.supported_providers, ('fmi', 'ghcnd'))
        self.assertEqual(provider.supported_resolutions, ('daily', '1hour'))

    def test_discovery_country_fi_includes_fmi_hourly(self) -> None:
        self.assertEqual(list_providers(country='FI'), ['fmi', 'ghcnd'])
        self.assertEqual(list_resolutions(country='FI', provider='fmi'), ['1hour'])
        self.assertEqual(
            list_supported_elements(country='FI', provider='fmi', resolution='1hour'),
            ['tas_mean', 'wind_speed'],
        )
        self.assertEqual(
            list_supported_elements(country='FI', provider='fmi', resolution='1hour', provider_raw=True),
            ['t2m', 'ws_10min'],
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
            elements=['tas_mean', 'wind_speed'],
        )
        # Ensure we never hit the network in this test.
        with patch('weatherdownload.providers.fi.hourly_fmi.requests.get', side_effect=AssertionError('unexpected network')):
            observations = download_observations(query, country='FI', station_metadata=stations)

        self.assertEqual(list(observations.columns), FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)
        self.assertEqual(observations['station_id'].unique().tolist(), ['100971'])
        self.assertEqual(observations['element_raw'].unique().tolist(), ['t2m', 'ws_10min'])
        self.assertEqual(observations['element'].unique().tolist(), ['tas_mean', 'wind_speed'])
        self.assertTrue(pd.api.types.is_datetime64tz_dtype(observations['timestamp']))


if __name__ == '__main__':
    unittest.main()

