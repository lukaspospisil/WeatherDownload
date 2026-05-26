import unittest
from pathlib import Path

from weatherdownload import (
    list_providers,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')


class PortugalGhcndProviderTests(unittest.TestCase):
    def test_supported_countries_include_pt(self) -> None:
        self.assertIn('PT', list_supported_countries())

    def test_discovery_country_pt_returns_ghcnd_daily_without_evap(self) -> None:
        self.assertIn('ghcnd', list_providers(country='PT'))
        self.assertIn('daily', list_resolutions(country='PT', provider='ghcnd'))
        self.assertEqual(
            list_supported_elements(country='PT', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertNotIn(
            'open_water_evaporation',
            list_supported_elements(country='PT', provider='ghcnd', resolution='daily'),
        )

    def test_station_metadata_reader_filters_ghcnd_stations_by_po_prefix(self) -> None:
        stations = read_station_metadata(country='PT', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['PO000000001', 'PO000000002'])


if __name__ == '__main__':
    unittest.main()
