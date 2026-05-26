import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    get_provider,
    list_providers,
    list_resolutions,
    list_station_elements,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.cli import main
from weatherdownload.providers.gb.observations import build_station_dly_url
from weatherdownload.providers.gb.parser import (
    GHCND_NORMALIZED_DAILY_COLUMNS,
    build_station_supported_raw_elements,
    normalize_daily_observations_ghcnd,
    parse_ghcnd_dly_text,
    parse_ghcnd_inventory_text,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_INVENTORY_PATH = Path('tests/data/sample_ghcnd_inventory.txt')
SAMPLE_DLY_PATH = Path('tests/data/sample_ghcnd_GB000000001.dly')


class GreatBritainGhcndProviderTests(unittest.TestCase):
    def test_supported_countries_include_gb(self) -> None:
        self.assertIn('GB', list_supported_countries())

    def test_provider_capability_metadata_is_explicit(self) -> None:
        provider = get_provider('GB')
        self.assertEqual(provider.supported_country_codes, ('GB',))
        self.assertEqual(provider.supported_providers, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(
            provider.supported_canonical_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
        )

    def test_discovery_country_gb_returns_ghcnd_daily_elements_without_evap(self) -> None:
        self.assertEqual(list_providers(country='GB'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='GB', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='GB', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='GB', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )

    def test_query_normalizes_canonical_and_raw_gb_ghcnd_elements(self) -> None:
        canonical_query = ObservationQuery(
            country='GB',
            provider='ghcnd',
            resolution='daily',
            station_ids=['gb000000001'],
            start_date='2020-02-01',
            end_date='2020-02-02',
            elements=['tas_max', 'precipitation'],
        )
        raw_query = ObservationQuery(
            country='GB',
            provider='ghcnd',
            resolution='daily',
            station_ids=['GB000000001'],
            start_date='2020-02-01',
            end_date='2020-02-02',
            elements=['TMAX', 'PRCP'],
        )
        self.assertEqual(canonical_query.station_ids, ['GB000000001'])
        self.assertEqual(canonical_query.elements, ['TMAX', 'PRCP'])
        self.assertEqual(raw_query.elements, ['TMAX', 'PRCP'])

    def test_shared_inventory_filter_can_build_country_specific_station_elements(self) -> None:
        inventory_table = parse_ghcnd_inventory_text(SAMPLE_INVENTORY_PATH.read_text(encoding='utf-8'))
        station_elements = build_station_supported_raw_elements(
            inventory_table,
            country_prefix='GB',
            supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
        )
        self.assertEqual(
            station_elements,
            {
                'GB000000001': ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
                'GB000000002': ['PRCP'],
            },
        )

    def test_read_station_metadata_filters_to_gb_supported_core_inventory(self) -> None:
        stations = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['GB000000001', 'GB000000002'])
        self.assertEqual(stations.iloc[0]['begin_date'], '2015-01-01T00:00Z')
        self.assertEqual(stations.iloc[0]['end_date'], '2017-12-31T00:00Z')
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_builds_rows_for_gb_supported_elements(self) -> None:
        observation_metadata = read_station_observation_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(observation_metadata['station_id'].unique().tolist(), ['GB000000001', 'GB000000002'])
        self.assertEqual(
            observation_metadata[['station_id', 'element']].to_dict('records'),
            [
                {'station_id': 'GB000000001', 'element': 'PRCP'},
                {'station_id': 'GB000000001', 'element': 'SNWD'},
                {'station_id': 'GB000000001', 'element': 'TAVG'},
                {'station_id': 'GB000000001', 'element': 'TMAX'},
                {'station_id': 'GB000000001', 'element': 'TMIN'},
                {'station_id': 'GB000000002', 'element': 'PRCP'},
            ],
        )

    def test_parse_and_normalize_gb_dly_converts_units_and_drops_missing(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='GB',
            provider='ghcnd',
            resolution='daily',
            station_ids=['GB000000001'],
            start_date='2020-02-01',
            end_date='2020-02-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(list(normalized.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        lookup = {(row.element, str(row.observation_date)): row for row in normalized.itertuples(index=False)}
        self.assertAlmostEqual(float(lookup[('tas_mean', '2020-02-01')].value), 7.2)
        self.assertAlmostEqual(float(lookup[('tas_mean', '2020-02-02')].value), 8.1)
        self.assertAlmostEqual(float(lookup[('tas_max', '2020-02-01')].value), 9.5)
        self.assertAlmostEqual(float(lookup[('tas_max', '2020-02-02')].value), 10.4)
        self.assertAlmostEqual(float(lookup[('tas_min', '2020-02-01')].value), 4.8)
        self.assertNotIn(('tas_min', '2020-02-02'), lookup)
        self.assertAlmostEqual(float(lookup[('precipitation', '2020-02-01')].value), 1.8)
        self.assertAlmostEqual(float(lookup[('precipitation', '2020-02-02')].value), 0.0)
        self.assertAlmostEqual(float(lookup[('snow_depth', '2020-02-01')].value), 0.0)
        self.assertAlmostEqual(float(lookup[('snow_depth', '2020-02-02')].value), 0.0)

    def test_build_station_dly_url_uses_official_all_directory(self) -> None:
        self.assertEqual(
            build_station_dly_url('GB000000001'),
            'https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/GB000000001.dly',
        )

    def test_download_observations_reads_local_gb_dly_fixture_and_canonicalizes_output(self) -> None:
        query = ObservationQuery(
            country='GB',
            provider='ghcnd',
            resolution='daily',
            station_ids=['GB000000001'],
            start_date='2020-02-01',
            end_date='2020-02-02',
            elements=['tas_max', 'precipitation'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='GB')
        self.assertEqual(list(observations.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['PRCP', 'TMAX'])

    def test_list_station_elements_for_gb_fixture_station_is_inventory_driven(self) -> None:
        stations = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list_station_elements(stations, 'GB000000001', 'ghcnd', 'daily', country='GB'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_station_elements(stations, 'GB000000002', 'ghcnd', 'daily', country='GB'),
            ['precipitation'],
        )

    def test_station_metadata_excludes_gb_station_with_only_unsupported_elements(self) -> None:
        stations = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertNotIn('GB000000003', stations['station_id'].tolist())

    def test_cli_station_elements_for_gb_uses_inventory_specific_availability(self) -> None:
        stations = read_station_metadata(country='GB', source_url=str(SAMPLE_STATIONS_PATH))
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=stations):
            with redirect_stdout(buffer):
                exit_code = main(['stations', 'elements', '--country', 'GB', '--provider', 'ghcnd', '--station-id', 'GB000000002', '--resolution', 'daily'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('precipitation', output)
        self.assertNotIn('tas_max', output)
        self.assertNotIn('open_water_evaporation', output)


if __name__ == '__main__':
    unittest.main()
