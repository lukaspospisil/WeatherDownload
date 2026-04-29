import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    get_provider,
    list_dataset_scopes,
    list_providers,
    list_resolutions,
    list_station_elements,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.cli import main
from weatherdownload.providers.ghcnd.parser import build_station_supported_raw_elements
from weatherdownload.providers.us.parser import (
    GHCND_NORMALIZED_DAILY_COLUMNS,
    normalize_daily_observations_ghcnd,
    parse_ghcnd_dly_text,
    parse_ghcnd_inventory_text,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_INVENTORY_PATH = Path('tests/data/sample_ghcnd_inventory.txt')

COUNTRY_SPECS = {
    'DE': {
        'ghcn_prefix': 'GM',
        'national_provider': 'historical',
        'station_core': 'GM000000001',
        'station_prcp_only': 'GM000000002',
        'station_unsupported': 'GM000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_GM000000001.dly'),
        'start_date': '2020-12-01',
        'end_date': '2020-12-02',
    },
    'SK': {
        'ghcn_prefix': 'LO',
        'national_provider': 'recent',
        'station_core': 'LO000000001',
        'station_prcp_only': 'LO000000002',
        'station_unsupported': 'LO000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_LO000000001.dly'),
        'start_date': '2021-01-01',
        'end_date': '2021-01-02',
    },
    'CH': {
        'ghcn_prefix': 'SZ',
        'national_provider': 'historical',
        'station_core': 'SZ000000001',
        'station_prcp_only': 'SZ000000002',
        'station_unsupported': 'SZ000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_SZ000000001.dly'),
        'start_date': '2021-02-01',
        'end_date': '2021-02-02',
    },
    'DK': {
        'ghcn_prefix': 'DA',
        'national_provider': 'historical',
        'station_core': 'DA000000001',
        'station_prcp_only': 'DA000000002',
        'station_unsupported': 'DA000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_DA000000001.dly'),
        'start_date': '2021-03-01',
        'end_date': '2021-03-02',
    },
    'SE': {
        'ghcn_prefix': 'SW',
        'national_provider': 'historical',
        'station_core': 'SW000000001',
        'station_prcp_only': 'SW000000002',
        'station_unsupported': 'SW000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_SW000000001.dly'),
        'start_date': '2021-04-01',
        'end_date': '2021-04-02',
    },
    'AT': {
        'ghcn_prefix': 'AU',
        'national_provider': 'historical',
        'station_core': 'AU000000001',
        'station_prcp_only': 'AU000000002',
        'station_unsupported': 'AU000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_AU000000001.dly'),
        'start_date': '2021-05-01',
        'end_date': '2021-05-02',
    },
}


class MappedPrefixGhcndProviderTests(unittest.TestCase):
    def test_provider_capability_metadata_is_explicit_for_mapped_prefix_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                provider = get_provider(country)
                self.assertEqual(provider.supported_country_codes, (country,))
                self.assertIn('ghcnd', provider.supported_dataset_scopes)
                self.assertEqual(spec['national_provider'] in provider.supported_dataset_scopes, True)
                self.assertIn('daily', provider.supported_resolutions)
                self.assertIn('tas_max', provider.supported_canonical_elements)
                self.assertNotIn('open_water_evaporation', list_supported_elements(country=country, provider='ghcnd', resolution='daily'))

    def test_discovery_for_mapped_prefix_countries_returns_ghcnd_daily_without_evap(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                self.assertEqual(list_dataset_scopes(country=country), ['ghcnd', spec['national_provider']])
                self.assertEqual(list_providers(country=country), ['ghcnd', spec['national_provider']])
                self.assertEqual(list_resolutions(country=country, dataset_scope='ghcnd'), ['daily'])
                self.assertEqual(list_resolutions(country=country, provider='ghcnd'), ['daily'])
                self.assertEqual(
                    list_supported_elements(country=country, provider='ghcnd', resolution='daily'),
                    ['tas_max', 'tas_min', 'precipitation'],
                )
                self.assertEqual(
                    list_supported_elements(country=country, provider='ghcnd', resolution='daily', provider_raw=True),
                    ['TMAX', 'TMIN', 'PRCP'],
                )

    def test_query_normalizes_canonical_and_raw_elements_for_mapped_prefix_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                canonical_query = ObservationQuery(
                    country=country,
                    dataset_scope='ghcnd',
                    resolution='daily',
                    station_ids=[spec['station_core'].lower()],
                    start_date=spec['start_date'],
                    end_date=spec['end_date'],
                    elements=['tas_max', 'precipitation'],
                )
                raw_query = ObservationQuery(
                    country=country,
                    provider='ghcnd',
                    resolution='daily',
                    station_ids=[spec['station_core']],
                    start_date=spec['start_date'],
                    end_date=spec['end_date'],
                    elements=['TMAX', 'PRCP'],
                )
                self.assertEqual(canonical_query.station_ids, [spec['station_core']])
                self.assertEqual(canonical_query.elements, ['TMAX', 'PRCP'])
                self.assertEqual(raw_query.elements, ['TMAX', 'PRCP'])

    def test_shared_inventory_filter_can_build_country_specific_station_elements(self) -> None:
        inventory_table = parse_ghcnd_inventory_text(SAMPLE_INVENTORY_PATH.read_text(encoding='utf-8'))
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                station_elements = build_station_supported_raw_elements(
                    inventory_table,
                    country_prefix=spec['ghcn_prefix'],
                    supported_elements=('TMAX', 'TMIN', 'PRCP'),
                )
                self.assertEqual(
                    station_elements,
                    {
                        spec['station_core']: ['TMAX', 'TMIN', 'PRCP'],
                        spec['station_prcp_only']: ['PRCP'],
                    },
                )

    def test_read_station_metadata_filters_to_supported_inventory_for_mapped_prefix_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                stations = read_station_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertEqual(
                    list(stations.columns),
                    ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
                )
                self.assertEqual(stations['station_id'].tolist(), [spec['station_core'], spec['station_prcp_only']])
                self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_builds_rows_for_mapped_prefix_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                observation_metadata = read_station_observation_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertEqual(
                    observation_metadata[['station_id', 'element']].to_dict('records'),
                    [
                        {'station_id': spec['station_core'], 'element': 'PRCP'},
                        {'station_id': spec['station_core'], 'element': 'TMAX'},
                        {'station_id': spec['station_core'], 'element': 'TMIN'},
                        {'station_id': spec['station_prcp_only'], 'element': 'PRCP'},
                    ],
                )

    def test_parse_and_normalize_country_dly_converts_units_and_drops_missing(self) -> None:
        expected_values = {
            'DE': {'tas_max': (6.1, 7.3), 'tas_min': (-0.8, -0.3), 'precipitation': (1.2, 3.4)},
            'SK': {'tas_max': (-1.1, 0.2), 'tas_min': (-12.6, -9.8), 'precipitation': (0.5, 1.8)},
            'CH': {'tas_max': (7.5, 8.4), 'tas_min': (-0.9, 0.4), 'precipitation': (2.1, 0.8)},
            'DK': {'tas_max': (4.9, 6.7), 'tas_min': (1.1, 2.4), 'precipitation': (1.4, 0.2)},
            'SE': {'tas_max': (3.2, 4.5), 'tas_min': (-10.2, -8.8), 'precipitation': (0.0, 1.1)},
            'AT': {'tas_max': (20.1, 21.6), 'tas_min': (9.8, 10.5), 'precipitation': (1.9, 0.7)},
        }
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                raw_table = parse_ghcnd_dly_text(spec['fixture_path'].read_text(encoding='utf-8'))
                query = ObservationQuery(
                    country=country,
                    dataset_scope='ghcnd',
                    resolution='daily',
                    station_ids=[spec['station_core']],
                    start_date=spec['start_date'],
                    end_date=spec['end_date'],
                    elements=['tas_max', 'tas_min', 'precipitation'],
                )
                normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
                self.assertEqual(list(normalized.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
                lookup = {(row.element, str(row.observation_date)): row for row in normalized.itertuples(index=False)}
                start_date, end_date = spec['start_date'], spec['end_date']
                self.assertAlmostEqual(float(lookup[('tas_max', start_date)].value), expected_values[country]['tas_max'][0])
                self.assertAlmostEqual(float(lookup[('tas_max', end_date)].value), expected_values[country]['tas_max'][1])
                self.assertAlmostEqual(float(lookup[('tas_min', start_date)].value), expected_values[country]['tas_min'][0])
                self.assertAlmostEqual(float(lookup[('tas_min', end_date)].value), expected_values[country]['tas_min'][1])
                self.assertAlmostEqual(float(lookup[('precipitation', start_date)].value), expected_values[country]['precipitation'][0])
                self.assertAlmostEqual(float(lookup[('precipitation', end_date)].value), expected_values[country]['precipitation'][1])

    def test_download_observations_reads_local_fixture_and_canonicalizes_output(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                query = ObservationQuery(
                    country=country,
                    provider='ghcnd',
                    resolution='daily',
                    station_ids=[spec['station_core']],
                    start_date=spec['start_date'],
                    end_date=spec['end_date'],
                    elements=['tas_max', 'precipitation'],
                )
                with patch(
                    'weatherdownload.providers.ghcnd.observations._read_text',
                    return_value=spec['fixture_path'].read_text(encoding='utf-8'),
                ):
                    observations = download_observations(query, country=country)
                self.assertEqual(list(observations.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
                self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max'])
                self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['PRCP', 'TMAX'])

    def test_list_station_elements_for_mapped_prefix_countries_is_inventory_driven(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                stations = read_station_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertEqual(
                    list_station_elements(stations, spec['station_core'], 'ghcnd', 'daily', country=country),
                    ['tas_max', 'tas_min', 'precipitation'],
                )
                self.assertEqual(
                    list_station_elements(stations, spec['station_prcp_only'], 'ghcnd', 'daily', country=country),
                    ['precipitation'],
                )

    def test_station_metadata_excludes_unsupported_only_station_for_mapped_prefix_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                stations = read_station_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertNotIn(spec['station_unsupported'], stations['station_id'].tolist())

    def test_cli_station_elements_for_germany_ghcnd_uses_inventory_specific_availability(self) -> None:
        stations = read_station_metadata(country='DE', source_url=str(SAMPLE_STATIONS_PATH))
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=stations):
            with redirect_stdout(buffer):
                exit_code = main(['stations', 'elements', '--country', 'DE', '--provider', 'ghcnd', '--station-id', 'GM000000002', '--resolution', 'daily'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('precipitation', output)
        self.assertNotIn('tas_max', output)
        self.assertNotIn('open_water_evaporation', output)


if __name__ == '__main__':
    unittest.main()
