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
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.cli import main
from weatherdownload.providers.ghcnd.parser import build_station_supported_raw_elements
from weatherdownload.providers.fi.parser import (
    GHCND_NORMALIZED_DAILY_COLUMNS,
    normalize_daily_observations_ghcnd,
    parse_ghcnd_dly_text,
    parse_ghcnd_inventory_text,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_INVENTORY_PATH = Path('tests/data/sample_ghcnd_inventory.txt')

COUNTRY_SPECS = {
    'FI': {
        'station_core': 'FI000000001',
        'station_prcp_only': 'FI000000002',
        'station_unsupported': 'FI000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_FI000000001.dly'),
        'start_date': '2020-08-01',
        'end_date': '2020-08-02',
    },
    'FR': {
        'station_core': 'FR000000001',
        'station_prcp_only': 'FR000000002',
        'station_unsupported': 'FR000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_FR000000001.dly'),
        'start_date': '2020-10-01',
        'end_date': '2020-10-02',
    },
    'IT': {
        'station_core': 'IT000000001',
        'station_prcp_only': 'IT000000002',
        'station_unsupported': 'IT000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_IT000000001.dly'),
        'start_date': '2020-11-01',
        'end_date': '2020-11-02',
    },
    'NO': {
        'station_core': 'NO000000001',
        'station_prcp_only': 'NO000000002',
        'station_unsupported': 'NO000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_NO000000001.dly'),
        'start_date': '2020-09-01',
        'end_date': '2020-09-02',
    },
    'NZ': {
        'station_core': 'NZ000000001',
        'station_prcp_only': 'NZ000000002',
        'station_unsupported': 'NZ000000003',
        'fixture_path': Path('tests/data/sample_ghcnd_NZ000000001.dly'),
        'start_date': '1998-01-01',
        'end_date': '1998-01-02',
    },
}


class DirectPrefixGhcndProviderTests(unittest.TestCase):
    def test_supported_countries_include_new_direct_prefix_ghcnd_countries(self) -> None:
        supported = set(list_supported_countries())
        for country in COUNTRY_SPECS:
            self.assertIn(country, supported)

    def test_provider_capability_metadata_is_explicit_for_new_countries(self) -> None:
        for country in COUNTRY_SPECS:
            with self.subTest(country=country):
                provider = get_provider(country)
                self.assertEqual(provider.supported_country_codes, (country,))
                self.assertEqual(provider.supported_dataset_scopes, ('ghcnd',))
                self.assertEqual(provider.supported_resolutions, ('daily',))
                self.assertEqual(
                    provider.supported_canonical_elements,
                    ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
                )

    def test_discovery_for_new_countries_returns_ghcnd_daily_without_evap(self) -> None:
        for country in COUNTRY_SPECS:
            with self.subTest(country=country):
                self.assertEqual(list_dataset_scopes(country=country), ['ghcnd'])
                self.assertEqual(list_providers(country=country), ['ghcnd'])
                self.assertEqual(list_resolutions(country=country, dataset_scope='ghcnd'), ['daily'])
                self.assertEqual(list_resolutions(country=country, provider='ghcnd'), ['daily'])
                self.assertEqual(
                    list_supported_elements(country=country, provider='ghcnd', resolution='daily'),
                    ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
                )
                self.assertEqual(
                    list_supported_elements(country=country, provider='ghcnd', resolution='daily', provider_raw=True),
                    ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
                )

    def test_query_normalizes_canonical_and_raw_elements_for_new_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                canonical_query = ObservationQuery(
                    country=country,
                    dataset_scope='ghcnd',
                    resolution='daily',
                    station_ids=[spec['station_core'].lower()],
                    start_date=spec['start_date'],
                    end_date=spec['end_date'],
                    elements=['tas_mean', 'tas_max', 'precipitation', 'snow_depth'],
                )
                raw_query = ObservationQuery(
                    country=country,
                    dataset_scope='ghcnd',
                    resolution='daily',
                    station_ids=[spec['station_core']],
                    start_date=spec['start_date'],
                    end_date=spec['end_date'],
                    elements=['TAVG', 'TMAX', 'PRCP', 'SNWD'],
                )
                self.assertEqual(canonical_query.station_ids, [spec['station_core']])
                self.assertEqual(canonical_query.elements, ['TAVG', 'TMAX', 'PRCP', 'SNWD'])
                self.assertEqual(raw_query.elements, ['TAVG', 'TMAX', 'PRCP', 'SNWD'])

    def test_shared_inventory_filter_can_build_country_specific_station_elements(self) -> None:
        inventory_table = parse_ghcnd_inventory_text(SAMPLE_INVENTORY_PATH.read_text(encoding='utf-8'))
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                station_elements = build_station_supported_raw_elements(
                    inventory_table,
                    country_prefix=country,
                    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
                )
                self.assertEqual(
                    station_elements,
                    {
                        spec['station_core']: ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
                        spec['station_prcp_only']: ['PRCP'],
                    },
                )

    def test_read_station_metadata_filters_to_supported_inventory_for_new_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                stations = read_station_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertEqual(
                    list(stations.columns),
                    ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
                )
                self.assertEqual(stations['station_id'].tolist(), [spec['station_core'], spec['station_prcp_only']])
                self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_builds_rows_for_new_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                observation_metadata = read_station_observation_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertEqual(
                    observation_metadata[['station_id', 'element']].to_dict('records'),
                    [
                        {'station_id': spec['station_core'], 'element': 'PRCP'},
                        {'station_id': spec['station_core'], 'element': 'SNWD'},
                        {'station_id': spec['station_core'], 'element': 'TAVG'},
                        {'station_id': spec['station_core'], 'element': 'TMAX'},
                        {'station_id': spec['station_core'], 'element': 'TMIN'},
                        {'station_id': spec['station_prcp_only'], 'element': 'PRCP'},
                    ],
                )

    def test_parse_and_normalize_fi_dly_converts_units_and_drops_missing(self) -> None:
        raw_table = parse_ghcnd_dly_text(COUNTRY_SPECS['FI']['fixture_path'].read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='FI',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['FI000000001'],
            start_date='2020-08-01',
            end_date='2020-08-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(list(normalized.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        lookup = {(row.element, str(row.observation_date)): row for row in normalized.itertuples(index=False)}
        self.assertAlmostEqual(float(lookup[('tas_mean', '2020-08-01')].value), 18.0)
        self.assertAlmostEqual(float(lookup[('tas_mean', '2020-08-02')].value), 18.5)
        self.assertAlmostEqual(float(lookup[('tas_max', '2020-08-01')].value), 24.5)
        self.assertAlmostEqual(float(lookup[('tas_max', '2020-08-02')].value), 25.0)
        self.assertAlmostEqual(float(lookup[('tas_min', '2020-08-01')].value), 11.5)
        self.assertNotIn(('tas_min', '2020-08-02'), lookup)
        self.assertAlmostEqual(float(lookup[('precipitation', '2020-08-01')].value), 2.2)
        self.assertAlmostEqual(float(lookup[('precipitation', '2020-08-02')].value), 0.1)
        self.assertAlmostEqual(float(lookup[('snow_depth', '2020-08-01')].value), 0.0)
        self.assertAlmostEqual(float(lookup[('snow_depth', '2020-08-02')].value), 0.0)

    def test_download_observations_reads_local_fi_fixture_and_canonicalizes_output(self) -> None:
        query = ObservationQuery(
            country='FI',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['FI000000001'],
            start_date='2020-08-01',
            end_date='2020-08-02',
            elements=['tas_max', 'precipitation'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=COUNTRY_SPECS['FI']['fixture_path'].read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='FI')
        self.assertEqual(list(observations.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['PRCP', 'TMAX'])

    def test_list_station_elements_for_new_countries_is_inventory_driven(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                stations = read_station_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertEqual(
                    list_station_elements(stations, spec['station_core'], 'ghcnd', 'daily', country=country),
                    ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
                )
                self.assertEqual(
                    list_station_elements(stations, spec['station_prcp_only'], 'ghcnd', 'daily', country=country),
                    ['precipitation'],
                )

    def test_station_metadata_excludes_unsupported_only_station_for_new_countries(self) -> None:
        for country, spec in COUNTRY_SPECS.items():
            with self.subTest(country=country):
                stations = read_station_metadata(country=country, source_url=str(SAMPLE_STATIONS_PATH))
                self.assertNotIn(spec['station_unsupported'], stations['station_id'].tolist())

    def test_cli_station_elements_for_fi_uses_inventory_specific_availability(self) -> None:
        stations = read_station_metadata(country='FI', source_url=str(SAMPLE_STATIONS_PATH))
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=stations):
            with redirect_stdout(buffer):
                exit_code = main(['stations', 'elements', '--country', 'FI', '--provider', 'ghcnd', '--station-id', 'FI000000002', '--resolution', 'daily'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('precipitation', output)
        self.assertNotIn('tas_max', output)
        self.assertNotIn('open_water_evaporation', output)


if __name__ == '__main__':
    unittest.main()
