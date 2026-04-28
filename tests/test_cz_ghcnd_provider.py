import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_dataset_scopes,
    list_providers,
    list_resolutions,
    list_station_elements,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
    station_supports,
)
from weatherdownload.cli import main
from weatherdownload.providers.ghcnd.parser import build_station_supported_raw_elements
from weatherdownload.providers.us.parser import GHCND_NORMALIZED_DAILY_COLUMNS, normalize_daily_observations_ghcnd, parse_ghcnd_dly_text, parse_ghcnd_inventory_text

SAMPLE_CHMI_META1_PATH = Path('tests/data/sample_meta1.csv')
SAMPLE_GHCND_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_GHCND_INVENTORY_PATH = Path('tests/data/sample_ghcnd_inventory.txt')
SAMPLE_GHCND_DLY_PATH = Path('tests/data/sample_ghcnd_EZM00011406.dly')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class CzechGhcndProviderTests(unittest.TestCase):
    def test_discovery_country_cz_includes_historical_csv_and_ghcnd(self) -> None:
        self.assertEqual(list_dataset_scopes(country='CZ'), ['ghcnd', 'historical', 'historical_csv', 'now', 'recent'])
        self.assertEqual(list_providers(country='CZ'), ['ghcnd', 'historical', 'historical_csv', 'now', 'recent'])
        self.assertEqual(list_resolutions(country='CZ', provider='ghcnd'), ['daily'])

    def test_discovery_country_cz_ghcnd_daily_elements_exclude_open_water_evaporation(self) -> None:
        self.assertEqual(
            list_supported_elements(country='CZ', provider='ghcnd', resolution='daily'),
            ['tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='CZ', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TMAX', 'TMIN', 'PRCP'],
        )
        self.assertIn(
            'open_water_evaporation',
            list_supported_elements(country='CZ', provider='historical_csv', resolution='daily'),
        )

    def test_query_normalizes_canonical_and_raw_cz_ghcnd_elements(self) -> None:
        canonical_query = ObservationQuery(
            country='CZ',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['ezm00011406'],
            start_date='2020-05-01',
            end_date='2020-05-02',
            elements=['tas_max', 'precipitation'],
        )
        raw_query = ObservationQuery(
            country='CZ',
            provider='ghcnd',
            resolution='daily',
            station_ids=['EZM00011406'],
            start_date='2020-05-01',
            end_date='2020-05-02',
            elements=['TMAX', 'PRCP'],
        )
        self.assertEqual(canonical_query.station_ids, ['EZM00011406'])
        self.assertEqual(canonical_query.elements, ['TMAX', 'PRCP'])
        self.assertEqual(raw_query.elements, ['TMAX', 'PRCP'])

    def test_shared_inventory_filter_can_build_cz_station_elements_from_ez_prefix(self) -> None:
        inventory_table = parse_ghcnd_inventory_text(SAMPLE_GHCND_INVENTORY_PATH.read_text(encoding='utf-8'))
        station_elements = build_station_supported_raw_elements(
            inventory_table,
            country_prefix='EZ',
            supported_elements=('TMAX', 'TMIN', 'PRCP'),
        )
        self.assertEqual(
            station_elements,
            {
                'EZM00011406': ['TMAX', 'TMIN', 'PRCP'],
                'EZM00011520': ['PRCP'],
            },
        )

    def test_read_station_metadata_country_cz_with_ghcnd_source_uses_raw_ghcn_ids(self) -> None:
        stations = read_station_metadata(country='CZ', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['EZM00011406', 'EZM00011520'])
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_cz_with_ghcnd_source_is_inventory_driven(self) -> None:
        observation_metadata = read_station_observation_metadata(country='CZ', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        self.assertEqual(
            observation_metadata[['station_id', 'element']].to_dict('records'),
            [
                {'station_id': 'EZM00011406', 'element': 'PRCP'},
                {'station_id': 'EZM00011406', 'element': 'TMAX'},
                {'station_id': 'EZM00011406', 'element': 'TMIN'},
                {'station_id': 'EZM00011520', 'element': 'PRCP'},
            ],
        )

    def test_default_cz_station_metadata_combines_chmi_and_ghcnd_without_cross_advertising(self) -> None:
        chmi_text = SAMPLE_CHMI_META1_PATH.read_text(encoding='utf-8')
        ghcnd_stations = read_station_metadata(country='CZ', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(chmi_text)):
            with patch('weatherdownload.providers.cz.read_station_metadata_ghcnd', return_value=ghcnd_stations):
                stations = read_station_metadata(country='CZ')
        self.assertIn('0-20000-0-11406', stations['station_id'].tolist())
        self.assertIn('EZM00011406', stations['station_id'].tolist())
        self.assertFalse(station_supports(stations, '0-20000-0-11406', 'ghcnd', 'daily', country='CZ'))
        self.assertFalse(station_supports(stations, 'EZM00011406', 'historical_csv', 'daily', country='CZ'))
        self.assertEqual(list_station_elements(stations, 'EZM00011406', 'ghcnd', 'daily', country='CZ'), ['tas_max', 'tas_min', 'precipitation'])
        self.assertEqual(list_station_elements(stations, '0-20000-0-11406', 'historical_csv', 'daily', country='CZ')[0], 'open_water_evaporation')

    def test_parse_and_normalize_cz_ghcnd_dly_converts_units_and_drops_missing(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_GHCND_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='CZ',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['EZM00011406'],
            start_date='2020-05-01',
            end_date='2020-05-02',
            elements=['tas_max', 'tas_min', 'precipitation'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(list(normalized.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        lookup = {(row.element, str(row.observation_date)): row for row in normalized.itertuples(index=False)}
        self.assertAlmostEqual(float(lookup[('tas_max', '2020-05-01')].value), 21.5)
        self.assertAlmostEqual(float(lookup[('tas_max', '2020-05-02')].value), 22.0)
        self.assertAlmostEqual(float(lookup[('precipitation', '2020-05-01')].value), 1.8)
        self.assertAlmostEqual(float(lookup[('precipitation', '2020-05-02')].value), 4.2)
        self.assertAlmostEqual(float(lookup[('tas_min', '2020-05-01')].value), 5.5)
        self.assertNotIn(('tas_min', '2020-05-02'), lookup)

    def test_download_observations_reads_local_cz_ghcnd_fixture_and_canonicalizes_output(self) -> None:
        query = ObservationQuery(
            country='CZ',
            provider='ghcnd',
            resolution='daily',
            station_ids=['EZM00011406'],
            start_date='2020-05-01',
            end_date='2020-05-02',
            elements=['tas_max', 'precipitation'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_GHCND_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='CZ')
        self.assertEqual(list(observations.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['PRCP', 'TMAX'])

    def test_cli_station_elements_for_cz_ghcnd_uses_inventory_specific_availability(self) -> None:
        stations = read_station_metadata(country='CZ', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=stations):
            with redirect_stdout(buffer):
                exit_code = main(['stations', 'elements', '--country', 'CZ', '--provider', 'ghcnd', '--station-id', 'EZM00011520', '--resolution', 'daily'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('precipitation', output)
        self.assertNotIn('tas_max', output)
        self.assertNotIn('open_water_evaporation', output)


if __name__ == '__main__':
    unittest.main()
