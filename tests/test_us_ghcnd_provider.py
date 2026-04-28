import unittest
from contextlib import redirect_stdout
import io
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    get_provider,
    list_dataset_scopes,
    list_resolutions,
    list_station_elements,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.cli import main
from weatherdownload.providers.us.observations import build_station_dly_url
from weatherdownload.providers.us.parser import (
    GHCND_NORMALIZED_DAILY_COLUMNS,
    normalize_daily_observations_ghcnd,
    parse_ghcnd_dly_text,
    parse_ghcnd_inventory_text,
    parse_ghcnd_stations_text,
)
from weatherdownload.providers.us.registry import get_dataset_spec

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_INVENTORY_PATH = Path('tests/data/sample_ghcnd_inventory.txt')
SAMPLE_DLY_PATH = Path('tests/data/sample_ghcnd_USC00000001.dly')


class GhcndProviderTests(unittest.TestCase):
    def test_supported_countries_include_us(self) -> None:
        self.assertIn('US', list_supported_countries())

    def test_provider_capability_metadata_is_explicit(self) -> None:
        provider = get_provider('US')
        self.assertEqual(provider.supported_country_codes, ('US',))
        self.assertEqual(provider.supported_dataset_scopes, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(provider.supported_canonical_elements, ('tas_max', 'tas_min', 'precipitation', 'open_water_evaporation'))

    def test_discovery_country_us_returns_ghcnd_daily_elements(self) -> None:
        self.assertEqual(list_dataset_scopes(country='US'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='US', dataset_scope='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='US', dataset_scope='ghcnd', resolution='daily'),
            ['tas_max', 'tas_min', 'precipitation', 'open_water_evaporation'],
        )
        self.assertEqual(
            list_supported_elements(country='US', dataset_scope='ghcnd', resolution='daily', provider_raw=True),
            ['TMAX', 'TMIN', 'PRCP', 'EVAP'],
        )
        mapping = list_supported_elements(country='US', dataset_scope='ghcnd', resolution='daily', include_mapping=True)
        self.assertEqual(
            mapping[['element', 'element_raw']].to_dict('records'),
            [
                {'element': 'tas_max', 'element_raw': 'TMAX'},
                {'element': 'tas_min', 'element_raw': 'TMIN'},
                {'element': 'precipitation', 'element_raw': 'PRCP'},
                {'element': 'open_water_evaporation', 'element_raw': 'EVAP'},
            ],
        )

    def test_query_normalizes_canonical_and_raw_ghcnd_elements(self) -> None:
        canonical_query = ObservationQuery(
            country='US',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['usc00000001'],
            start_date='2020-05-01',
            end_date='2020-05-02',
            elements=['tas_max', 'precipitation', 'open_water_evaporation'],
        )
        raw_query = ObservationQuery(
            country='US',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['USC00000001'],
            start_date='2020-05-01',
            end_date='2020-05-02',
            elements=['TMAX', 'PRCP', 'EVAP'],
        )
        self.assertEqual(canonical_query.station_ids, ['USC00000001'])
        self.assertEqual(canonical_query.elements, ['TMAX', 'PRCP', 'EVAP'])
        self.assertEqual(raw_query.elements, ['TMAX', 'PRCP', 'EVAP'])

    def test_read_station_metadata_filters_to_us_supported_core_inventory(self) -> None:
        stations = read_station_metadata(country='US', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['USC00000001', 'USC00000002', 'USC00000003'])
        self.assertEqual(stations.iloc[0]['begin_date'], '2017-01-01T00:00Z')
        self.assertEqual(stations.iloc[0]['end_date'], '2020-12-31T00:00Z')
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_builds_rows_for_supported_elements(self) -> None:
        observation_metadata = read_station_observation_metadata(country='US', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(observation_metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(observation_metadata['station_id'].unique().tolist(), ['USC00000001', 'USC00000002', 'USC00000003'])
        self.assertEqual(
            observation_metadata[['station_id', 'element']].to_dict('records'),
            [
                {'station_id': 'USC00000001', 'element': 'EVAP'},
                {'station_id': 'USC00000001', 'element': 'PRCP'},
                {'station_id': 'USC00000001', 'element': 'TMAX'},
                {'station_id': 'USC00000001', 'element': 'TMIN'},
                {'station_id': 'USC00000002', 'element': 'PRCP'},
                {'station_id': 'USC00000002', 'element': 'TMAX'},
                {'station_id': 'USC00000002', 'element': 'TMIN'},
                {'station_id': 'USC00000003', 'element': 'EVAP'},
            ],
        )
        self.assertTrue(observation_metadata.iloc[0]['description'].startswith('Evaporation of water from evaporation pan'))

    def test_parse_station_and_inventory_fixtures(self) -> None:
        stations_table = parse_ghcnd_stations_text(SAMPLE_STATIONS_PATH.read_text(encoding='utf-8'))
        inventory_table = parse_ghcnd_inventory_text(SAMPLE_INVENTORY_PATH.read_text(encoding='utf-8'))
        self.assertEqual(stations_table.iloc[0]['station_id'], 'USC00000001')
        self.assertEqual(inventory_table.iloc[0]['element_raw'], 'EVAP')
        self.assertEqual(inventory_table.iloc[0]['begin_year'], 2018)

    def test_parse_dly_expands_monthly_supported_records(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_DLY_PATH.read_text(encoding='utf-8'))
        self.assertEqual(len(raw_table), 124)
        self.assertEqual(raw_table.iloc[0]['station_id'], 'USC00000001')
        self.assertEqual(raw_table.iloc[0]['element_raw'], 'EVAP')
        self.assertEqual(raw_table.iloc[0]['value_raw'], 12)
        self.assertEqual(raw_table.iloc[1]['qflag'], 'X')
        self.assertEqual(raw_table.iloc[2]['value_raw'], -9999)

    def test_parse_dly_handles_shorter_month_without_invalid_day_dates(self) -> None:
        september_text = SAMPLE_DLY_PATH.read_text(encoding='utf-8').replace('202005', '191709', 1)
        raw_table = parse_ghcnd_dly_text(september_text)
        self.assertEqual(len(raw_table), 123)
        evap_rows = raw_table[raw_table['element_raw'] == 'EVAP']
        self.assertEqual(evap_rows.iloc[-1]['observation_date'].isoformat(), '1917-09-30')

    def test_normalize_daily_observations_converts_supported_units_and_filters_dates(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='US',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['USC00000001'],
            start_date='2020-05-01',
            end_date='2020-05-03',
            elements=['tas_max', 'tas_min', 'precipitation', 'open_water_evaporation'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(list(normalized.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(
            sorted(normalized['element'].unique().tolist()),
            ['open_water_evaporation', 'precipitation', 'tas_max', 'tas_min'],
        )
        lookup = {
            (row.element, str(row.observation_date)): row
            for row in normalized.itertuples(index=False)
        }
        self.assertAlmostEqual(float(lookup[('open_water_evaporation', '2020-05-01')].value), 1.2)
        self.assertAlmostEqual(float(lookup[('open_water_evaporation', '2020-05-02')].value), 0.0)
        self.assertAlmostEqual(float(lookup[('precipitation', '2020-05-01')].value), 2.5)
        self.assertAlmostEqual(float(lookup[('tas_max', '2020-05-01')].value), 21.5)
        self.assertAlmostEqual(float(lookup[('tas_min', '2020-05-01')].value), 5.5)
        self.assertTrue(pd.isna(lookup[('open_water_evaporation', '2020-05-01')].quality))
        self.assertEqual(lookup[('open_water_evaporation', '2020-05-02')].quality, 'X')

    def test_build_station_dly_url_uses_official_all_directory(self) -> None:
        self.assertEqual(
            build_station_dly_url('USC00000001'),
            'https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/USC00000001.dly',
        )

    def test_download_observations_reads_local_dly_fixture_and_canonicalizes_output(self) -> None:
        query = ObservationQuery(
            country='US',
            dataset_scope='ghcnd',
            resolution='daily',
            station_ids=['USC00000001'],
            start_date='2020-05-01',
            end_date='2020-05-03',
            elements=['tas_max', 'precipitation', 'open_water_evaporation'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='US')
        self.assertEqual(list(observations.columns), GHCND_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['open_water_evaporation', 'precipitation', 'tas_max'],
        )
        self.assertEqual(
            sorted(observations['element_raw'].unique().tolist()),
            ['EVAP', 'PRCP', 'TMAX'],
        )
        self.assertAlmostEqual(float(observations[observations['element'] == 'open_water_evaporation'].iloc[0]['value']), 1.2)

    def test_list_station_elements_for_us_fixture_station(self) -> None:
        stations = read_station_metadata(country='US', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list_station_elements(stations, 'USC00000001', 'ghcnd', 'daily', country='US'),
            ['tas_max', 'tas_min', 'precipitation', 'open_water_evaporation'],
        )
        self.assertEqual(
            list_station_elements(stations, 'USC00000002', 'ghcnd', 'daily', country='US'),
            ['tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_station_elements(stations, 'USC00000003', 'ghcnd', 'daily', country='US'),
            ['open_water_evaporation'],
        )
        mapping = list_station_elements(stations, 'USC00000002', 'ghcnd', 'daily', country='US', include_mapping=True)
        self.assertEqual(mapping['element_raw'].tolist(), ['TMAX', 'TMIN', 'PRCP'])
        evap_mapping = list_station_elements(stations, 'USC00000003', 'ghcnd', 'daily', country='US', include_mapping=True)
        self.assertEqual(evap_mapping['element_raw'].tolist(), ['EVAP'])

    def test_station_metadata_excludes_station_with_only_unsupported_elements(self) -> None:
        stations = read_station_metadata(country='US', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertNotIn('USC00000004', stations['station_id'].tolist())

    def test_cli_station_elements_uses_inventory_specific_availability(self) -> None:
        stations = read_station_metadata(country='US', source_url=str(SAMPLE_STATIONS_PATH))
        buffer = io.StringIO()
        with patch('weatherdownload.cli.read_station_metadata', return_value=stations):
            with redirect_stdout(buffer):
                exit_code = main(['stations', 'elements', '--country', 'US', '--station-id', 'USC00000002', '--dataset-scope', 'ghcnd', '--resolution', 'daily'])
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn('tas_max', output)
        self.assertIn('tas_min', output)
        self.assertIn('precipitation', output)
        self.assertNotIn('open_water_evaporation', output)


if __name__ == '__main__':
    unittest.main()
