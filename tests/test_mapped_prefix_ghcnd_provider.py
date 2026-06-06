import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    get_provider,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.fi.parser import normalize_daily_observations_ghcnd, parse_ghcnd_dly_text

SAMPLE_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_INVENTORY_PATH = Path('tests/data/sample_ghcnd_inventory.txt')
SAMPLE_BK_DLY_PATH = Path('tests/data/sample_ghcnd_BK000000001.dly')
SAMPLE_BO_DLY_PATH = Path('tests/data/sample_ghcnd_BO000000001.dly')
SAMPLE_LO_DLY_PATH = Path('tests/data/sample_ghcnd_LO000000001.dly')
SAMPLE_MJ_DLY_PATH = Path('tests/data/sample_ghcnd_MJ000000001.dly')
SAMPLE_RI_DLY_PATH = Path('tests/data/sample_ghcnd_RI000000001.dly')
SAMPLE_TU_DLY_PATH = Path('tests/data/sample_ghcnd_TU000000001.dly')
SAMPLE_UP_DLY_PATH = Path('tests/data/sample_ghcnd_UP000000001.dly')


class MappedPrefixGhcndProviderTests(unittest.TestCase):
    def test_by_provider_is_explicit_ghcnd_daily_only(self) -> None:
        provider = get_provider('BY')
        self.assertEqual(provider.supported_country_codes, ('BY',))
        self.assertEqual(provider.supported_providers, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(
            provider.supported_canonical_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
        )

    def test_by_discovery_exposes_conservative_shared_ghcnd_contract(self) -> None:
        self.assertEqual(list_providers(country='BY'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='BY', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='BY', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='BY', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )

    def test_by_station_metadata_filters_bo_prefix_not_by(self) -> None:
        stations = read_station_metadata(country='BY', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['BO000000001', 'BO000000002'])
        self.assertTrue(stations['station_id'].str.startswith('BO').all())
        self.assertFalse(stations['station_id'].str.startswith('BY').any())

    def test_by_station_elements_follow_bo_inventory_not_by(self) -> None:
        station_elements = read_station_observation_metadata(
            country='BY',
            source_url=str(SAMPLE_INVENTORY_PATH),
        )
        self.assertEqual(station_elements['station_id'].unique().tolist(), ['BO000000001', 'BO000000002'])
        self.assertTrue(station_elements['station_id'].str.startswith('BO').all())
        self.assertFalse(station_elements['station_id'].str.startswith('BY').any())

    def test_by_download_uses_bo_station_ids_and_shared_normalization(self) -> None:
        station_metadata = read_station_metadata(country='BY', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='BY',
            provider='ghcnd',
            resolution='daily',
            station_ids=['BO000000001'],
            start_date='2021-02-01',
            end_date='2021-02-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_BO_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='BY', station_metadata=station_metadata)

        values = {(row.element, str(row.observation_date)): row.value for row in observations.itertuples()}
        self.assertEqual(values[('tas_mean', '2021-02-01')], -1.5)
        self.assertEqual(values[('tas_max', '2021-02-01')], 2.1)
        self.assertEqual(values[('tas_min', '2021-02-01')], -4.3)
        self.assertEqual(values[('precipitation', '2021-02-01')], 0.7)
        self.assertEqual(values[('snow_depth', '2021-02-01')], 180.0)

    def test_by_fixture_parses_through_shared_ghcnd_parser(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_BO_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='BY',
            provider='ghcnd',
            resolution='daily',
            station_ids=['BO000000001'],
            start_date='2021-02-01',
            end_date='2021-02-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(sorted(normalized['station_id'].unique().tolist()), ['BO000000001'])
        self.assertFalse((normalized['station_id'].str.startswith('BY')).any())

    def test_sk_discovery_keeps_recent_and_exposes_stable_ghcnd_daily_contract(self) -> None:
        self.assertEqual(list_providers(country='SK'), ['ghcnd', 'recent'])
        self.assertEqual(list_resolutions(country='SK', provider='ghcnd'), ['daily'])
        self.assertEqual(list_resolutions(country='SK', provider='recent'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='SK', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='SK', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )
        self.assertEqual(
            list_supported_elements(country='SK', provider='recent', resolution='daily'),
            ['tas_max', 'tas_min', 'sunshine_duration', 'precipitation', 'open_water_evaporation'],
        )

    def test_sk_station_metadata_filters_lo_prefix_not_sk(self) -> None:
        stations = read_station_metadata(country='SK', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['LO000000001', 'LO000000002'])
        self.assertTrue(stations['station_id'].str.startswith('LO').all())
        self.assertFalse(stations['station_id'].str.startswith('SK').any())

    def test_sk_station_elements_follow_lo_inventory_not_sk(self) -> None:
        station_elements = read_station_observation_metadata(
            country='SK',
            source_url=str(SAMPLE_INVENTORY_PATH),
        )
        self.assertEqual(station_elements['station_id'].unique().tolist(), ['LO000000001', 'LO000000002'])
        self.assertTrue(station_elements['station_id'].str.startswith('LO').all())
        self.assertFalse(station_elements['station_id'].str.startswith('SK').any())

    def test_sk_ghcnd_download_uses_lo_station_ids_and_shared_normalization(self) -> None:
        station_metadata = read_station_metadata(country='SK', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='SK',
            provider='ghcnd',
            resolution='daily',
            station_ids=['LO000000001'],
            start_date='2021-01-01',
            end_date='2021-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_LO_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='SK', station_metadata=station_metadata)

        values = {(row.element, str(row.observation_date)): row.value for row in observations.itertuples()}
        self.assertEqual(values[('tas_mean', '2021-01-01')], -6.8)
        self.assertEqual(values[('tas_max', '2021-01-01')], -1.1)
        self.assertEqual(values[('tas_min', '2021-01-01')], -12.6)
        self.assertEqual(values[('precipitation', '2021-01-01')], 0.5)
        self.assertEqual(values[('snow_depth', '2021-01-01')], 220.0)

    def test_sk_fixture_parses_through_shared_ghcnd_parser(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_LO_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='SK',
            provider='ghcnd',
            resolution='daily',
            station_ids=['LO000000001'],
            start_date='2021-01-01',
            end_date='2021-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(sorted(normalized['station_id'].unique().tolist()), ['LO000000001'])
        self.assertFalse((normalized['station_id'].str.startswith('SK')).any())

    def test_ba_provider_is_explicit_ghcnd_daily_only(self) -> None:
        provider = get_provider('BA')
        self.assertEqual(provider.supported_country_codes, ('BA',))
        self.assertEqual(provider.supported_providers, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(
            provider.supported_canonical_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
        )

    def test_ba_discovery_exposes_conservative_shared_ghcnd_contract(self) -> None:
        self.assertEqual(list_providers(country='BA'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='BA', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='BA', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='BA', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )

    def test_ba_station_metadata_filters_bk_prefix_not_ba(self) -> None:
        stations = read_station_metadata(country='BA', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['BK000000001', 'BK000000002'])
        self.assertTrue(stations['station_id'].str.startswith('BK').all())
        self.assertFalse(stations['station_id'].str.startswith('BA').any())

    def test_ba_station_elements_follow_bk_inventory_not_ba(self) -> None:
        station_elements = read_station_observation_metadata(
            country='BA',
            source_url=str(SAMPLE_INVENTORY_PATH),
        )
        self.assertEqual(station_elements['station_id'].unique().tolist(), ['BK000000001', 'BK000000002'])
        self.assertTrue(station_elements['station_id'].str.startswith('BK').all())
        self.assertFalse(station_elements['station_id'].str.startswith('BA').any())

    def test_ba_download_uses_bk_station_ids_and_shared_normalization(self) -> None:
        station_metadata = read_station_metadata(country='BA', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='BA',
            provider='ghcnd',
            resolution='daily',
            station_ids=['BK000000001'],
            start_date='2021-06-01',
            end_date='2021-06-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_BK_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='BA', station_metadata=station_metadata)

        values = {(row.element, str(row.observation_date)): row.value for row in observations.itertuples()}
        self.assertEqual(values[('tas_mean', '2021-06-01')], 17.2)
        self.assertEqual(values[('tas_max', '2021-06-01')], 24.6)
        self.assertEqual(values[('tas_min', '2021-06-01')], 9.8)
        self.assertEqual(values[('precipitation', '2021-06-01')], 1.1)
        self.assertEqual(values[('snow_depth', '2021-06-01')], 0.0)

    def test_ba_fixture_parses_through_shared_ghcnd_parser(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_BK_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='BA',
            provider='ghcnd',
            resolution='daily',
            station_ids=['BK000000001'],
            start_date='2021-06-01',
            end_date='2021-06-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(sorted(normalized['station_id'].unique().tolist()), ['BK000000001'])
        self.assertFalse((normalized['station_id'].str.startswith('BA')).any())

    def test_me_provider_is_explicit_ghcnd_daily_only(self) -> None:
        provider = get_provider('ME')
        self.assertEqual(provider.supported_country_codes, ('ME',))
        self.assertEqual(provider.supported_providers, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(
            provider.supported_canonical_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
        )

    def test_me_discovery_exposes_conservative_shared_ghcnd_contract(self) -> None:
        self.assertEqual(list_providers(country='ME'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='ME', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='ME', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='ME', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )

    def test_me_station_metadata_filters_mj_prefix_not_me(self) -> None:
        stations = read_station_metadata(country='ME', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['MJ000000001', 'MJ000000002'])
        self.assertTrue(stations['station_id'].str.startswith('MJ').all())
        self.assertFalse(stations['station_id'].str.startswith('ME').any())

    def test_me_station_elements_follow_mj_inventory_not_me(self) -> None:
        station_elements = read_station_observation_metadata(
            country='ME',
            source_url=str(SAMPLE_INVENTORY_PATH),
        )
        self.assertEqual(station_elements['station_id'].unique().tolist(), ['MJ000000001', 'MJ000000002'])
        self.assertTrue(station_elements['station_id'].str.startswith('MJ').all())
        self.assertFalse(station_elements['station_id'].str.startswith('ME').any())

    def test_me_download_uses_mj_station_ids_and_shared_normalization(self) -> None:
        station_metadata = read_station_metadata(country='ME', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='ME',
            provider='ghcnd',
            resolution='daily',
            station_ids=['MJ000000001'],
            start_date='2021-05-01',
            end_date='2021-05-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_MJ_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='ME', station_metadata=station_metadata)

        values = {(row.element, str(row.observation_date)): row.value for row in observations.itertuples()}
        self.assertEqual(values[('tas_mean', '2021-05-01')], 13.6)
        self.assertEqual(values[('tas_max', '2021-05-01')], 20.7)
        self.assertEqual(values[('tas_min', '2021-05-01')], 7.4)
        self.assertEqual(values[('precipitation', '2021-05-01')], 1.4)
        self.assertEqual(values[('snow_depth', '2021-05-01')], 0.0)

    def test_me_fixture_parses_through_shared_ghcnd_parser(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_MJ_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='ME',
            provider='ghcnd',
            resolution='daily',
            station_ids=['MJ000000001'],
            start_date='2021-05-01',
            end_date='2021-05-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(sorted(normalized['station_id'].unique().tolist()), ['MJ000000001'])
        self.assertFalse((normalized['station_id'].str.startswith('ME')).any())

    def test_rs_provider_is_explicit_ghcnd_daily_only(self) -> None:
        provider = get_provider('RS')
        self.assertEqual(provider.supported_country_codes, ('RS',))
        self.assertEqual(provider.supported_providers, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(
            provider.supported_canonical_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
        )

    def test_rs_discovery_exposes_conservative_shared_ghcnd_contract(self) -> None:
        self.assertEqual(list_providers(country='RS'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='RS', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='RS', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='RS', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )

    def test_rs_station_metadata_filters_ri_prefix_not_rs(self) -> None:
        stations = read_station_metadata(country='RS', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['RI000000001', 'RI000000002'])
        self.assertTrue(stations['station_id'].str.startswith('RI').all())
        self.assertFalse(stations['station_id'].str.startswith('RS').any())

    def test_rs_station_elements_follow_ri_inventory_not_rs(self) -> None:
        station_elements = read_station_observation_metadata(
            country='RS',
            source_url=str(SAMPLE_INVENTORY_PATH),
        )
        self.assertEqual(station_elements['station_id'].unique().tolist(), ['RI000000001', 'RI000000002'])
        self.assertTrue(station_elements['station_id'].str.startswith('RI').all())
        self.assertFalse(station_elements['station_id'].str.startswith('RS').any())

    def test_rs_download_uses_ri_station_ids_and_shared_normalization(self) -> None:
        station_metadata = read_station_metadata(country='RS', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='RS',
            provider='ghcnd',
            resolution='daily',
            station_ids=['RI000000001'],
            start_date='2021-04-01',
            end_date='2021-04-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_RI_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='RS', station_metadata=station_metadata)

        values = {(row.element, str(row.observation_date)): row.value for row in observations.itertuples()}
        self.assertEqual(values[('tas_mean', '2021-04-01')], 11.4)
        self.assertEqual(values[('tas_max', '2021-04-01')], 18.3)
        self.assertEqual(values[('tas_min', '2021-04-01')], 5.8)
        self.assertEqual(values[('precipitation', '2021-04-01')], 0.9)
        self.assertEqual(values[('snow_depth', '2021-04-01')], 0.0)

    def test_rs_fixture_parses_through_shared_ghcnd_parser(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_RI_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='RS',
            provider='ghcnd',
            resolution='daily',
            station_ids=['RI000000001'],
            start_date='2021-04-01',
            end_date='2021-04-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(sorted(normalized['station_id'].unique().tolist()), ['RI000000001'])
        self.assertFalse((normalized['station_id'].str.startswith('RS')).any())

    def test_tr_provider_is_explicit_ghcnd_daily_only(self) -> None:
        provider = get_provider('TR')
        self.assertEqual(provider.supported_country_codes, ('TR',))
        self.assertEqual(provider.supported_providers, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(
            provider.supported_canonical_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
        )

    def test_tr_discovery_exposes_conservative_shared_ghcnd_contract(self) -> None:
        self.assertEqual(list_providers(country='TR'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='TR', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='TR', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='TR', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )

    def test_tr_station_metadata_filters_tu_prefix_not_tr(self) -> None:
        stations = read_station_metadata(country='TR', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['TU000000001', 'TU000000002'])
        self.assertTrue(stations['station_id'].str.startswith('TU').all())
        self.assertFalse(stations['station_id'].str.startswith('TR').any())

    def test_tr_station_elements_follow_tu_inventory_not_tr(self) -> None:
        station_elements = read_station_observation_metadata(
            country='TR',
            source_url=str(SAMPLE_INVENTORY_PATH),
        )
        self.assertEqual(station_elements['station_id'].unique().tolist(), ['TU000000001', 'TU000000002'])
        self.assertTrue(station_elements['station_id'].str.startswith('TU').all())
        self.assertFalse(station_elements['station_id'].str.startswith('TR').any())

    def test_tr_download_uses_tu_station_ids_and_shared_normalization(self) -> None:
        station_metadata = read_station_metadata(country='TR', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='TR',
            provider='ghcnd',
            resolution='daily',
            station_ids=['TU000000001'],
            start_date='2021-03-01',
            end_date='2021-03-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_TU_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='TR', station_metadata=station_metadata)

        values = {(row.element, str(row.observation_date)): row.value for row in observations.itertuples()}
        self.assertEqual(values[('tas_mean', '2021-03-01')], 9.2)
        self.assertEqual(values[('tas_max', '2021-03-01')], 15.1)
        self.assertEqual(values[('tas_min', '2021-03-01')], 4.1)
        self.assertEqual(values[('precipitation', '2021-03-01')], 1.6)
        self.assertEqual(values[('snow_depth', '2021-03-01')], 0.0)

    def test_tr_fixture_parses_through_shared_ghcnd_parser(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_TU_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='TR',
            provider='ghcnd',
            resolution='daily',
            station_ids=['TU000000001'],
            start_date='2021-03-01',
            end_date='2021-03-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(sorted(normalized['station_id'].unique().tolist()), ['TU000000001'])
        self.assertFalse((normalized['station_id'].str.startswith('TR')).any())

    def test_ua_provider_is_explicit_ghcnd_daily_only(self) -> None:
        provider = get_provider('UA')
        self.assertEqual(provider.supported_country_codes, ('UA',))
        self.assertEqual(provider.supported_providers, ('ghcnd',))
        self.assertEqual(provider.supported_resolutions, ('daily',))
        self.assertEqual(
            provider.supported_canonical_elements,
            ('tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'),
        )

    def test_ua_discovery_exposes_conservative_shared_ghcnd_contract(self) -> None:
        self.assertEqual(list_providers(country='UA'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='UA', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='UA', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='UA', provider='ghcnd', resolution='daily', provider_raw=True),
            ['TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'],
        )

    def test_ua_station_metadata_filters_up_prefix_not_ua(self) -> None:
        stations = read_station_metadata(country='UA', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['UP000000001', 'UP000000002'])
        self.assertTrue(stations['station_id'].str.startswith('UP').all())
        self.assertFalse(stations['station_id'].str.startswith('UA').any())

    def test_ua_station_elements_follow_up_inventory_not_ua(self) -> None:
        station_elements = read_station_observation_metadata(
            country='UA',
            source_url=str(SAMPLE_INVENTORY_PATH),
        )
        self.assertEqual(station_elements['station_id'].unique().tolist(), ['UP000000001', 'UP000000002'])
        self.assertTrue(station_elements['station_id'].str.startswith('UP').all())
        self.assertFalse(station_elements['station_id'].str.startswith('UA').any())

    def test_ua_download_uses_up_station_ids_and_shared_normalization(self) -> None:
        station_metadata = read_station_metadata(country='UA', source_url=str(SAMPLE_STATIONS_PATH))
        query = ObservationQuery(
            country='UA',
            provider='ghcnd',
            resolution='daily',
            station_ids=['UP000000001'],
            start_date='2021-04-01',
            end_date='2021-04-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        with patch(
            'weatherdownload.providers.ghcnd.observations._read_text',
            return_value=SAMPLE_UP_DLY_PATH.read_text(encoding='utf-8'),
        ):
            observations = download_observations(query, country='UA', station_metadata=station_metadata)

        values = {(row.element, str(row.observation_date)): row.value for row in observations.itertuples()}
        self.assertEqual(values[('tas_mean', '2021-04-01')], 8.4)
        self.assertEqual(values[('tas_max', '2021-04-01')], 13.7)
        self.assertEqual(values[('tas_min', '2021-04-01')], 3.1)
        self.assertEqual(values[('precipitation', '2021-04-01')], 0.4)
        self.assertEqual(values[('snow_depth', '2021-04-01')], 95.0)

    def test_ua_fixture_parses_through_shared_ghcnd_parser(self) -> None:
        raw_table = parse_ghcnd_dly_text(SAMPLE_UP_DLY_PATH.read_text(encoding='utf-8'))
        query = ObservationQuery(
            country='UA',
            provider='ghcnd',
            resolution='daily',
            station_ids=['UP000000001'],
            start_date='2021-04-01',
            end_date='2021-04-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        normalized = normalize_daily_observations_ghcnd(raw_table, query=query)
        self.assertEqual(sorted(normalized['station_id'].unique().tolist()), ['UP000000001'])
        self.assertFalse((normalized['station_id'].str.startswith('UA')).any())


if __name__ == '__main__':
    unittest.main()
