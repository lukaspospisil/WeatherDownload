import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    get_provider,
    list_providers,
    list_providers,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    normalize_country_code,
    read_station_metadata,
    read_station_observation_metadata,
)

SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')
SAMPLE_GHCND_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_GHCND_STATIONS_TEXT = SAMPLE_GHCND_STATIONS_PATH.read_text(encoding='utf-8')
SAMPLE_GHCND_INVENTORY_TEXT = Path('tests/data/sample_ghcnd_inventory.txt').read_text(encoding='utf-8')
SAMPLE_DWD_STATIONS = '''Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe
----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------
00003 18910101 20241231 202 50.7827 6.0941 Aachen Baden-W\u00fcrttemberg Frei
00044 20070401 20241231 79 52.9336 8.2370 Alfhausen Niedersachsen Frei
'''.encode('latin-1')


class _MockResponse:
    def __init__(self, text: str | None = None, status_code: int = 200, content: bytes | None = None) -> None:
        self.text = text or ''
        self.status_code = status_code
        self.encoding = 'utf-8'
        self.content = content if content is not None else self.text.encode('utf-8')

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


def _mock_ghcnd_metadata_response(url: str, timeout: int = 60) -> _MockResponse:
    if url.endswith('ghcnd-stations.txt'):
        return _MockResponse(SAMPLE_GHCND_STATIONS_TEXT)
    if url.endswith('ghcnd-inventory.txt'):
        return _MockResponse(SAMPLE_GHCND_INVENTORY_TEXT)
    raise AssertionError(f'unexpected GHCND metadata URL: {url}')


class ProviderTests(unittest.TestCase):
    def test_supported_countries_and_normalization(self) -> None:
        self.assertEqual(list_supported_countries(), ['AT', 'BE', 'CA', 'CH', 'CZ', 'DE', 'DK', 'FI', 'FR', 'HU', 'IT', 'MX', 'NL', 'NO', 'NZ', 'PL', 'SE', 'SK', 'US'])
        self.assertEqual(normalize_country_code('de'), 'DE')
        self.assertEqual(normalize_country_code(None), 'CZ')

    def test_every_registered_country_has_discoverable_provider_paths(self) -> None:
        for country in list_supported_countries():
            with self.subTest(country=country):
                providers = list_providers(country=country)
                self.assertEqual(providers, list_providers(country=country))
                self.assertTrue(providers)
                weather_provider = get_provider(country=country)
                implemented_specs = weather_provider.list_implemented_dataset_specs()
                for provider_name in providers:
                    resolutions = list_resolutions(country=country, provider=provider_name)
                    self.assertTrue(resolutions)
                    for resolution in resolutions:
                        spec = next(
                            (item for item in implemented_specs if item.provider == provider_name and item.resolution == resolution),
                            None,
                        )
                        if spec is None:
                            continue
                        elements = list_supported_elements(country=country, provider=provider_name, resolution=resolution)
                        self.assertTrue(elements)

    def test_discovery_country_ca_includes_conservative_ghcnd_core_without_evap(self) -> None:
        self.assertEqual(list_providers(country='CA'), ['ghcnd'])
        self.assertEqual(list_providers(country='CA'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='CA', provider='ghcnd'), ['daily'])
        self.assertEqual(list_resolutions(country='CA', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='CA', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )

    def test_discovery_country_us_includes_conservative_ghcnd_core(self) -> None:
        self.assertEqual(list_providers(country='US'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='US', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='US', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth', 'open_water_evaporation'],
        )

    def test_discovery_country_mx_includes_conservative_ghcnd_core_without_evap(self) -> None:
        self.assertEqual(list_providers(country='MX'), ['ghcnd'])
        self.assertEqual(list_providers(country='MX'), ['ghcnd'])
        self.assertEqual(list_resolutions(country='MX', provider='ghcnd'), ['daily'])
        self.assertEqual(list_resolutions(country='MX', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='MX', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )

    def test_discovery_direct_prefix_ghcnd_countries_include_conservative_core_without_evap(self) -> None:
        for country in ['FI', 'FR', 'IT', 'NO', 'NZ']:
            with self.subTest(country=country):
                self.assertEqual(list_providers(country=country), ['ghcnd'])
                self.assertEqual(list_providers(country=country), ['ghcnd'])
                self.assertEqual(list_resolutions(country=country, provider='ghcnd'), ['daily'])
                self.assertEqual(list_resolutions(country=country, provider='ghcnd'), ['daily'])
                self.assertEqual(
                    list_supported_elements(country=country, provider='ghcnd', resolution='daily'),
                    ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
                )

    def test_discovery_country_cz_includes_chmi_and_ghcnd_daily_without_evap_on_ghcnd(self) -> None:
        self.assertIn('historical_csv', list_providers(country='CZ'))
        self.assertIn('ghcnd', list_providers(country='CZ'))
        self.assertEqual(list_resolutions(country='CZ', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='CZ', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertIn(
            'open_water_evaporation',
            list_supported_elements(country='CZ', provider='historical_csv', resolution='daily'),
        )

    def test_read_station_metadata_country_de(self) -> None:
        def _mock_de_and_ghcnd_response(url: str, timeout: int = 60) -> _MockResponse:
            if 'dwd.de' in url:
                return _MockResponse(content=SAMPLE_DWD_STATIONS)
            return _mock_ghcnd_metadata_response(url, timeout=timeout)

        with patch('weatherdownload.providers.de.metadata.requests.get', side_effect=_mock_de_and_ghcnd_response):
            stations = read_station_metadata(country='DE')
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations.iloc[0]['station_id'], '00003')
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertIn('W\u00fcrttemberg', stations.iloc[0]['full_name'])
        self.assertNotIn('\ufffd', stations.iloc[0]['full_name'])
        self.assertIn('GM000000001', stations['station_id'].tolist())

    def test_read_station_observation_metadata_country_de(self) -> None:
        def _mock_de_and_ghcnd_response(url: str, timeout: int = 60) -> _MockResponse:
            if 'dwd.de' in url:
                return _MockResponse(content=SAMPLE_DWD_STATIONS)
            return _mock_ghcnd_metadata_response(url, timeout=timeout)

        with patch('weatherdownload.providers.de.metadata.requests.get', side_effect=_mock_de_and_ghcnd_response):
            observation_metadata = read_station_observation_metadata(country='DE')
        self.assertEqual(list(observation_metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertIn('TMK', observation_metadata['element'].tolist())
        self.assertIn('TT_TU', observation_metadata['element'].tolist())
        self.assertIn('TT_10', observation_metadata['element'].tolist())
        self.assertIn('GM000000001', observation_metadata['station_id'].tolist())

    def test_discovery_country_de_returns_canonical_names_by_default(self) -> None:
        self.assertEqual(list_providers(country='DE'), ['ghcnd', 'historical'])
        self.assertEqual(
            list_supported_elements(country='DE', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(list_resolutions(country='DE', provider='historical'), ['10min', '1hour', 'daily'])
        hourly_elements = list_supported_elements(country='DE', provider='historical', resolution='1hour')
        self.assertEqual(hourly_elements, ['tas_mean', 'relative_humidity', 'wind_speed'])
        tenmin_elements = list_supported_elements(country='DE', provider='historical', resolution='10min')
        self.assertEqual(tenmin_elements, ['tas_mean', 'relative_humidity', 'wind_speed'])

    def test_discovery_country_de_can_return_raw_codes(self) -> None:
        hourly_elements = list_supported_elements(country='DE', provider='historical', resolution='1hour', provider_raw=True)
        self.assertEqual(hourly_elements, ['FF', 'RF_TU', 'TT_TU'])
        tenmin_elements = list_supported_elements(country='DE', provider='historical', resolution='10min', provider_raw=True)
        self.assertEqual(tenmin_elements, ['FF_10', 'RF_10', 'TT_10'])

    def test_discovery_country_at_includes_daily_and_hourly(self) -> None:
        self.assertEqual(list_providers(country='AT'), ['ghcnd', 'historical'])
        self.assertEqual(
            list_supported_elements(country='AT', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(list_resolutions(country='AT', provider='historical'), ['10min', '1hour', 'daily'])
        daily_elements = list_supported_elements(country='AT', provider='historical', resolution='daily')
        hourly_elements = list_supported_elements(country='AT', provider='historical', resolution='1hour')
        self.assertEqual(daily_elements, ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration', 'wind_speed', 'pressure', 'relative_humidity'])
        self.assertEqual(hourly_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])

    def test_discovery_country_be_includes_daily_hourly_and_tenmin(self) -> None:
        self.assertEqual(list_providers(country='BE'), ['historical'])
        self.assertEqual(list_resolutions(country='BE', provider='historical'), ['10min', '1hour', 'daily'])
        daily_elements = list_supported_elements(country='BE', provider='historical', resolution='daily')
        hourly_elements = list_supported_elements(country='BE', provider='historical', resolution='1hour')
        tenmin_elements = list_supported_elements(country='BE', provider='historical', resolution='10min')
        self.assertEqual(daily_elements, ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(hourly_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(tenmin_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])

    def test_discovery_country_ch_includes_daily_hourly_and_tenmin(self) -> None:
        self.assertEqual(list_providers(country='CH'), ['ghcnd', 'historical'])
        self.assertEqual(
            list_supported_elements(country='CH', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(list_resolutions(country='CH', provider='historical'), ['10min', '1hour', 'daily'])
        self.assertEqual(
            list_supported_elements(country='CH', provider='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='CH', provider='historical', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='CH', provider='historical', resolution='10min'),
            ['tas_mean', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure', 'sunshine_duration'],
        )
    def test_discovery_country_hu_includes_daily_hourly_tenmin_and_wind_scope(self) -> None:
        self.assertEqual(list_providers(country='HU'), ['historical', 'historical_wind'])
        self.assertEqual(list_resolutions(country='HU', provider='historical'), ['10min', '1hour', 'daily'])
        self.assertEqual(list_resolutions(country='HU', provider='historical_wind'), ['10min'])
        self.assertEqual(
            list_supported_elements(country='HU', provider='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', provider='historical', resolution='1hour'),
            ['precipitation', 'tas_mean', 'pressure', 'relative_humidity', 'wind_speed'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', provider='historical', resolution='10min'),
            ['precipitation', 'tas_mean', 'pressure', 'relative_humidity', 'wind_speed'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', provider='historical_wind', resolution='10min'),
            ['wind_speed', 'wind_speed_max'],
        )

    def test_discovery_country_nl_includes_daily_hourly_and_tenmin(self) -> None:
        self.assertEqual(list_providers(country='NL'), ['historical'])
        self.assertEqual(list_resolutions(country='NL', provider='historical'), ['10min', '1hour', 'daily'])
        self.assertEqual(
            list_supported_elements(country='NL', provider='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration', 'wind_speed', 'pressure', 'relative_humidity'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', provider='historical', resolution='daily', provider_raw=True),
            ['TG', 'TX', 'TN', 'RH', 'SQ', 'FG', 'PG', 'UG'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', provider='historical', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', provider='historical', resolution='1hour', provider_raw=True),
            ['T', 'RH', 'FH', 'U', 'P', 'SQ'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', provider='historical', resolution='10min'),
            ['tas_mean', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', provider='historical', resolution='10min', provider_raw=True),
            ['ta', 'ff', 'rh', 'pp', 'ss'],
        )

    def test_nl_subdaily_queries_are_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='NL', provider='historical', resolution='1hour', station_ids=['0-20000-0-06260'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='NL', provider='historical', resolution='10min', station_ids=['0-20000-0-06260'], start='2024-01-01T09:10:00Z', end='2024-01-01T09:20:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['T', 'P'])
        self.assertEqual(tenmin_query.elements, ['ta', 'pp'])

    def test_discovery_country_dk_includes_daily(self) -> None:
        self.assertEqual(list_providers(country='DK'), ['ghcnd', 'historical'])
        self.assertEqual(
            list_supported_elements(country='DK', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(list_resolutions(country='DK', provider='historical'), ['10min', '1hour', 'daily'])
        daily_elements = list_supported_elements(country='DK', provider='historical', resolution='daily')
        hourly_elements = list_supported_elements(country='DK', provider='historical', resolution='1hour')
        tenmin_elements = list_supported_elements(country='DK', provider='historical', resolution='10min')
        self.assertEqual(daily_elements, ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(hourly_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(tenmin_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])

    def test_de_subdaily_queries_are_now_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='DE', provider='historical', resolution='1hour', station_ids=['00003'], start='2024-01-01T00:00:00Z', end='2024-01-01T01:00:00Z', elements=['tas_mean', 'wind_speed'])
        tenmin_query = ObservationQuery(country='DE', provider='historical', resolution='10min', station_ids=['00003'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:10:00Z', elements=['tas_mean', 'relative_humidity'])
        self.assertEqual(hourly_query.elements, ['TT_TU', 'FF'])
        self.assertEqual(tenmin_query.elements, ['TT_10', 'RF_10'])

    def test_at_hourly_query_is_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='AT', provider='historical', resolution='1hour', station_ids=['1'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['tl', 'p'])

    def test_be_subdaily_queries_are_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='BE', provider='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='BE', provider='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(tenmin_query.elements, ['temp_dry_shelter_avg', 'pressure'])

    def test_dk_subdaily_queries_are_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='DK', provider='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='DK', provider='historical', resolution='10min', station_ids=['06180'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['mean_temp', 'mean_pressure'])
        self.assertEqual(tenmin_query.elements, ['temp_dry', 'pressure'])

    def test_ch_subdaily_queries_are_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='CH', provider='historical', resolution='1hour', station_ids=['AIG'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='CH', provider='historical', resolution='10min', station_ids=['AIG'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['tre200h0', 'prestah0'])
        self.assertEqual(tenmin_query.elements, ['tre200s0', 'prestas0'])
    def test_hu_subdaily_queries_are_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='HU', provider='historical', resolution='1hour', station_ids=['13704'], start='2026-01-01T00:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='HU', provider='historical', resolution='10min', station_ids=['13704'], start='2026-01-01T00:00:00Z', end='2026-01-01T00:10:00Z', elements=['tas_mean', 'pressure'])
        wind_query = ObservationQuery(country='HU', provider='historical_wind', resolution='10min', station_ids=['26327'], start='2026-01-01T00:00:00Z', end='2026-01-01T00:10:00Z', elements=['wind_speed', 'wind_speed_max'])
        self.assertEqual(hourly_query.elements, ['ta', 'p'])
        self.assertEqual(tenmin_query.elements, ['ta', 'p'])
        self.assertEqual(wind_query.elements, ['fs', 'fx'])

    def test_discovery_country_pl_includes_daily_and_hourly(self) -> None:
        self.assertEqual(list_providers(country='PL'), ['historical', 'historical_klimat'])
        self.assertEqual(list_resolutions(country='PL', provider='historical'), ['1hour', 'daily'])
        self.assertEqual(list_resolutions(country='PL', provider='historical_klimat'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='PL', provider='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', provider='historical', resolution='1hour'),
            ['tas_mean', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', provider='historical', resolution='1hour', provider_raw=True),
            ['TEMP', 'FWR', 'PORW', 'WLGW', 'CPW', 'PPPS'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', provider='historical_klimat', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', provider='historical_klimat', resolution='daily', provider_raw=True),
            ['STD', 'TMAX', 'TMIN', 'SMDB'],
        )

    def test_pl_daily_and_hourly_queries_are_provider_valid(self) -> None:
        daily_query = ObservationQuery(country='PL', provider='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=['tas_mean', 'precipitation'])
        hourly_query = ObservationQuery(country='PL', provider='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['tas_mean', 'pressure'])
        klimat_query = ObservationQuery(country='PL', provider='historical_klimat', resolution='daily', station_ids=['00375'], start_date='2026-01-01', end_date='2026-01-02', elements=['tas_mean', 'precipitation'])
        self.assertEqual(daily_query.elements, ['STD', 'SMDB'])
        self.assertEqual(hourly_query.elements, ['TEMP', 'PPPS'])
        self.assertEqual(klimat_query.elements, ['STD', 'SMDB'])
    def test_discovery_country_se_includes_daily_and_hourly(self) -> None:
        self.assertEqual(list_providers(country='SE'), ['ghcnd', 'historical'])
        self.assertEqual(
            list_supported_elements(country='SE', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertEqual(list_resolutions(country='SE', provider='historical'), ['1hour', 'daily'])
        self.assertEqual(
            list_supported_elements(country='SE', provider='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='SE', provider='historical', resolution='daily', provider_raw=True),
            ['2', '20', '19', '5'],
        )
        self.assertEqual(
            list_supported_elements(country='SE', provider='historical', resolution='1hour'),
            ['tas_mean', 'wind_speed', 'relative_humidity', 'precipitation', 'pressure'],
        )
        self.assertEqual(
            list_supported_elements(country='SE', provider='historical', resolution='1hour', provider_raw=True),
            ['1', '4', '6', '7', '9'],
        )

    def test_se_hourly_query_is_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='SE', provider='historical', resolution='1hour', station_ids=['98230'], start='2012-11-29T11:00:00Z', end='2012-11-29T13:00:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['1', '9'])

    def test_read_station_metadata_preserves_positional_source_url_compatibility(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)) as mock_get:
            stations = read_station_metadata('https://example.test/meta1.csv')
        mock_get.assert_called_once_with('https://example.test/meta1.csv', timeout=60)
        self.assertEqual(stations.iloc[0]['station_id'], '0-20000-0-11406')


if __name__ == '__main__':
    unittest.main()











