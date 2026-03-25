import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    list_dataset_scopes,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    normalize_country_code,
    read_station_metadata,
    read_station_observation_metadata,
)

SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')
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


class ProviderTests(unittest.TestCase):
    def test_supported_countries_and_normalization(self) -> None:
        self.assertEqual(list_supported_countries(), ['AT', 'BE', 'CZ', 'DE', 'DK', 'NL', 'SE', 'SK'])
        self.assertEqual(normalize_country_code('de'), 'DE')
        self.assertEqual(normalize_country_code(None), 'CZ')

    def test_read_station_metadata_country_de(self) -> None:
        with patch('weatherdownload.dwd_metadata.requests.get', return_value=_MockResponse(content=SAMPLE_DWD_STATIONS)):
            stations = read_station_metadata(country='DE')
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations.iloc[0]['station_id'], '00003')
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertIn('W\u00fcrttemberg', stations.iloc[0]['full_name'])
        self.assertNotIn('\ufffd', stations.iloc[0]['full_name'])

    def test_read_station_observation_metadata_country_de(self) -> None:
        with patch('weatherdownload.dwd_metadata.requests.get', return_value=_MockResponse(content=SAMPLE_DWD_STATIONS)):
            observation_metadata = read_station_observation_metadata(country='DE')
        self.assertEqual(list(observation_metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertIn('TMK', observation_metadata['element'].tolist())
        self.assertIn('TT_TU', observation_metadata['element'].tolist())
        self.assertIn('TT_10', observation_metadata['element'].tolist())
        self.assertTrue(observation_metadata['station_id'].str.len().eq(5).all())

    def test_discovery_country_de_returns_canonical_names_by_default(self) -> None:
        self.assertEqual(list_dataset_scopes(country='DE'), ['historical'])
        self.assertEqual(list_resolutions(country='DE', dataset_scope='historical'), ['10min', '1hour', 'daily'])
        hourly_elements = list_supported_elements(country='DE', dataset_scope='historical', resolution='1hour')
        self.assertEqual(hourly_elements, ['tas_mean', 'relative_humidity', 'wind_speed'])
        tenmin_elements = list_supported_elements(country='DE', dataset_scope='historical', resolution='10min')
        self.assertEqual(tenmin_elements, ['tas_mean', 'relative_humidity', 'wind_speed'])

    def test_discovery_country_de_can_return_raw_codes(self) -> None:
        hourly_elements = list_supported_elements(country='DE', dataset_scope='historical', resolution='1hour', provider_raw=True)
        self.assertEqual(hourly_elements, ['FF', 'RF_TU', 'TT_TU'])
        tenmin_elements = list_supported_elements(country='DE', dataset_scope='historical', resolution='10min', provider_raw=True)
        self.assertEqual(tenmin_elements, ['FF_10', 'RF_10', 'TT_10'])

    def test_discovery_country_at_includes_daily_and_hourly(self) -> None:
        self.assertEqual(list_dataset_scopes(country='AT'), ['historical'])
        self.assertEqual(list_resolutions(country='AT', dataset_scope='historical'), ['10min', '1hour', 'daily'])
        daily_elements = list_supported_elements(country='AT', dataset_scope='historical', resolution='daily')
        hourly_elements = list_supported_elements(country='AT', dataset_scope='historical', resolution='1hour')
        self.assertEqual(daily_elements, ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration', 'wind_speed', 'pressure', 'relative_humidity'])
        self.assertEqual(hourly_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])

    def test_discovery_country_be_includes_daily_hourly_and_tenmin(self) -> None:
        self.assertEqual(list_dataset_scopes(country='BE'), ['historical'])
        self.assertEqual(list_resolutions(country='BE', dataset_scope='historical'), ['10min', '1hour', 'daily'])
        daily_elements = list_supported_elements(country='BE', dataset_scope='historical', resolution='daily')
        hourly_elements = list_supported_elements(country='BE', dataset_scope='historical', resolution='1hour')
        tenmin_elements = list_supported_elements(country='BE', dataset_scope='historical', resolution='10min')
        self.assertEqual(daily_elements, ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(hourly_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(tenmin_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])

    def test_discovery_country_dk_includes_daily(self) -> None:
        self.assertEqual(list_dataset_scopes(country='DK'), ['historical'])
        self.assertEqual(list_resolutions(country='DK', dataset_scope='historical'), ['10min', '1hour', 'daily'])
        daily_elements = list_supported_elements(country='DK', dataset_scope='historical', resolution='daily')
        hourly_elements = list_supported_elements(country='DK', dataset_scope='historical', resolution='1hour')
        tenmin_elements = list_supported_elements(country='DK', dataset_scope='historical', resolution='10min')
        self.assertEqual(daily_elements, ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(hourly_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])
        self.assertEqual(tenmin_elements, ['tas_mean', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration'])

    def test_de_subdaily_queries_are_now_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='DE', dataset_scope='historical', resolution='1hour', station_ids=['00003'], start='2024-01-01T00:00:00Z', end='2024-01-01T01:00:00Z', elements=['tas_mean', 'wind_speed'])
        tenmin_query = ObservationQuery(country='DE', dataset_scope='historical', resolution='10min', station_ids=['00003'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:10:00Z', elements=['tas_mean', 'relative_humidity'])
        self.assertEqual(hourly_query.elements, ['TT_TU', 'FF'])
        self.assertEqual(tenmin_query.elements, ['TT_10', 'RF_10'])

    def test_at_hourly_query_is_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='AT', dataset_scope='historical', resolution='1hour', station_ids=['1'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['tl', 'p'])

    def test_be_subdaily_queries_are_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(tenmin_query.elements, ['temp_dry_shelter_avg', 'pressure'])

    def test_dk_subdaily_queries_are_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        tenmin_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='10min', station_ids=['06180'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['mean_temp', 'mean_pressure'])
        self.assertEqual(tenmin_query.elements, ['temp_dry', 'pressure'])

    def test_discovery_country_se_includes_daily_and_hourly(self) -> None:
        self.assertEqual(list_dataset_scopes(country='SE'), ['historical'])
        self.assertEqual(list_resolutions(country='SE', dataset_scope='historical'), ['1hour', 'daily'])
        self.assertEqual(
            list_supported_elements(country='SE', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='SE', dataset_scope='historical', resolution='daily', provider_raw=True),
            ['2', '20', '19', '5'],
        )
        self.assertEqual(
            list_supported_elements(country='SE', dataset_scope='historical', resolution='1hour'),
            ['tas_mean', 'wind_speed', 'relative_humidity', 'precipitation', 'pressure'],
        )
        self.assertEqual(
            list_supported_elements(country='SE', dataset_scope='historical', resolution='1hour', provider_raw=True),
            ['1', '4', '6', '7', '9'],
        )

    def test_se_hourly_query_is_provider_valid(self) -> None:
        hourly_query = ObservationQuery(country='SE', dataset_scope='historical', resolution='1hour', station_ids=['98230'], start='2012-11-29T11:00:00Z', end='2012-11-29T13:00:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(hourly_query.elements, ['1', '9'])

    def test_read_station_metadata_preserves_positional_source_url_compatibility(self) -> None:
        with patch('weatherdownload.metadata.requests.get', return_value=_MockResponse(SAMPLE_META1)) as mock_get:
            stations = read_station_metadata('https://example.test/meta1.csv')
        mock_get.assert_called_once_with('https://example.test/meta1.csv', timeout=60)
        self.assertEqual(stations.iloc[0]['station_id'], '0-20000-0-11406')


if __name__ == '__main__':
    unittest.main()

