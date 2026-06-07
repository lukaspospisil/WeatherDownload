import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.lv.observations import build_lvgmc_daily_sql, build_lvgmc_hourly_sql
from weatherdownload.providers.lv.parser import (
    hourly_period_date,
    hourly_period_timestamp,
    normalize_lvgmc_parameter_metadata,
    parse_lvgmc_payload_json,
)


FIXTURE_DIR = Path('tests/data/lv_lvgmc')
ARCHIVE_ONE_DAY_TEXT = (FIXTURE_DIR / 'archive_hourly_one_station_one_day.json').read_text(encoding='utf-8')
ARCHIVE_NULLS_TEXT = (FIXTURE_DIR / 'archive_hourly_with_nulls.json').read_text(encoding='utf-8')
EMPTY_RESPONSE_TEXT = (FIXTURE_DIR / 'empty_response.json').read_text(encoding='utf-8')
PARAMETERS_TEXT = (FIXTURE_DIR / 'parameters.json').read_text(encoding='utf-8')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


def _lvgmc_observation_response(url: str, params=None, timeout: int = 60) -> _MockTextResponse:
    del timeout
    sql = (params or {}).get('sql', '')
    if '"ecc62e27-2071-483c-bca9-5e53d979faa8"' not in sql:
        raise AssertionError(f'unexpected LVGMC SQL query: {sql}')
    if "'0001'" not in sql:
        raise AssertionError(f'unexpected LVGMC station filter: {sql}')
    if "DATETIME > '2026-01-01T00:00:00Z'" in sql and "DATETIME <= '2026-01-02T00:00:00Z'" in sql:
        return _MockTextResponse(ARCHIVE_ONE_DAY_TEXT)
    if "DATETIME > '2026-01-03T00:00:00Z'" in sql and "DATETIME <= '2026-01-04T00:00:00Z'" in sql:
        return _MockTextResponse(EMPTY_RESPONSE_TEXT)
    raise AssertionError(f'unexpected LVGMC SQL bounds: {sql}')


class LatviaLvgmcProviderTests(unittest.TestCase):
    def test_lv_discovery_exposes_lvgmc_hourly_daily_and_ghcnd_daily(self) -> None:
        self.assertEqual(list_providers(country='LV'), ['ghcnd', 'lvgmc'])
        self.assertEqual(list_resolutions(country='LV', provider='lvgmc'), ['1hour', 'daily'])
        self.assertEqual(list_resolutions(country='LV', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='LV', provider='lvgmc', resolution='1hour'),
            ['tas_mean', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'pressure', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='LV', provider='lvgmc', resolution='1hour', provider_raw=True),
            ['HTDRY', 'HPRAB', 'HWNDS', 'HWSMX', 'HRLH', 'HPRSL', 'HSNOW'],
        )
        self.assertEqual(
            list_supported_elements(country='LV', provider='lvgmc', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'pressure', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='LV', provider='lvgmc', resolution='daily', provider_raw=True),
            ['HTDRY', 'HATMX', 'HATMN', 'HPRAB', 'HWNDS', 'HWSMX', 'HRLH', 'HPRSL', 'HSNOW'],
        )
        self.assertEqual(
            list_supported_elements(country='LV', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )

    def test_read_station_metadata_country_lv_lvgmc_from_fixture(self) -> None:
        stations = read_station_metadata(country='LV', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['0001', '0002'])
        self.assertEqual(stations['full_name'].tolist(), ['Riga Center', 'Liepaja'])
        self.assertEqual(stations['longitude'].tolist(), [24.1052, 21.0108])
        self.assertEqual(stations['latitude'].tolist(), [56.9496, 56.5047])
        self.assertEqual(stations['elevation_m'].tolist(), [7.0, 4.0])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('lvgmc', 'daily')]['0001'],
            ['HTDRY', 'HATMX', 'HATMN', 'HPRAB', 'HWNDS', 'HWSMX', 'HRLH', 'HPRSL', 'HSNOW'],
        )
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('lvgmc', '1hour')]['0001'],
            ['HTDRY', 'HPRAB', 'HWNDS', 'HWSMX', 'HRLH', 'HPRSL', 'HSNOW'],
        )

    def test_parameter_metadata_parser_keeps_expected_fields(self) -> None:
        payload = parse_lvgmc_payload_json(PARAMETERS_TEXT)
        parameter_metadata = normalize_lvgmc_parameter_metadata(payload['result']['records'])
        self.assertEqual(parameter_metadata['HTDRY']['abbreviation'], 'HTDRY')
        self.assertEqual(parameter_metadata['HTDRY']['description_en'], 'Hourly average air temperature')
        self.assertEqual(parameter_metadata['HTDRY']['unit'], 'degC')
        self.assertEqual(parameter_metadata['HTDRY']['scale'], 1.0)
        self.assertEqual(parameter_metadata['HTDRY']['lower_limit'], -50.0)
        self.assertEqual(parameter_metadata['HTDRY']['upper_limit'], 50.0)

    def test_read_station_observation_metadata_country_lv_lvgmc_from_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='LV', source_url=str(FIXTURE_DIR))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['station_id'].unique().tolist()), ['0001', '0002'])
        self.assertEqual(
            sorted(metadata['element'].unique().tolist()),
            ['HATMN', 'HATMX', 'HPRAB', 'HPRSL', 'HRLH', 'HSNOW', 'HTDRY', 'HWNDS', 'HWSMX'],
        )
        self.assertEqual(sorted(metadata['obs_type'].unique().tolist()), ['HISTORICAL_1HOUR', 'HISTORICAL_DAILY'])
        hourly_schedule = metadata[metadata['obs_type'] == 'HISTORICAL_1HOUR']['schedule'].iloc[0]
        self.assertIn('preceding hour', hourly_schedule)

    def test_hourly_period_date_uses_previous_hour_bucket(self) -> None:
        self.assertEqual(hourly_period_date(__import__('pandas').Timestamp('2026-01-02T00:00:00Z')), __import__('datetime').date(2026, 1, 1))
        self.assertEqual(hourly_period_date(__import__('pandas').Timestamp('2026-01-01T00:00:00Z')), __import__('datetime').date(2025, 12, 31))
        self.assertEqual(hourly_period_timestamp(__import__('pandas').Timestamp('2026-01-01T02:00:00Z')).isoformat(), '2026-01-01T01:00:00+00:00')

    def test_build_lvgmc_daily_sql_limits_station_abbreviations_and_interval(self) -> None:
        sql = build_lvgmc_daily_sql(
            resource_id='ecc62e27-2071-483c-bca9-5e53d979faa8',
            station_id='0001',
            raw_elements=['HTDRY', 'HPRAB'],
            observation_date=__import__('datetime').date(2026, 1, 1),
        )
        self.assertIn('FROM "ecc62e27-2071-483c-bca9-5e53d979faa8"', sql)
        self.assertIn("STATION_ID = '0001'", sql)
        self.assertIn("ABBREVIATION IN ('HTDRY', 'HPRAB')", sql)
        self.assertIn("DATETIME > '2026-01-01T00:00:00Z'", sql)
        self.assertIn("DATETIME <= '2026-01-02T00:00:00Z'", sql)

    def test_build_lvgmc_hourly_sql_limits_station_abbreviations_and_represented_interval(self) -> None:
        sql = build_lvgmc_hourly_sql(
            resource_id='ecc62e27-2071-483c-bca9-5e53d979faa8',
            station_id='0001',
            raw_elements=['HTDRY', 'HPRAB'],
            start_timestamp='2026-01-01T00:00:00Z',
            end_timestamp='2026-01-01T02:00:00Z',
        )
        self.assertIn('FROM "ecc62e27-2071-483c-bca9-5e53d979faa8"', sql)
        self.assertIn("STATION_ID = '0001'", sql)
        self.assertIn("ABBREVIATION IN ('HTDRY', 'HPRAB')", sql)
        self.assertIn("DATETIME > '2026-01-01T00:00:00Z'", sql)
        self.assertIn("DATETIME <= '2026-01-01T03:00:00Z'", sql)

    def test_download_daily_observations_lv_lvgmc_aggregates_fixture_payload(self) -> None:
        station_metadata = read_station_metadata(country='LV', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='LV',
            provider='lvgmc',
            resolution='daily',
            station_ids=['0001'],
            start_date='2026-01-01',
            end_date='2026-01-01',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'pressure', 'snow_depth'],
        )
        with patch('weatherdownload.providers.lv.observations.requests.get', side_effect=_lvgmc_observation_response):
            observations = download_observations(query, country='LV', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['precipitation', 'pressure', 'relative_humidity', 'snow_depth', 'tas_max', 'tas_mean', 'tas_min', 'wind_speed', 'wind_speed_max'],
        )
        lookup = {(row.element, row.observation_date.isoformat()): round(float(row.value), 4) for row in observations.itertuples(index=False)}
        self.assertEqual(lookup[('tas_mean', '2026-01-01')], 3.0)
        self.assertEqual(lookup[('tas_max', '2026-01-01')], 6.0)
        self.assertEqual(lookup[('tas_min', '2026-01-01')], -1.0)
        self.assertEqual(lookup[('precipitation', '2026-01-01')], 0.5)
        self.assertEqual(lookup[('wind_speed', '2026-01-01')], 4.0)
        self.assertEqual(lookup[('wind_speed_max', '2026-01-01')], 9.0)
        self.assertEqual(lookup[('relative_humidity', '2026-01-01')], 70.0)
        self.assertEqual(lookup[('pressure', '2026-01-01')], 1002.0)
        self.assertEqual(lookup[('snow_depth', '2026-01-01')], 12.0)

    def test_download_hourly_observations_lv_lvgmc_exposes_recent_archive_slice(self) -> None:
        station_metadata = read_station_metadata(country='LV', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='LV',
            provider='lvgmc',
            resolution='1hour',
            station_ids=['0001'],
            start='2026-01-01T00:00:00Z',
            end='2026-01-01T02:00:00Z',
            elements=['tas_mean', 'precipitation', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'pressure', 'snow_depth'],
        )
        with patch('weatherdownload.providers.lv.observations.requests.get', return_value=_MockTextResponse(ARCHIVE_ONE_DAY_TEXT)):
            observations = download_observations(query, country='LV', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['precipitation', 'pressure', 'relative_humidity', 'snow_depth', 'tas_mean', 'wind_speed', 'wind_speed_max'],
        )
        self.assertEqual(observations['timestamp'].astype(str).min(), '2026-01-01 00:00:00+00:00')
        lookup = {
            (row.element, row.timestamp.isoformat()): round(float(row.value), 4)
            for row in observations.itertuples(index=False)
        }
        self.assertEqual(lookup[('tas_mean', '2026-01-01T00:00:00+00:00')], 1.0)
        self.assertEqual(lookup[('tas_mean', '2026-01-01T01:00:00+00:00')], 3.0)
        self.assertEqual(lookup[('precipitation', '2026-01-01T00:00:00+00:00')], 0.2)
        self.assertEqual(lookup[('wind_speed_max', '2026-01-01T01:00:00+00:00')], 7.0)
        self.assertEqual(lookup[('snow_depth', '2026-01-01T00:00:00+00:00')], 10.0)

    def test_download_daily_observations_lv_lvgmc_ignores_nulls_and_drops_all_null_elements(self) -> None:
        station_metadata = read_station_metadata(country='LV', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='LV',
            provider='lvgmc',
            resolution='daily',
            station_ids=['0001'],
            start_date='2026-01-01',
            end_date='2026-01-01',
            elements=['tas_mean', 'precipitation', 'pressure', 'snow_depth'],
        )
        with patch('weatherdownload.providers.lv.observations.requests.get', return_value=_MockTextResponse(ARCHIVE_NULLS_TEXT)):
            observations = download_observations(query, country='LV', station_metadata=station_metadata)
        lookup = {(row.element, row.observation_date.isoformat()): round(float(row.value), 4) for row in observations.itertuples(index=False)}
        self.assertEqual(lookup[('tas_mean', '2026-01-01')], 2.0)
        self.assertEqual(lookup[('precipitation', '2026-01-01')], 0.5)
        self.assertEqual(lookup[('snow_depth', '2026-01-01')], 11.0)
        self.assertNotIn(('pressure', '2026-01-01'), lookup)

    def test_download_daily_observations_lv_lvgmc_empty_result_is_no_observations(self) -> None:
        station_metadata = read_station_metadata(country='LV', source_url=str(FIXTURE_DIR))
        query = ObservationQuery(
            country='LV',
            provider='lvgmc',
            resolution='daily',
            station_ids=['0001'],
            start_date='2026-01-03',
            end_date='2026-01-03',
            elements=['tas_mean'],
        )
        with patch('weatherdownload.providers.lv.observations.requests.get', side_effect=_lvgmc_observation_response):
            with self.assertRaisesRegex(Exception, 'No observations found'):
                download_observations(query, country='LV', station_metadata=station_metadata)


if __name__ == '__main__':
    unittest.main()
