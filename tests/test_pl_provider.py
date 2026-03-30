import io
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    QueryValidationError,
    download_observations,
    list_dataset_scopes,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.pl.daily import (
    PL_DAILY_KLIMAT_BASE_URL,
    PL_DAILY_SYNOP_BASE_URL,
    build_pl_daily_download_targets,
)
from weatherdownload.providers.pl.hourly import PL_HOURLY_SYNOP_BASE_URL, build_pl_hourly_download_targets
from weatherdownload.providers.pl.metadata import read_station_metadata_pl
from weatherdownload.providers.pl.parser import (
    PL_NORMALIZED_DAILY_COLUMNS,
    PL_NORMALIZED_SUBDAILY_COLUMNS,
    parse_pl_daily_klimat_csv,
    parse_pl_daily_synop_csv,
    parse_pl_hourly_synop_csv,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_pl_wykaz_stacji.csv')
SAMPLE_METEO_COORDINATES_PATH = Path('tests/data/sample_pl_meteo_api.json')
SAMPLE_STATION_2025_PATH = Path('tests/data/sample_pl_synop_station_2025.csv')
SAMPLE_MONTH_2026_01_PATH = Path('tests/data/sample_pl_synop_month_2026_01.csv')
SAMPLE_KLIMAT_MONTH_2026_01_PATH = Path('tests/data/sample_pl_klimat_month_2026_01.csv')
SAMPLE_HOURLY_STATION_2025_PATH = Path('tests/data/sample_pl_synop_hourly_station_2025.csv')
EXPECTED_PL_DAILY_MAPPING = {
    'tas_mean': 'STD',
    'tas_max': 'TMAX',
    'tas_min': 'TMIN',
    'precipitation': 'SMDB',
    'sunshine_duration': 'USL',
}
EXPECTED_PL_KLIMAT_MAPPING = {
    'tas_mean': 'STD',
    'tas_max': 'TMAX',
    'tas_min': 'TMIN',
    'precipitation': 'SMDB',
}
EXPECTED_PL_HOURLY_MAPPING = {
    'tas_mean': 'TEMP',
    'wind_speed': 'FWR',
    'wind_speed_max': 'PORW',
    'relative_humidity': 'WLGW',
    'vapour_pressure': 'CPW',
    'pressure': 'PPPS',
}


class _MockResponse:
    def __init__(self, text: str | None = None, status_code: int = 200, content: bytes | None = None) -> None:
        self.text = text or ''
        self.status_code = status_code
        self.encoding = 'utf-8'
        self.content = content if content is not None else self.text.encode('utf-8')

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')



def _build_zip_bytes(filename: str, csv_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, csv_text.replace(chr(65279), '').encode('cp1250'))
    return buffer.getvalue()


class PolandProviderTests(unittest.TestCase):
    def test_supported_countries_include_pl(self) -> None:
        self.assertIn('PL', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='PL'), ['historical', 'historical_klimat'])
        self.assertEqual(list_resolutions(country='PL', dataset_scope='historical'), ['1hour', 'daily'])
        self.assertEqual(list_resolutions(country='PL', dataset_scope='historical_klimat'), ['daily'])

    def test_read_station_metadata_country_pl_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['00375', '00400', '00600'])
        self.assertEqual(stations.iloc[0]['gh_id'], '352200375')
        self.assertEqual(stations.iloc[0]['full_name'], 'WARSZAWA')
        self.assertTrue(stations['longitude'].isna().all())

    def test_read_station_metadata_country_pl_enriches_official_coordinates_on_exact_gh_id_match(self) -> None:
        stations = read_station_metadata_pl(
            source_url=str(SAMPLE_STATIONS_PATH),
            coordinates_source_url=str(SAMPLE_METEO_COORDINATES_PATH),
        )
        lookup = stations.set_index('station_id')
        self.assertAlmostEqual(float(lookup.loc['00375', 'longitude']), 20.961111)
        self.assertAlmostEqual(float(lookup.loc['00375', 'latitude']), 52.162778)
        self.assertAlmostEqual(float(lookup.loc['00600', 'longitude']), 19.002222)
        self.assertAlmostEqual(float(lookup.loc['00600', 'latitude']), 49.806667)
        self.assertTrue(pd.isna(lookup.loc['00400', 'longitude']))
        self.assertTrue(pd.isna(lookup.loc['00400', 'latitude']))
        self.assertTrue(stations['elevation_m'].isna().all())

    def test_read_station_observation_metadata_country_pl_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(set(metadata['obs_type']), {'HISTORICAL_DAILY', 'HISTORICAL_HOURLY'})
        self.assertEqual(sorted(metadata['element'].unique().tolist()), ['CPW', 'FWR', 'PORW', 'PPPS', 'SMDB', 'STD', 'TEMP', 'TMAX', 'TMIN', 'USL', 'WLGW'])
        self.assertTrue(metadata['height'].isna().all())

    def test_discovery_country_pl_returns_scope_specific_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='PL', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', dataset_scope='historical', resolution='1hour'),
            ['tas_mean', 'wind_speed', 'wind_speed_max', 'relative_humidity', 'vapour_pressure', 'pressure'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', dataset_scope='historical', resolution='1hour', provider_raw=True),
            ['TEMP', 'FWR', 'PORW', 'WLGW', 'CPW', 'PPPS'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', dataset_scope='historical_klimat', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', dataset_scope='historical_klimat', resolution='daily', provider_raw=True),
            ['STD', 'TMAX', 'TMIN', 'SMDB'],
        )

    def test_pl_queries_accept_scope_specific_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=['STD', 'SMDB'])
        klimat_query = ObservationQuery(country='PL', dataset_scope='historical_klimat', resolution='daily', station_ids=['00375'], start_date='2026-01-01', end_date='2026-01-02', elements=['tas_mean', 'precipitation'])
        hourly_query = ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['tas_mean', 'pressure'])
        hourly_raw_query = ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['TEMP', 'PPPS'])
        self.assertEqual(canonical_query.elements, ['STD', 'SMDB'])
        self.assertEqual(raw_query.elements, ['STD', 'SMDB'])
        self.assertEqual(klimat_query.elements, ['STD', 'SMDB'])
        self.assertEqual(hourly_query.elements, ['TEMP', 'PPPS'])
        self.assertEqual(hourly_raw_query.elements, ['TEMP', 'PPPS'])

    def test_pl_query_rejects_unsupported_resolution_and_scope_specific_element(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-01', elements=['tas_mean'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=['wind_speed'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='PL', dataset_scope='historical_klimat', resolution='daily', station_ids=['00375'], start_date='2026-01-01', end_date='2026-01-02', elements=['sunshine_duration'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='PL', dataset_scope='historical_klimat', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['tas_mean'])

    def test_parse_pl_daily_synop_csv_keeps_source_columns(self) -> None:
        parsed = parse_pl_daily_synop_csv(SAMPLE_STATION_2025_PATH.read_text(encoding='utf-8'))
        self.assertIn('NSP', parsed.columns)
        self.assertIn('ROK', parsed.columns)
        self.assertIn('STD', parsed.columns)
        self.assertIn('WSMDB', parsed.columns)
        self.assertEqual(len(parsed), 2)

    def test_parse_pl_daily_klimat_csv_keeps_source_columns(self) -> None:
        parsed = parse_pl_daily_klimat_csv(SAMPLE_KLIMAT_MONTH_2026_01_PATH.read_text(encoding='utf-8'))
        self.assertEqual(parsed.columns.tolist(), ['NSP', 'POST', 'ROK', 'MC', 'DZ', 'TMAX', 'WTMAX', 'TMIN', 'WTMIN', 'STD', 'WSTD', 'TMNG', 'WTMNG', 'SMDB', 'WSMDB', 'ROOP', 'PKSN', 'WPKSN'])
        self.assertEqual(len(parsed), 3)
        self.assertEqual(parsed.iloc[0]['POST'], 'WARSZAWA')

    def test_parse_pl_hourly_synop_csv_keeps_source_columns(self) -> None:
        parsed = parse_pl_hourly_synop_csv(SAMPLE_HOURLY_STATION_2025_PATH.read_text(encoding='utf-8'))
        self.assertIn('GG', parsed.columns)
        self.assertIn('TEMP', parsed.columns)
        self.assertIn('WLGW', parsed.columns)
        self.assertIn('PPPS', parsed.columns)
        self.assertEqual(len(parsed), 2)

    def test_build_targets_country_pl_handles_daily_and_hourly_archive_shapes(self) -> None:
        query_recent = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2026-01-02', elements=['tas_mean'])
        recent_targets = build_pl_daily_download_targets(query_recent)
        self.assertEqual([target.archive_url for target in recent_targets], [
            f'{PL_DAILY_SYNOP_BASE_URL}/2025/2025_375_s.zip',
            f'{PL_DAILY_SYNOP_BASE_URL}/2026/2026_01_s.zip',
        ])

        hourly_recent = ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['tas_mean'])
        hourly_recent_targets = build_pl_hourly_download_targets(hourly_recent)
        self.assertEqual([target.archive_url for target in hourly_recent_targets], [
            f'{PL_HOURLY_SYNOP_BASE_URL}/2025/2025_375_s.zip',
        ])

        query_legacy = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='1999-01-01', end_date='1999-01-02', elements=['tas_mean'])
        legacy_targets = build_pl_daily_download_targets(query_legacy)
        self.assertEqual([target.archive_url for target in legacy_targets], [
            f'{PL_DAILY_SYNOP_BASE_URL}/1996_2000/1996_2000_375_s.zip',
        ])

        hourly_legacy = ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='1999-01-01T00:00:00Z', end='1999-01-01T01:00:00Z', elements=['tas_mean'])
        hourly_legacy_targets = build_pl_hourly_download_targets(hourly_legacy)
        self.assertEqual([target.archive_url for target in hourly_legacy_targets], [
            f'{PL_HOURLY_SYNOP_BASE_URL}/1996_2000/1996_2000_375_s.zip',
        ])

        klimat_recent = ObservationQuery(country='PL', dataset_scope='historical_klimat', resolution='daily', station_ids=['00375'], start_date='2026-01-01', end_date='2026-01-02', elements=['tas_mean'])
        klimat_recent_targets = build_pl_daily_download_targets(klimat_recent)
        self.assertEqual([target.archive_url for target in klimat_recent_targets], [
            f'{PL_DAILY_KLIMAT_BASE_URL}/2026/2026_01_k.zip',
        ])

    def test_download_daily_observations_country_pl_combines_station_year_and_current_year_month(self) -> None:
        station_metadata = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        station_zip = _build_zip_bytes('2025_375_s.csv', SAMPLE_STATION_2025_PATH.read_text(encoding='utf-8'))
        month_zip = _build_zip_bytes('2026_01_s.csv', SAMPLE_MONTH_2026_01_PATH.read_text(encoding='utf-8'))

        def fake_get(url, timeout=60):
            if url == f'{PL_DAILY_SYNOP_BASE_URL}/2025/2025_375_s.zip':
                return _MockResponse(content=station_zip)
            if url == f'{PL_DAILY_SYNOP_BASE_URL}/2026/2026_01_s.zip':
                return _MockResponse(content=month_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2026-01-02', elements=['tas_mean', 'precipitation', 'sunshine_duration'])
        with patch('weatherdownload.providers.pl.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PL', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), PL_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'sunshine_duration', 'tas_mean'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        value_lookup = observations.set_index(['element', 'observation_date'])['value']
        flag_lookup = observations.set_index(['element', 'observation_date'])['flag']
        self.assertAlmostEqual(float(value_lookup[('tas_mean', pd.Timestamp('2025-01-02').date())]), 4.2)
        self.assertAlmostEqual(float(value_lookup[('precipitation', pd.Timestamp('2026-01-02').date())]), 0.0)
        self.assertAlmostEqual(float(value_lookup[('sunshine_duration', pd.Timestamp('2026-01-02').date())]), 0.0)
        self.assertEqual(flag_lookup[('precipitation', pd.Timestamp('2026-01-02').date())], '9')

    def test_download_hourly_observations_country_pl_station_year_archive(self) -> None:
        station_metadata = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        hourly_zip = _build_zip_bytes('2025_375_s.csv', SAMPLE_HOURLY_STATION_2025_PATH.read_text(encoding='utf-8'))

        def fake_get(url, timeout=60):
            if url == f'{PL_HOURLY_SYNOP_BASE_URL}/2025/2025_375_s.zip':
                return _MockResponse(content=hourly_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['tas_mean', 'pressure', 'wind_speed'])
        with patch('weatherdownload.providers.pl.hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PL', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), PL_NORMALIZED_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['pressure', 'tas_mean', 'wind_speed'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['1hour'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertEqual(str(observations.iloc[0]['timestamp']), '2025-01-01 00:00:00+00:00')
        value_lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(value_lookup[('tas_mean', pd.Timestamp('2025-01-01T00:00:00Z'))]), 1.2)
        self.assertAlmostEqual(float(value_lookup[('pressure', pd.Timestamp('2025-01-01T01:00:00Z'))]), 1008.5)
        self.assertAlmostEqual(float(value_lookup[('wind_speed', pd.Timestamp('2025-01-01T00:00:00Z'))]), 3.4)

    def test_download_daily_observations_country_pl_klimat_month_archive(self) -> None:
        station_metadata = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        klimat_zip = _build_zip_bytes('2026_01_k.csv', SAMPLE_KLIMAT_MONTH_2026_01_PATH.read_text(encoding='utf-8'))

        def fake_get(url, timeout=60):
            if url == f'{PL_DAILY_KLIMAT_BASE_URL}/2026/2026_01_k.zip':
                return _MockResponse(content=klimat_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='PL', dataset_scope='historical_klimat', resolution='daily', station_ids=['00375'], start_date='2026-01-01', end_date='2026-01-02', elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'])
        with patch('weatherdownload.providers.pl.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PL', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), PL_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max', 'tas_mean', 'tas_min'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical_klimat'])
        value_lookup = observations.set_index(['element', 'observation_date'])['value']
        flag_lookup = observations.set_index(['element', 'observation_date'])['flag']
        self.assertAlmostEqual(float(value_lookup[('tas_mean', pd.Timestamp('2026-01-01').date())]), 0.8)
        self.assertAlmostEqual(float(value_lookup[('tas_max', pd.Timestamp('2026-01-02').date())]), 4.0)
        self.assertAlmostEqual(float(value_lookup[('precipitation', pd.Timestamp('2026-01-02').date())]), 0.0)
        self.assertEqual(flag_lookup[('precipitation', pd.Timestamp('2026-01-02').date())], '9')
        self.assertTrue((observations['quality'].isna()).all())

    def test_pl_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        station_zip = _build_zip_bytes('2025_375_s.csv', SAMPLE_STATION_2025_PATH.read_text(encoding='utf-8'))

        def fake_get(url, timeout=60):
            if url == f'{PL_DAILY_SYNOP_BASE_URL}/2025/2025_375_s.zip':
                return _MockResponse(content=station_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=list(EXPECTED_PL_DAILY_MAPPING.keys()))
        with patch('weatherdownload.providers.pl.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PL', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_PL_DAILY_MAPPING)
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_max', pd.Timestamp('2025-01-01').date())]), 6.5)
        self.assertAlmostEqual(float(lookup[('tas_min', pd.Timestamp('2025-01-02').date())]), -0.5)
        self.assertAlmostEqual(float(lookup[('sunshine_duration', pd.Timestamp('2025-01-02').date())]), 0.0)

    def test_pl_hourly_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        hourly_zip = _build_zip_bytes('2025_375_s.csv', SAMPLE_HOURLY_STATION_2025_PATH.read_text(encoding='utf-8'))

        def fake_get(url, timeout=60):
            if url == f'{PL_HOURLY_SYNOP_BASE_URL}/2025/2025_375_s.zip':
                return _MockResponse(content=hourly_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=list(EXPECTED_PL_HOURLY_MAPPING.keys()))
        with patch('weatherdownload.providers.pl.hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PL', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_PL_HOURLY_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2025-01-01T00:00:00Z'))]), 1.2)
        self.assertAlmostEqual(float(lookup[('vapour_pressure', pd.Timestamp('2025-01-01T00:00:00Z'))]), 6.5)
        self.assertAlmostEqual(float(lookup[('wind_speed_max', pd.Timestamp('2025-01-01T01:00:00Z'))]), 5.1)

    def test_pl_klimat_contract_mapping_and_synop_behavior_remains_unchanged(self) -> None:
        station_metadata = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        klimat_zip = _build_zip_bytes('2026_01_k.csv', SAMPLE_KLIMAT_MONTH_2026_01_PATH.read_text(encoding='utf-8'))

        def fake_get(url, timeout=60):
            if url == f'{PL_DAILY_KLIMAT_BASE_URL}/2026/2026_01_k.zip':
                return _MockResponse(content=klimat_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='PL', dataset_scope='historical_klimat', resolution='daily', station_ids=['00375'], start_date='2026-01-01', end_date='2026-01-02', elements=list(EXPECTED_PL_KLIMAT_MAPPING.keys()))
        with patch('weatherdownload.providers.pl.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PL', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_PL_KLIMAT_MAPPING)
        self.assertNotIn('sunshine_duration', observations['element'].unique().tolist())


if __name__ == '__main__':
    unittest.main()


