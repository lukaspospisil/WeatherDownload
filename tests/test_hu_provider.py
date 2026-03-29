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
from weatherdownload.providers.hu.daily import HU_DAILY_HISTORICAL_URL, HU_DAILY_RECENT_URL
from weatherdownload.providers.hu.hourly import HU_HOURLY_HISTORICAL_URL, HU_HOURLY_RECENT_URL
from weatherdownload.providers.hu.parser import (
    HU_NORMALIZED_DAILY_COLUMNS,
    HU_NORMALIZED_SUBDAILY_COLUMNS,
    parse_hu_daily_csv,
    parse_hu_subdaily_csv,
)
from weatherdownload.providers.hu.registry import (
    HU_TENMIN_HISTORICAL_URL,
    HU_TENMIN_RECENT_URL,
    HU_TENMIN_WIND_HISTORICAL_URL,
    HU_TENMIN_WIND_METADATA_URL,
    HU_TENMIN_WIND_RECENT_URL,
)

SAMPLE_STATIONS_PATH = Path('tests/data/sample_hu_station_meta_auto.csv')
SAMPLE_WIND_STATIONS_PATH = Path('tests/data/sample_hu_station_meta_auto_wind.csv')
SAMPLE_DAILY_HISTORICAL_INDEX = Path('tests/data/sample_hu_daily_historical_index.html').read_text(encoding='utf-8')
SAMPLE_DAILY_HISTORICAL_CSV = Path('tests/data/sample_hu_daily_hist_13704.csv').read_text(encoding='utf-8')
SAMPLE_DAILY_RECENT_CSV = Path('tests/data/sample_hu_daily_recent_13704.csv').read_text(encoding='utf-8')
SAMPLE_HOURLY_HISTORICAL_INDEX = Path('tests/data/sample_hu_hourly_historical_index.html').read_text(encoding='utf-8')
SAMPLE_HOURLY_HISTORICAL_CSV = Path('tests/data/sample_hu_hourly_hist_13704.csv').read_text(encoding='utf-8')
SAMPLE_HOURLY_RECENT_CSV = Path('tests/data/sample_hu_hourly_recent_13704.csv').read_text(encoding='utf-8')
SAMPLE_TENMIN_HISTORICAL_INDEX = Path('tests/data/sample_hu_tenmin_historical_index.html').read_text(encoding='utf-8')
SAMPLE_TENMIN_HISTORICAL_CSV = Path('tests/data/sample_hu_tenmin_hist_13704.csv').read_text(encoding='utf-8')
SAMPLE_TENMIN_RECENT_CSV = Path('tests/data/sample_hu_tenmin_recent_13704.csv').read_text(encoding='utf-8')
SAMPLE_TENMIN_WIND_HISTORICAL_INDEX = Path('tests/data/sample_hu_tenmin_wind_historical_index.html').read_text(encoding='utf-8')
SAMPLE_TENMIN_WIND_HISTORICAL_CSV = Path('tests/data/sample_hu_tenmin_wind_hist_26327.csv').read_text(encoding='utf-8')
SAMPLE_TENMIN_WIND_RECENT_CSV = Path('tests/data/sample_hu_tenmin_wind_recent_26327.csv').read_text(encoding='utf-8')
EXPECTED_HU_DAILY_MAPPING = {
    'tas_mean': 't',
    'tas_max': 'tx',
    'tas_min': 'tn',
    'precipitation': 'rau',
    'wind_speed': 'fs',
    'relative_humidity': 'u',
    'sunshine_duration': 'f',
}
EXPECTED_HU_HOURLY_MAPPING = {
    'precipitation': 'r',
    'tas_mean': 'ta',
    'pressure': 'p',
    'relative_humidity': 'u',
    'wind_speed': 'f',
}
EXPECTED_HU_TENMIN_MAPPING = {
    'precipitation': 'r',
    'tas_mean': 'ta',
    'pressure': 'p',
    'relative_humidity': 'u',
    'wind_speed': 'fs',
}
EXPECTED_HU_TENMIN_WIND_MAPPING = {
    'wind_speed': 'fs',
    'wind_speed_max': 'fx',
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
        archive.writestr(filename, csv_text.encode('utf-8'))
    return buffer.getvalue()


class HungaryProviderTests(unittest.TestCase):
    def test_supported_countries_include_hu(self) -> None:
        self.assertIn('HU', list_supported_countries())
        self.assertEqual(list_dataset_scopes(country='HU'), ['historical', 'historical_wind'])
        self.assertEqual(list_resolutions(country='HU', dataset_scope='historical'), ['10min', '1hour', 'daily'])
        self.assertEqual(list_resolutions(country='HU', dataset_scope='historical_wind'), ['10min'])

    def test_read_station_metadata_country_hu_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['13704', '13704', '13711'])
        self.assertEqual(stations.iloc[0]['full_name'], 'Sopron Kuruc-domb')
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_metadata_country_hu_merges_wind_metadata_by_default(self) -> None:
        generic_text = SAMPLE_STATIONS_PATH.read_text(encoding='utf-8')
        wind_text = SAMPLE_WIND_STATIONS_PATH.read_text(encoding='utf-8')

        def fake_get(url, timeout=60):
            if url.endswith('/meta/station_meta_auto.csv'):
                return _MockResponse(text=generic_text)
            if url == HU_TENMIN_WIND_METADATA_URL:
                return _MockResponse(text=wind_text)
            return _MockResponse(status_code=404)

        with patch('weatherdownload.providers.hu.metadata.requests.get', side_effect=fake_get):
            stations = read_station_metadata(country='HU')
        self.assertIn('26327', stations['station_id'].tolist())
        self.assertIn('13704', stations['station_id'].tolist())
        self.assertTrue(stations[stations['station_id'] == '26327'].iloc[0]['full_name'].startswith('Z'))

    def test_read_station_observation_metadata_country_hu_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(set(metadata['obs_type']), {'HISTORICAL_DAILY', 'HISTORICAL_HOURLY', 'HISTORICAL_10MIN'})
        self.assertEqual(sorted(metadata['element'].unique().tolist()), ['f', 'fs', 'p', 'r', 'rau', 't', 'ta', 'tn', 'tx', 'u'])
        self.assertTrue(metadata['height'].isna().all())

    def test_read_station_observation_metadata_country_hu_includes_wind_scope_by_default(self) -> None:
        generic_text = SAMPLE_STATIONS_PATH.read_text(encoding='utf-8')
        wind_text = SAMPLE_WIND_STATIONS_PATH.read_text(encoding='utf-8')

        def fake_get(url, timeout=60):
            if url.endswith('/meta/station_meta_auto.csv'):
                return _MockResponse(text=generic_text)
            if url == HU_TENMIN_WIND_METADATA_URL:
                return _MockResponse(text=wind_text)
            return _MockResponse(status_code=404)

        with patch('weatherdownload.providers.hu.metadata.requests.get', side_effect=fake_get):
            metadata = read_station_observation_metadata(country='HU')
        self.assertIn('HISTORICAL_10MIN_WIND', set(metadata['obs_type']))
        wind_rows = metadata[metadata['station_id'] == '26327']
        self.assertEqual(sorted(wind_rows['element'].unique().tolist()), ['fs', 'fx'])

    def test_discovery_country_hu_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='HU', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', dataset_scope='historical', resolution='1hour'),
            ['precipitation', 'tas_mean', 'pressure', 'relative_humidity', 'wind_speed'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', dataset_scope='historical', resolution='10min'),
            ['precipitation', 'tas_mean', 'pressure', 'relative_humidity', 'wind_speed'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', dataset_scope='historical_wind', resolution='10min'),
            ['wind_speed', 'wind_speed_max'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', dataset_scope='historical_wind', resolution='10min', provider_raw=True),
            ['fs', 'fx'],
        )

    def test_hu_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2025-07-28', end_date='2025-07-29', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2025-07-28', end_date='2025-07-29', elements=['t', 'rau'])
        self.assertEqual(canonical_query.elements, ['t', 'rau'])
        self.assertEqual(raw_query.elements, ['t', 'rau'])

    def test_hu_hourly_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='1hour', station_ids=['13704'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean', 'pressure'])
        raw_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='1hour', station_ids=['13704'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=['ta', 'p'])
        self.assertEqual(canonical_query.elements, ['ta', 'p'])
        self.assertEqual(raw_query.elements, ['ta', 'p'])

    def test_hu_tenmin_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='10min', station_ids=['13704'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['tas_mean', 'pressure'])
        raw_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='10min', station_ids=['13704'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['ta', 'p'])
        self.assertEqual(canonical_query.elements, ['ta', 'p'])
        self.assertEqual(raw_query.elements, ['ta', 'p'])

    def test_hu_tenmin_wind_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='HU', dataset_scope='historical_wind', resolution='10min', station_ids=['26327'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['wind_speed', 'wind_speed_max'])
        raw_query = ObservationQuery(country='HU', dataset_scope='historical_wind', resolution='10min', station_ids=['26327'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['fs', 'fx'])
        self.assertEqual(canonical_query.elements, ['fs', 'fx'])
        self.assertEqual(raw_query.elements, ['fs', 'fx'])

    def test_hu_subdaily_query_rejects_date_only_inputs(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='HU', dataset_scope='historical', resolution='1hour', station_ids=['13704'], start_date='2026-01-01', end_date='2026-01-01', elements=['tas_mean'])

    def test_hu_wind_scope_rejects_unsupported_resolution(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='HU', dataset_scope='historical_wind', resolution='daily', station_ids=['26327'], start_date='2026-01-01', end_date='2026-01-01', elements=['wind_speed'])

    def test_parse_hu_daily_csv_keeps_source_columns(self) -> None:
        parsed = parse_hu_daily_csv(SAMPLE_DAILY_HISTORICAL_CSV)
        self.assertIn('StationNumber', parsed.columns)
        self.assertIn('Time', parsed.columns)
        self.assertIn('rau', parsed.columns)
        self.assertIn('Q_rau', parsed.columns)
        self.assertEqual(len(parsed), 2)

    def test_parse_hu_subdaily_csv_keeps_source_columns(self) -> None:
        parsed = parse_hu_subdaily_csv(SAMPLE_HOURLY_HISTORICAL_CSV)
        self.assertIn('StationNumber', parsed.columns)
        self.assertIn('Time', parsed.columns)
        self.assertIn('ta', parsed.columns)
        self.assertIn('Q_ta', parsed.columns)
        self.assertEqual(len(parsed), 2)

    def test_download_daily_observations_country_hu_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_1D_20050727_20251231_13704.csv', SAMPLE_DAILY_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_1D_20260101_20260328_13704.csv', SAMPLE_DAILY_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_DAILY_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_DAILY_HISTORICAL_INDEX)
            if url == f'{HU_DAILY_HISTORICAL_URL}HABP_1D_13704_20050727_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_DAILY_RECENT_URL}HABP_1D_13704_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2025-07-28', end_date='2026-01-02', elements=['tas_mean', 'precipitation', 'sunshine_duration'])
        with patch('weatherdownload.providers.hu.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), HU_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'sunshine_duration', 'tas_mean'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertIn(pd.Timestamp('2026-01-02').date(), observations['observation_date'].tolist())
        flag_lookup = observations.set_index(['element', 'observation_date'])['flag']
        self.assertEqual(flag_lookup[('precipitation', pd.Timestamp('2026-01-02').date())], 'A')

    def test_download_hourly_observations_country_hu_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_1H_20020101_20251231_13704.csv', SAMPLE_HOURLY_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_1H_20260101_20260329_13704.csv', SAMPLE_HOURLY_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_HOURLY_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_HOURLY_HISTORICAL_INDEX)
            if url == f'{HU_HOURLY_HISTORICAL_URL}HABP_1H_13704_20020101_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_HOURLY_RECENT_URL}HABP_1H_13704_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='1hour', station_ids=['13704'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean', 'pressure', 'precipitation'])
        with patch('weatherdownload.providers.hu.hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), HU_NORMALIZED_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'pressure', 'tas_mean'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['1hour'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertEqual(str(observations.iloc[0]['timestamp']), '2025-12-31 23:00:00+00:00')
        flag_lookup = observations.set_index(['element', 'timestamp'])['flag']
        self.assertEqual(flag_lookup[('precipitation', pd.Timestamp('2026-01-01T01:00:00Z'))], 'C')

    def test_download_tenmin_observations_country_hu_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_10M_20020101_20251231_13704.csv', SAMPLE_TENMIN_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_10M_20260101_20260329_13704.csv', SAMPLE_TENMIN_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_TENMIN_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_TENMIN_HISTORICAL_INDEX)
            if url == f'{HU_TENMIN_HISTORICAL_URL}HABP_10M_13704_20020101_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_TENMIN_RECENT_URL}HABP_10M_13704_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='10min', station_ids=['13704'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['tas_mean', 'pressure', 'precipitation'])
        with patch('weatherdownload.providers.hu.tenmin.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), HU_NORMALIZED_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'pressure', 'tas_mean'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['10min'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertEqual(str(observations.iloc[0]['timestamp']), '2025-12-31 23:50:00+00:00')
        flag_lookup = observations.set_index(['element', 'timestamp'])['flag']
        self.assertEqual(flag_lookup[('precipitation', pd.Timestamp('2026-01-01T00:10:00Z'))], 'A')

    def test_download_tenmin_wind_observations_country_hu_uses_separate_scope(self) -> None:
        generic_station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        wind_station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_WIND_STATIONS_PATH))
        station_metadata = pd.concat([generic_station_metadata, wind_station_metadata], ignore_index=True)
        historical_zip = _build_zip_bytes('HABP_10MWIND_20251231_20260101_26327.csv', SAMPLE_TENMIN_WIND_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_10MWIND_20260101_20260328_26327.csv', SAMPLE_TENMIN_WIND_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_TENMIN_WIND_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_TENMIN_WIND_HISTORICAL_INDEX)
            if url == f'{HU_TENMIN_WIND_HISTORICAL_URL}HABP_10MWIND_26327_20020101_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_TENMIN_WIND_RECENT_URL}HABP_10MWIND_26327_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical_wind', resolution='10min', station_ids=['26327'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['wind_speed', 'wind_speed_max'])
        with patch('weatherdownload.providers.hu.tenmin_wind.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), HU_NORMALIZED_SUBDAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['wind_speed', 'wind_speed_max'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical_wind'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['10min'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertEqual(str(observations.iloc[0]['timestamp']), '2025-12-31 23:50:00+00:00')
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_HU_TENMIN_WIND_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('wind_speed', pd.Timestamp('2025-12-31T23:50:00Z'))]), 2.4)
        self.assertAlmostEqual(float(lookup[('wind_speed_max', pd.Timestamp('2026-01-01T00:10:00Z'))]), 3.4)

    def test_hu_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_1D_20050727_20251231_13704.csv', SAMPLE_DAILY_HISTORICAL_CSV)

        def fake_get(url, timeout=60):
            if url == HU_DAILY_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_DAILY_HISTORICAL_INDEX)
            if url == f'{HU_DAILY_HISTORICAL_URL}HABP_1D_13704_20050727_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2005-07-28', end_date='2005-07-29', elements=list(EXPECTED_HU_DAILY_MAPPING.keys()))
        with patch('weatherdownload.providers.hu.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_HU_DAILY_MAPPING)
        tas_mean_row = observations[(observations['element'] == 'tas_mean') & (observations['observation_date'] == pd.Timestamp('2005-07-28').date())]
        sunshine_row = observations[(observations['element'] == 'sunshine_duration') & (observations['observation_date'] == pd.Timestamp('2005-07-29').date())]
        self.assertEqual(len(tas_mean_row), 1)
        self.assertEqual(len(sunshine_row), 1)
        self.assertAlmostEqual(float(tas_mean_row.iloc[0]['value']), 28.3)
        self.assertAlmostEqual(float(sunshine_row.iloc[0]['value']), 5.4)

    def test_hu_hourly_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_1H_20020101_20251231_13704.csv', SAMPLE_HOURLY_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_1H_20260101_20260329_13704.csv', SAMPLE_HOURLY_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_HOURLY_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_HOURLY_HISTORICAL_INDEX)
            if url == f'{HU_HOURLY_HISTORICAL_URL}HABP_1H_13704_20020101_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_HOURLY_RECENT_URL}HABP_1H_13704_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='1hour', station_ids=['13704'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=list(EXPECTED_HU_HOURLY_MAPPING.keys()))
        with patch('weatherdownload.providers.hu.hourly.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_HU_HOURLY_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2025-12-31T23:00:00Z'))]), 1.8)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2026-01-01T00:00:00Z'))]), 1003.5)

    def test_hu_tenmin_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_10M_20020101_20251231_13704.csv', SAMPLE_TENMIN_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_10M_20260101_20260329_13704.csv', SAMPLE_TENMIN_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_TENMIN_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_TENMIN_HISTORICAL_INDEX)
            if url == f'{HU_TENMIN_HISTORICAL_URL}HABP_10M_13704_20020101_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_TENMIN_RECENT_URL}HABP_10M_13704_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='10min', station_ids=['13704'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=list(EXPECTED_HU_TENMIN_MAPPING.keys()))
        with patch('weatherdownload.providers.hu.tenmin.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_HU_TENMIN_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', pd.Timestamp('2025-12-31T23:50:00Z'))]), 1.6)
        self.assertAlmostEqual(float(lookup[('pressure', pd.Timestamp('2026-01-01T00:10:00Z'))]), 1003.6)

    def test_hu_tenmin_wind_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_WIND_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_10MWIND_20251231_20260101_26327.csv', SAMPLE_TENMIN_WIND_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_10MWIND_20260101_20260328_26327.csv', SAMPLE_TENMIN_WIND_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_TENMIN_WIND_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_TENMIN_WIND_HISTORICAL_INDEX)
            if url == f'{HU_TENMIN_WIND_HISTORICAL_URL}HABP_10MWIND_26327_20020101_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_TENMIN_WIND_RECENT_URL}HABP_10MWIND_26327_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical_wind', resolution='10min', station_ids=['26327'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=list(EXPECTED_HU_TENMIN_WIND_MAPPING.keys()))
        with patch('weatherdownload.providers.hu.tenmin_wind.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_HU_TENMIN_WIND_MAPPING)
        lookup = observations.set_index(['element', 'timestamp'])['value']
        self.assertAlmostEqual(float(lookup[('wind_speed', pd.Timestamp('2025-12-31T23:50:00Z'))]), 2.4)
        self.assertAlmostEqual(float(lookup[('wind_speed_max', pd.Timestamp('2026-01-01T00:10:00Z'))]), 3.4)


if __name__ == '__main__':
    unittest.main()

