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
from weatherdownload.hu_daily import HU_DAILY_HISTORICAL_URL, HU_DAILY_RECENT_URL
from weatherdownload.hu_parser import HU_NORMALIZED_DAILY_COLUMNS, parse_hu_daily_csv

SAMPLE_STATIONS_PATH = Path('tests/data/sample_hu_station_meta_auto.csv')
SAMPLE_HISTORICAL_INDEX = Path('tests/data/sample_hu_daily_historical_index.html').read_text(encoding='utf-8')
SAMPLE_HISTORICAL_CSV = Path('tests/data/sample_hu_daily_hist_13704.csv').read_text(encoding='utf-8')
SAMPLE_RECENT_CSV = Path('tests/data/sample_hu_daily_recent_13704.csv').read_text(encoding='utf-8')
EXPECTED_HU_DAILY_MAPPING = {
    'tas_mean': 't',
    'tas_max': 'tx',
    'tas_min': 'tn',
    'precipitation': 'rau',
    'wind_speed': 'fs',
    'relative_humidity': 'u',
    'sunshine_duration': 'f',
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
        self.assertEqual(list_dataset_scopes(country='HU'), ['historical'])
        self.assertEqual(list_resolutions(country='HU', dataset_scope='historical'), ['daily'])

    def test_read_station_metadata_country_hu_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['13704', '13704', '13711'])
        self.assertEqual(stations.iloc[0]['full_name'], 'Sopron Kuruc-domb')
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_hu_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(set(metadata['obs_type']), {'HISTORICAL_DAILY'})
        self.assertEqual(sorted(metadata['element'].unique().tolist()), ['f', 'fs', 'rau', 't', 'tn', 'tx', 'u'])
        self.assertTrue(metadata['height'].isna().all())

    def test_discovery_country_hu_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='HU', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='HU', dataset_scope='historical', resolution='daily', provider_raw=True),
            ['t', 'tx', 'tn', 'rau', 'fs', 'u', 'f'],
        )

    def test_hu_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2025-07-28', end_date='2025-07-29', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2025-07-28', end_date='2025-07-29', elements=['t', 'rau'])
        self.assertEqual(canonical_query.elements, ['t', 'rau'])
        self.assertEqual(raw_query.elements, ['t', 'rau'])

    def test_hu_unsupported_hourly_query_stays_explicit(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='HU', dataset_scope='historical', resolution='1hour', station_ids=['13704'], start='2026-01-01T00:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean'])

    def test_parse_hu_daily_csv_keeps_source_columns(self) -> None:
        parsed = parse_hu_daily_csv(SAMPLE_HISTORICAL_CSV)
        self.assertIn('StationNumber', parsed.columns)
        self.assertIn('Time', parsed.columns)
        self.assertIn('rau', parsed.columns)
        self.assertIn('Q_rau', parsed.columns)
        self.assertEqual(len(parsed), 2)

    def test_download_daily_observations_country_hu_combines_historical_and_recent(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_1D_20050727_20251231_13704.csv', SAMPLE_HISTORICAL_CSV)
        recent_zip = _build_zip_bytes('HABP_1D_20260101_20260328_13704.csv', SAMPLE_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url == HU_DAILY_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_HISTORICAL_INDEX)
            if url == f'{HU_DAILY_HISTORICAL_URL}HABP_1D_13704_20050727_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            if url == f'{HU_DAILY_RECENT_URL}HABP_1D_13704_akt.zip':
                return _MockResponse(content=recent_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2025-07-28', end_date='2026-01-02', elements=['tas_mean', 'precipitation', 'sunshine_duration'])
        with patch('weatherdownload.hu_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), HU_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'sunshine_duration', 'tas_mean'])
        self.assertEqual(observations['dataset_scope'].unique().tolist(), ['historical'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        self.assertIn(pd.Timestamp('2026-01-02').date(), observations['observation_date'].tolist())
        flag_lookup = observations.set_index(['element', 'observation_date'])['flag']
        self.assertEqual(flag_lookup[('precipitation', pd.Timestamp('2026-01-02').date())], 'A')

    def test_hu_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='HU', source_url=str(SAMPLE_STATIONS_PATH))
        historical_zip = _build_zip_bytes('HABP_1D_20050727_20251231_13704.csv', SAMPLE_HISTORICAL_CSV)

        def fake_get(url, timeout=60):
            if url == HU_DAILY_HISTORICAL_URL:
                return _MockResponse(text=SAMPLE_HISTORICAL_INDEX)
            if url == f'{HU_DAILY_HISTORICAL_URL}HABP_1D_13704_20050727_20251231_hist.zip':
                return _MockResponse(content=historical_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2005-07-28', end_date='2005-07-29', elements=list(EXPECTED_HU_DAILY_MAPPING.keys()))
        with patch('weatherdownload.hu_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='HU', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_HU_DAILY_MAPPING)
        tas_mean_row = observations[(observations['element'] == 'tas_mean') & (observations['observation_date'] == pd.Timestamp('2005-07-28').date())]
        sunshine_row = observations[(observations['element'] == 'sunshine_duration') & (observations['observation_date'] == pd.Timestamp('2005-07-29').date())]
        self.assertEqual(len(tas_mean_row), 1)
        self.assertEqual(len(sunshine_row), 1)
        self.assertAlmostEqual(float(tas_mean_row.iloc[0]['value']), 28.3)
        self.assertAlmostEqual(float(sunshine_row.iloc[0]['value']), 5.4)


if __name__ == '__main__':
    unittest.main()



