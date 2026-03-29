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
from weatherdownload.pl_daily import PL_DAILY_SYNOP_BASE_URL, build_pl_daily_download_targets
from weatherdownload.pl_parser import PL_NORMALIZED_DAILY_COLUMNS, parse_pl_daily_synop_csv

SAMPLE_STATIONS_PATH = Path('tests/data/sample_pl_wykaz_stacji.csv')
SAMPLE_STATION_2025_PATH = Path('tests/data/sample_pl_synop_station_2025.csv')
SAMPLE_MONTH_2026_01_PATH = Path('tests/data/sample_pl_synop_month_2026_01.csv')
EXPECTED_PL_DAILY_MAPPING = {
    'tas_mean': 'STD',
    'tas_max': 'TMAX',
    'tas_min': 'TMIN',
    'precipitation': 'SMDB',
    'sunshine_duration': 'USL',
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
        self.assertEqual(list_dataset_scopes(country='PL'), ['historical'])
        self.assertEqual(list_resolutions(country='PL', dataset_scope='historical'), ['daily'])

    def test_read_station_metadata_country_pl_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(stations.columns), ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'])
        self.assertEqual(stations['station_id'].tolist(), ['00375', '00400', '00600'])
        self.assertEqual(stations.iloc[0]['gh_id'], '352200375')
        self.assertEqual(stations.iloc[0]['full_name'], 'WARSZAWA')
        self.assertTrue(stations['longitude'].isna().all())

    def test_read_station_observation_metadata_country_pl_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(list(metadata.columns), ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])
        self.assertEqual(set(metadata['obs_type']), {'HISTORICAL_DAILY'})
        self.assertEqual(sorted(metadata['element'].unique().tolist()), ['SMDB', 'STD', 'TMAX', 'TMIN', 'USL'])
        self.assertTrue(metadata['height'].isna().all())

    def test_discovery_country_pl_returns_canonical_and_raw_elements(self) -> None:
        self.assertEqual(
            list_supported_elements(country='PL', dataset_scope='historical', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='PL', dataset_scope='historical', resolution='daily', provider_raw=True),
            ['STD', 'TMAX', 'TMIN', 'SMDB', 'USL'],
        )

    def test_pl_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=['tas_mean', 'precipitation'])
        raw_query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=['STD', 'SMDB'])
        self.assertEqual(canonical_query.elements, ['STD', 'SMDB'])
        self.assertEqual(raw_query.elements, ['STD', 'SMDB'])

    def test_pl_query_rejects_unsupported_resolution_and_element(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['tas_mean'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=['wind_speed'])

    def test_parse_pl_daily_synop_csv_keeps_source_columns(self) -> None:
        parsed = parse_pl_daily_synop_csv(SAMPLE_STATION_2025_PATH.read_text(encoding='utf-8'))
        self.assertIn('NSP', parsed.columns)
        self.assertIn('ROK', parsed.columns)
        self.assertIn('STD', parsed.columns)
        self.assertIn('WSMDB', parsed.columns)
        self.assertEqual(len(parsed), 2)

    def test_build_targets_country_pl_handles_station_year_current_year_and_bucket_paths(self) -> None:
        query_recent = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2026-01-02', elements=['tas_mean'])
        recent_targets = build_pl_daily_download_targets(query_recent)
        self.assertEqual([target.archive_url for target in recent_targets], [
            f'{PL_DAILY_SYNOP_BASE_URL}/2025/2025_375_s.zip',
            f'{PL_DAILY_SYNOP_BASE_URL}/2026/2026_01_s.zip',
        ])

        query_legacy = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='1999-01-01', end_date='1999-01-02', elements=['tas_mean'])
        legacy_targets = build_pl_daily_download_targets(query_legacy)
        self.assertEqual([target.archive_url for target in legacy_targets], [
            f'{PL_DAILY_SYNOP_BASE_URL}/1996_2000/1996_2000_375_s.zip',
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
        with patch('weatherdownload.pl_daily.requests.get', side_effect=fake_get):
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

    def test_pl_daily_contract_mapping_and_key_values_are_stable(self) -> None:
        station_metadata = read_station_metadata(country='PL', source_url=str(SAMPLE_STATIONS_PATH))
        station_zip = _build_zip_bytes('2025_375_s.csv', SAMPLE_STATION_2025_PATH.read_text(encoding='utf-8'))

        def fake_get(url, timeout=60):
            if url == f'{PL_DAILY_SYNOP_BASE_URL}/2025/2025_375_s.zip':
                return _MockResponse(content=station_zip)
            return _MockResponse(status_code=404)

        query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2025-01-02', elements=list(EXPECTED_PL_DAILY_MAPPING.keys()))
        with patch('weatherdownload.pl_daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='PL', station_metadata=station_metadata)
        mapping = {row.element: row.element_raw for row in observations[['element', 'element_raw']].drop_duplicates().itertuples(index=False)}
        self.assertEqual(mapping, EXPECTED_PL_DAILY_MAPPING)
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_max', pd.Timestamp('2025-01-01').date())]), 6.5)
        self.assertAlmostEqual(float(lookup[('tas_min', pd.Timestamp('2025-01-02').date())]), -0.5)
        self.assertAlmostEqual(float(lookup[('sunshine_duration', pd.Timestamp('2025-01-02').date())]), 0.0)


if __name__ == '__main__':
    unittest.main()


