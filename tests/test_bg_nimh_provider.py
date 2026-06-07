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
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.bg.parser import BG_NIMH_NORMALIZED_DAILY_COLUMNS


SAMPLE_BG_RAIN_PAGE_PATH = Path('tests/data/sample_bg_openData_rain.html')
SAMPLE_BG_SNOW_PAGE_PATH = Path('tests/data/sample_bg_openData_snow.html')
SAMPLE_BG_RAIN_202601_PATH = Path('tests/data/sample_bg_mosv_prec_202601.csv')
SAMPLE_BG_RAIN_202602_PATH = Path('tests/data/sample_bg_mosv_prec_202602.csv')
SAMPLE_BG_SNOW_202601_PATH = Path('tests/data/sample_bg_mosv_snow_202601.csv')
SAMPLE_BG_SNOW_202602_PATH = Path('tests/data/sample_bg_mosv_snow_202602.csv')


class _MockResponse:
    def __init__(self, text: str | None = None, status_code: int = 200, content: bytes | None = None) -> None:
        self.text = text or ''
        self.status_code = status_code
        self.encoding = 'utf-8'
        self.content = content if content is not None else self.text.encode('utf-8')

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


def _build_legend_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('legendSnow.csv', Path('tests/data/sample_bg_legendSnow.csv').read_bytes())
        archive.writestr('legendSnowCover.csv', Path('tests/data/sample_bg_legendSnowCover.csv').read_bytes())
    return buffer.getvalue()


SAMPLE_BG_LEGEND_ZIP_BYTES = _build_legend_zip()


def _mock_bg_open_data_get(url: str, timeout: int = 60) -> _MockResponse:
    del timeout
    normalized = url.replace('\\', '/')
    if normalized.endswith('/openData/rain'):
        return _MockResponse(SAMPLE_BG_RAIN_PAGE_PATH.read_text(encoding='utf-8'))
    if normalized.endswith('/openData/snow'):
        return _MockResponse(SAMPLE_BG_SNOW_PAGE_PATH.read_text(encoding='utf-8'))
    if normalized.endswith('mosv_prec_202601.csv'):
        return _MockResponse(SAMPLE_BG_RAIN_202601_PATH.read_text(encoding='utf-8'))
    if normalized.endswith('mosv_prec_202602.csv'):
        return _MockResponse(SAMPLE_BG_RAIN_202602_PATH.read_text(encoding='utf-8'))
    if normalized.endswith('mosv_snow_202601.csv'):
        return _MockResponse(SAMPLE_BG_SNOW_202601_PATH.read_text(encoding='utf-8'))
    if normalized.endswith('mosv_snow_202602.csv'):
        return _MockResponse(SAMPLE_BG_SNOW_202602_PATH.read_text(encoding='utf-8'))
    if normalized.endswith('legendSnow.zip'):
        return _MockResponse(content=SAMPLE_BG_LEGEND_ZIP_BYTES)
    raise AssertionError(f'unexpected BG NIMH URL: {url}')


class BulgariaNimhProviderTests(unittest.TestCase):
    def test_discovery_country_bg_includes_nimh_and_ghcnd_daily(self) -> None:
        self.assertEqual(list_providers(country='BG'), ['ghcnd', 'nimh'])
        self.assertEqual(list_resolutions(country='BG', provider='nimh'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='BG', provider='nimh', resolution='daily'),
            ['precipitation', 'snow_depth'],
        )
        self.assertEqual(
            list_supported_elements(country='BG', provider='nimh', resolution='daily', provider_raw=True),
            ['precipitation', 'snow_cover_depth'],
        )

    def test_bg_nimh_query_accepts_canonical_elements_and_rejects_daily_temperature(self) -> None:
        query = ObservationQuery(
            country='BG',
            provider='nimh',
            resolution='daily',
            station_ids=['1010'],
            start_date='2026-01-01',
            end_date='2026-01-03',
            elements=['precipitation', 'snow_depth'],
        )
        self.assertEqual(query.elements, ['precipitation', 'snow_cover_depth'])
        with self.assertRaises(QueryValidationError):
            ObservationQuery(
                country='BG',
                provider='nimh',
                resolution='daily',
                station_ids=['1010'],
                start_date='2026-01-01',
                end_date='2026-01-03',
                elements=['tas_mean'],
            )

    def test_read_station_metadata_country_bg_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='BG', source_url=str(SAMPLE_BG_RAIN_PAGE_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['1010', '1020', '3010'])
        self.assertEqual(stations['full_name'].tolist(), ['NovoSelo', 'Vidin', 'Vratca'])
        self.assertTrue(stations['gh_id'].isna().all())

    def test_read_station_observation_metadata_country_bg_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='BG', source_url=str(SAMPLE_BG_RAIN_PAGE_PATH))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(set(metadata['station_id']), {'1010', '1020', '3010'})
        self.assertEqual(set(metadata['element']), {'precipitation', 'snow_cover_depth'})

    def test_download_daily_observations_country_bg_combines_rain_and_snow(self) -> None:
        station_metadata = read_station_metadata(country='BG', source_url=str(SAMPLE_BG_RAIN_PAGE_PATH))
        query = ObservationQuery(
            country='BG',
            provider='nimh',
            resolution='daily',
            station_ids=['1010', '1020'],
            start_date='2026-01-01',
            end_date='2026-01-03',
            elements=['precipitation', 'snow_depth'],
        )

        with patch('weatherdownload.providers.bg.parser.requests.get', side_effect=_mock_bg_open_data_get):
            observations = download_observations(query, country='BG', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), BG_NIMH_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'snow_depth'])
        self.assertEqual(observations['provider'].unique().tolist(), ['nimh'])
        lookup = observations.set_index(['station_id', 'element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('1010', 'precipitation', pd.Timestamp('2026-01-01').date())]), 0.0)
        self.assertAlmostEqual(float(lookup[('1010', 'precipitation', pd.Timestamp('2026-01-02').date())]), 6.0)
        self.assertAlmostEqual(float(lookup[('1010', 'snow_depth', pd.Timestamp('2026-01-02').date())]), 5.0)
        self.assertAlmostEqual(float(lookup[('1020', 'snow_depth', pd.Timestamp('2026-01-01').date())]), 2.0)
        self.assertTrue(pd.isna(lookup[('1020', 'precipitation', pd.Timestamp('2026-01-02').date())]))
        self.assertTrue(pd.isna(lookup[('1020', 'snow_depth', pd.Timestamp('2026-01-02').date())]))


if __name__ == '__main__':
    unittest.main()
