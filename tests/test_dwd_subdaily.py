import io
import unittest
import zipfile
from unittest.mock import patch

import pandas as pd

from weatherdownload import ObservationQuery, download_observations


class _MockResponse:
    def __init__(self, text: str | None = None, content: bytes | None = None, status_code: int = 200) -> None:
        self.text = text or ''
        self.content = content if content is not None else self.text.encode('utf-8')
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


def _build_hourly_tu_zip() -> bytes:
    csv_text = (
        'STATIONS_ID;MESS_DATUM;QN_9;TT_TU;RF_TU;eor\n'
        '44;1999123123;1;2.5;80.0;eor\n'
        '44;2000010100;2;3.0;82.0;eor\n'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('produkt_tu_stunde_19991231_20000101_00044.txt', csv_text.encode('latin-1'))
    return buffer.getvalue()


def _build_hourly_ff_zip() -> bytes:
    csv_text = (
        'STATIONS_ID;MESS_DATUM;QN_9;FF;eor\n'
        '44;1999123123;1;4.5;eor\n'
        '44;2000010100;2;-999;eor\n'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('produkt_ff_stunde_19991231_20000101_00044.txt', csv_text.encode('latin-1'))
    return buffer.getvalue()


def _build_tenmin_tu_zip() -> bytes:
    csv_text = (
        'STATIONS_ID;MESS_DATUM;QN;TT_10;RF_10;eor\n'
        '44;199912312350;3;1.5;90.0;eor\n'
        '44;200001010000;4;1.7;91.0;eor\n'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('produkt_zehn_min_tu_19991231_20000101_00044.txt', csv_text.encode('latin-1'))
    return buffer.getvalue()


def _build_tenmin_ff_zip() -> bytes:
    csv_text = (
        'STATIONS_ID;MESS_DATUM;QN;FF_10;eor\n'
        '44;199912312350;3;6.5;eor\n'
        '44;200001010000;4;-999;eor\n'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('produkt_zehn_min_ff_19991231_20000101_00044.txt', csv_text.encode('latin-1'))
    return buffer.getvalue()


class DwdSubdailyDownloaderTests(unittest.TestCase):
    def _station_metadata(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                'station_id': '00044',
                'gh_id': pd.NA,
                'begin_date': '1990-01-01T00:00Z',
                'end_date': '2025-12-31T00:00Z',
                'full_name': 'Grossenbrode',
                'longitude': 11.09,
                'latitude': 54.36,
                'elevation_m': 10.0,
            }
        ])

    def test_download_hourly_observations_country_de_with_canonical_elements(self) -> None:
        tu_zip = _build_hourly_tu_zip()
        ff_zip = _build_hourly_ff_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/hourly/air_temperature/historical/'):
                return _MockResponse(text='<a href="stundenwerte_TU_00044_19991231_20000101_hist.zip">tu</a>')
            if url.endswith('/hourly/wind/historical/'):
                return _MockResponse(text='<a href="stundenwerte_FF_00044_19991231_20000101_hist.zip">ff</a>')
            if url.endswith('stundenwerte_TU_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=tu_zip)
            if url.endswith('stundenwerte_FF_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=ff_zip)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='1hour',
            station_ids=['00044'],
            start='1999-12-31T22:00:00Z',
            end='2000-01-01T00:00:00Z',
            elements=['tas_mean', 'relative_humidity', 'wind_speed'],
        )

        with patch('weatherdownload.dwd_subdaily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DE', station_metadata=self._station_metadata())

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'dataset_scope', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['relative_humidity', 'tas_mean', 'wind_speed'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['FF', 'RF_TU', 'TT_TU'])
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['dataset_scope'].eq('historical').all())
        self.assertTrue(observations['resolution'].eq('1hour').all())
        self.assertIn(pd.Timestamp('1999-12-31T22:00:00Z'), observations['timestamp'].tolist())
        self.assertIn(pd.Timestamp('2000-01-01T00:00:00Z'), observations['timestamp'].tolist())

        t_mean = observations[observations['element'] == 'tas_mean'].reset_index(drop=True)
        rh = observations[observations['element'] == 'relative_humidity'].reset_index(drop=True)
        wind = observations[observations['element'] == 'wind_speed'].reset_index(drop=True)
        self.assertEqual(float(t_mean.loc[0, 'value']), 2.5)
        self.assertEqual(int(t_mean.loc[0, 'quality']), 1)
        self.assertEqual(float(rh.loc[1, 'value']), 82.0)
        self.assertTrue(pd.isna(wind.loc[1, 'value']))
        self.assertEqual(int(wind.loc[1, 'quality']), 2)

    def test_download_hourly_observations_country_de_accepts_raw_codes(self) -> None:
        tu_zip = _build_hourly_tu_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/hourly/air_temperature/historical/'):
                return _MockResponse(text='<a href="stundenwerte_TU_00044_19991231_20000101_hist.zip">tu</a>')
            if url.endswith('stundenwerte_TU_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=tu_zip)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='1hour',
            station_ids=['00044'],
            start='1999-12-31T22:00:00Z',
            end='2000-01-01T00:00:00Z',
            elements=['TT_TU'],
        )

        with patch('weatherdownload.dwd_subdaily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DE', station_metadata=self._station_metadata())

        self.assertEqual(list(observations['element'].unique()), ['tas_mean'])
        self.assertEqual(list(observations['element_raw'].unique()), ['TT_TU'])

    def test_download_hourly_observations_country_de_all_history_skips_explicit_range_requirement(self) -> None:
        tu_zip = _build_hourly_tu_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/hourly/air_temperature/historical/'):
                return _MockResponse(text='<a href="stundenwerte_TU_00044_19991231_20000101_hist.zip">tu</a>')
            if url.endswith('stundenwerte_TU_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=tu_zip)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='1hour',
            station_ids=['00044'],
            all_history=True,
            elements=['tas_mean'],
        )

        with patch('weatherdownload.dwd_subdaily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DE', station_metadata=self._station_metadata())

        self.assertEqual(len(observations), 2)
        self.assertEqual(observations['element'].tolist(), ['tas_mean', 'tas_mean'])
        self.assertEqual(observations['quality'].tolist(), [1, 2])

    def test_download_tenmin_observations_country_de_with_canonical_elements(self) -> None:
        tu_zip = _build_tenmin_tu_zip()
        ff_zip = _build_tenmin_ff_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/10_minutes/air_temperature/historical/'):
                return _MockResponse(text='<a href="10minutenwerte_TU_00044_19991231_20000101_hist.zip">tu</a>')
            if url.endswith('/10_minutes/wind/historical/'):
                return _MockResponse(text='<a href="10minutenwerte_wind_00044_19991231_20000101_hist.zip">ff</a>')
            if url.endswith('10minutenwerte_TU_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=tu_zip)
            if url.endswith('10minutenwerte_wind_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=ff_zip)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='10min',
            station_ids=['00044'],
            start='1999-12-31T22:50:00Z',
            end='2000-01-01T00:00:00Z',
            elements=['tas_mean', 'relative_humidity', 'wind_speed'],
        )

        with patch('weatherdownload.dwd_subdaily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DE', station_metadata=self._station_metadata())

        self.assertEqual(sorted(observations['element'].unique().tolist()), ['relative_humidity', 'tas_mean', 'wind_speed'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['FF_10', 'RF_10', 'TT_10'])
        self.assertTrue(observations['flag'].isna().all())
        self.assertIn(pd.Timestamp('1999-12-31T22:50:00Z'), observations['timestamp'].tolist())
        self.assertIn(pd.Timestamp('2000-01-01T00:00:00Z'), observations['timestamp'].tolist())

        t_mean = observations[observations['element'] == 'tas_mean'].reset_index(drop=True)
        wind = observations[observations['element'] == 'wind_speed'].reset_index(drop=True)
        self.assertEqual(int(t_mean.loc[0, 'quality']), 3)
        self.assertEqual(int(t_mean.loc[1, 'quality']), 4)
        self.assertTrue(pd.isna(wind.loc[1, 'value']))

    def test_download_tenmin_observations_country_de_accepts_raw_codes(self) -> None:
        tu_zip = _build_tenmin_tu_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/10_minutes/air_temperature/historical/'):
                return _MockResponse(text='<a href="10minutenwerte_TU_00044_19991231_20000101_hist.zip">tu</a>')
            if url.endswith('10minutenwerte_TU_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=tu_zip)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='10min',
            station_ids=['00044'],
            start='1999-12-31T22:50:00Z',
            end='2000-01-01T00:00:00Z',
            elements=['TT_10'],
        )

        with patch('weatherdownload.dwd_subdaily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DE', station_metadata=self._station_metadata())

        self.assertEqual(list(observations['element'].unique()), ['tas_mean'])
        self.assertEqual(list(observations['element_raw'].unique()), ['TT_10'])

    def test_download_tenmin_observations_country_de_all_history_skips_explicit_range_requirement(self) -> None:
        tu_zip = _build_tenmin_tu_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/10_minutes/air_temperature/historical/'):
                return _MockResponse(text='<a href="10minutenwerte_TU_00044_19991231_20000101_hist.zip">tu</a>')
            if url.endswith('10minutenwerte_TU_00044_19991231_20000101_hist.zip'):
                return _MockResponse(content=tu_zip)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='10min',
            station_ids=['00044'],
            all_history=True,
            elements=['tas_mean'],
        )

        with patch('weatherdownload.dwd_subdaily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DE', station_metadata=self._station_metadata())

        self.assertEqual(len(observations), 2)
        self.assertEqual(observations['element_raw'].tolist(), ['TT_10', 'TT_10'])
        self.assertEqual(observations['quality'].tolist(), [3, 4])


if __name__ == '__main__':
    unittest.main()
