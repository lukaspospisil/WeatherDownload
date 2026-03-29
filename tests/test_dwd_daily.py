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


def _build_sample_dwd_daily_zip() -> bytes:
    csv_text = (
        ' stations_id ; mess_datum ; QN_3 ;  FX;  FM; qn_4 ; RSK ;RSKF; SDK;SHK_TAG;  NM; VPM;  PM; tmk ; UPM; TXK; TNK; TGK;eor\n'
        '3;20240101;1;12.5;4.0;2;5.0;1;3.2;-999;6.0;8.1;1013.2;2.5;85.0;4.1;0.2;-1.0;eor\n'
        '3;20240102;1;-999;-999;2;0.0;0;0.0;-999;2.0;7.5;1011.0;1.0;88.0;2.0;-0.5;-2.0;eor\n'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('produkt_klima_tag_20240101_20240102_00003.txt', csv_text.encode('latin-1'))
    return buffer.getvalue()


class DwdDailyDownloaderTests(unittest.TestCase):
    def test_download_daily_observations_country_de(self) -> None:
        directory_html = '<a href="tageswerte_KL_00003_20240101_20240102_hist.zip">zip</a>'
        zip_bytes = _build_sample_dwd_daily_zip()
        station_metadata = pd.DataFrame([
            {
                'station_id': '00003',
                'gh_id': pd.NA,
                'begin_date': '1891-01-01T00:00Z',
                'end_date': '2024-12-31T00:00Z',
                'full_name': 'Aachen',
                'longitude': 6.0941,
                'latitude': 50.7827,
                'elevation_m': 202.0,
            }
        ])

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/daily/kl/historical/'):
                return _MockResponse(text=directory_html)
            if url.endswith('tageswerte_KL_00003_20240101_20240102_hist.zip'):
                return _MockResponse(content=zip_bytes)
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['00003'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation', 'wind_speed'],
        )

        with patch('weatherdownload.providers.de.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='DE', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'dataset_scope', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_mean', 'wind_speed'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['FM', 'RSK', 'TMK'])
        self.assertTrue(observations['station_id'].eq('00003').all())
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['dataset_scope'].eq('historical').all())
        self.assertTrue(observations['resolution'].eq('daily').all())

        tmk = observations[observations['element'] == 'tas_mean'].reset_index(drop=True)
        fm = observations[observations['element'] == 'wind_speed'].reset_index(drop=True)
        self.assertEqual(tmk.loc[0, 'observation_date'].isoformat(), '2024-01-01')
        self.assertEqual(float(tmk.loc[0, 'value']), 2.5)
        self.assertEqual(int(tmk.loc[0, 'quality']), 2)
        self.assertEqual(float(fm.loc[0, 'value']), 4.0)
        self.assertEqual(int(fm.loc[0, 'quality']), 1)
        self.assertTrue(pd.isna(fm.loc[1, 'value']))


if __name__ == '__main__':
    unittest.main()

