import importlib.util
import io
import json
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import ObservationQuery, download_observations, read_station_metadata
from weatherdownload.metadata import STATION_METADATA_COLUMNS


FAO_MODULE_PATH = Path('examples/workflows/download_fao.py')
FAO_SPEC = importlib.util.spec_from_file_location('download_fao_contract_example', FAO_MODULE_PATH)
download_fao = importlib.util.module_from_spec(FAO_SPEC)
sys.modules[FAO_SPEC.name] = download_fao
assert FAO_SPEC.loader is not None
FAO_SPEC.loader.exec_module(download_fao)

SAMPLE_META1_TEXT = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')
SAMPLE_GEOSPHERE_METADATA_TEXT = Path('tests/data/sample_geosphere_klima_v2_1d_metadata.json').read_text(encoding='utf-8')
SAMPLE_GEOSPHERE_CSV_TEXT = Path('tests/data/sample_geosphere_klima_v2_1d.csv').read_text(encoding='utf-8')
SAMPLE_GEOSPHERE_HOURLY_CSV_TEXT = Path('tests/data/sample_geosphere_klima_v2_1h.csv').read_text(encoding='utf-8')
SAMPLE_GEOSPHERE_TENMIN_CSV_TEXT = Path('tests/data/sample_geosphere_klima_v2_10min.csv').read_text(encoding='utf-8')
SAMPLE_BE_STATIONS_PATH = Path('tests/data/sample_be_aws_station.json')
SAMPLE_BE_DAILY_TEXT = Path('tests/data/sample_be_aws_1day.json').read_text(encoding='utf-8')
SAMPLE_BE_HOURLY_TEXT = Path('tests/data/sample_be_aws_1hour.json').read_text(encoding='utf-8')
SAMPLE_BE_TENMIN_TEXT = Path('tests/data/sample_be_aws_10min.json').read_text(encoding='utf-8')
SAMPLE_CH_STATIONS_PATH = Path('tests/data/sample_ch_meta_stations.csv')
SAMPLE_CH_ITEM_PATH = Path('tests/data/sample_ch_aig_item.json')
SAMPLE_CH_ITEM_ASSETS = json.loads(SAMPLE_CH_ITEM_PATH.read_text(encoding='utf-8'))['assets']
SAMPLE_CH_DAILY_HISTORICAL_PATH = Path('tests/data/sample_ch_aig_d_historical.csv')
SAMPLE_CH_DAILY_RECENT_PATH = Path('tests/data/sample_ch_aig_d_recent.csv')
SAMPLE_CH_HOURLY_HISTORICAL_PATH = Path('tests/data/sample_ch_aig_h_historical_2020_2029.csv')
SAMPLE_CH_HOURLY_RECENT_PATH = Path('tests/data/sample_ch_aig_h_recent.csv')
SAMPLE_CH_TENMIN_HISTORICAL_PATH = Path('tests/data/sample_ch_aig_t_historical_2020_2029.csv')
SAMPLE_CH_TENMIN_RECENT_PATH = Path('tests/data/sample_ch_aig_t_recent.csv')
SAMPLE_DK_STATIONS_PATH = Path('tests/data/sample_dk_dmi_stations.json')
SAMPLE_DK_DAILY_TEXT = Path('tests/data/sample_dk_dmi_daily.json').read_text(encoding='utf-8')
SAMPLE_DK_HOURLY_TEXT = Path('tests/data/sample_dk_dmi_hourly.json').read_text(encoding='utf-8')
SAMPLE_DK_TENMIN_TEXT = Path('tests/data/sample_dk_dmi_tenmin.json').read_text(encoding='utf-8')
SAMPLE_KNMI_STATIONS_PATH = Path('tests/data/sample_knmi_station_metadata.csv')
SAMPLE_PL_STATIONS_PATH = Path('tests/data/sample_pl_wykaz_stacji.csv')
SAMPLE_PL_METEO_COORDINATES_JSON = Path('tests/data/sample_pl_meteo_api.json').read_text(encoding='utf-8')
SAMPLE_PL_STATION_2025_CSV = Path('tests/data/sample_pl_synop_station_2025.csv').read_text(encoding='utf-8')
SAMPLE_PL_MONTH_2026_01_CSV = Path('tests/data/sample_pl_synop_month_2026_01.csv').read_text(encoding='utf-8')
SAMPLE_PL_HOURLY_STATION_2025_CSV = Path('tests/data/sample_pl_synop_hourly_station_2025.csv').read_text(encoding='utf-8')
SAMPLE_HU_STATIONS_PATH = Path('tests/data/sample_hu_station_meta_auto.csv')
SAMPLE_HU_HISTORICAL_INDEX_HTML = Path('tests/data/sample_hu_daily_historical_index.html').read_text(encoding='utf-8')
SAMPLE_HU_HISTORICAL_CSV = Path('tests/data/sample_hu_daily_hist_13704.csv').read_text(encoding='utf-8')
SAMPLE_HU_RECENT_CSV = Path('tests/data/sample_hu_daily_recent_13704.csv').read_text(encoding='utf-8')
SAMPLE_HU_HOURLY_HISTORICAL_INDEX_HTML = Path('tests/data/sample_hu_hourly_historical_index.html').read_text(encoding='utf-8')
SAMPLE_HU_HOURLY_HISTORICAL_CSV = Path('tests/data/sample_hu_hourly_hist_13704.csv').read_text(encoding='utf-8')
SAMPLE_HU_HOURLY_RECENT_CSV = Path('tests/data/sample_hu_hourly_recent_13704.csv').read_text(encoding='utf-8')
SAMPLE_HU_TENMIN_HISTORICAL_INDEX_HTML = Path('tests/data/sample_hu_tenmin_historical_index.html').read_text(encoding='utf-8')
SAMPLE_HU_TENMIN_HISTORICAL_CSV = Path('tests/data/sample_hu_tenmin_hist_13704.csv').read_text(encoding='utf-8')
SAMPLE_HU_TENMIN_RECENT_CSV = Path('tests/data/sample_hu_tenmin_recent_13704.csv').read_text(encoding='utf-8')
SAMPLE_SE_FIXTURE_DIR = Path('tests/data/smhi_se')
SAMPLE_SHMU_PAYLOAD_PATH = Path('tests/data/sample_shmu_kli_inter_2025-01.json')
SAMPLE_SHMU_PAYLOAD_TEXT = SAMPLE_SHMU_PAYLOAD_PATH.read_text(encoding='utf-8')
SAMPLE_SHMU_INDEX_HTML = Path('tests/data/sample_shmu_recent_daily_index.html').read_text(encoding='utf-8')
SAMPLE_SHMU_MONTH_INDEX_HTML = Path('tests/data/sample_shmu_recent_daily_month_index.html').read_text(encoding='utf-8')
SAMPLE_GHCND_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')
SAMPLE_GHCND_CA_DLY_TEXT = Path('tests/data/sample_ghcnd_CA000000001.dly').read_text(encoding='utf-8')
SAMPLE_GHCND_FI_DLY_TEXT = Path('tests/data/sample_ghcnd_FI000000001.dly').read_text(encoding='utf-8')
SAMPLE_GHCND_FR_DLY_TEXT = Path('tests/data/sample_ghcnd_FR000000001.dly').read_text(encoding='utf-8')
SAMPLE_GHCND_IT_DLY_TEXT = Path('tests/data/sample_ghcnd_IT000000001.dly').read_text(encoding='utf-8')
SAMPLE_GHCND_MX_DLY_TEXT = Path('tests/data/sample_ghcnd_MX000000001.dly').read_text(encoding='utf-8')
SAMPLE_GHCND_NO_DLY_TEXT = Path('tests/data/sample_ghcnd_NO000000001.dly').read_text(encoding='utf-8')
SAMPLE_GHCND_NZ_DLY_TEXT = Path('tests/data/sample_ghcnd_NZ000000001.dly').read_text(encoding='utf-8')
SAMPLE_GHCND_DLY_TEXT = Path('tests/data/sample_ghcnd_USC00000001.dly').read_text(encoding='utf-8')
SAMPLE_DWD_STATIONS = '''Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe
----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------
00003 18910101 20241231 202 50.7827 6.0941 Aachen Baden-W\xfcrttemberg Frei
00044 20070401 20241231 79 52.9336 8.2370 Alfhausen Niedersachsen Frei
'''.encode('latin-1')


class _MockTextResponse:
    def __init__(self, text: str | None = None, status_code: int = 200, content: bytes | None = None) -> None:
        self.text = text or ''
        self.status_code = status_code
        self.encoding = 'utf-8'
        self.content = content if content is not None else self.text.encode('utf-8')

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


def _build_sample_hu_zip(filename: str, csv_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, csv_text.encode('utf-8'))
    return buffer.getvalue()

def _build_sample_pl_zip(filename: str, csv_text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, csv_text.replace(chr(65279), '').encode('cp1250'))
    return buffer.getvalue()


def _read_station_metadata_fixture(country: str) -> pd.DataFrame:
    if country == 'CZ':
        return read_station_metadata(country='CZ', source_url='tests/data/sample_meta1.csv')
    if country == 'AT':
        with patch('weatherdownload.providers.at.metadata.requests.get', return_value=_MockTextResponse(SAMPLE_GEOSPHERE_METADATA_TEXT)):
            return read_station_metadata(country='AT')
    if country == 'BE':
        return read_station_metadata(country='BE', source_url=str(SAMPLE_BE_STATIONS_PATH))
    if country == 'CH':
        return read_station_metadata(country='CH', source_url=str(SAMPLE_CH_STATIONS_PATH))
    if country == 'DE':
        with patch('weatherdownload.providers.de.metadata.requests.get', return_value=_MockTextResponse(content=SAMPLE_DWD_STATIONS)):
            return read_station_metadata(country='DE')
    if country == 'DK':
        return read_station_metadata(country='DK', source_url=str(SAMPLE_DK_STATIONS_PATH))
    if country == 'HU':
        return read_station_metadata(country='HU', source_url=str(SAMPLE_HU_STATIONS_PATH))
    if country == 'NL':
        return read_station_metadata(country='NL', source_url=str(SAMPLE_KNMI_STATIONS_PATH))
    if country == 'PL':
        with patch('weatherdownload.providers.pl.metadata.requests.get', return_value=_MockTextResponse(SAMPLE_PL_METEO_COORDINATES_JSON)):
            return read_station_metadata(country='PL', source_url=str(SAMPLE_PL_STATIONS_PATH), timeout=5)

    if country == 'SE':
        return read_station_metadata(country='SE', source_url=str(SAMPLE_SE_FIXTURE_DIR))
    if country == 'SK':
        return read_station_metadata(country='SK', source_url=str(SAMPLE_SHMU_PAYLOAD_PATH))
    if country == 'CA':
        return read_station_metadata(country='CA', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    if country == 'FI':
        return read_station_metadata(country='FI', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    if country == 'FR':
        return read_station_metadata(country='FR', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    if country == 'IT':
        return read_station_metadata(country='IT', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    if country == 'MX':
        return read_station_metadata(country='MX', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    if country == 'NO':
        return read_station_metadata(country='NO', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    if country == 'NZ':
        return read_station_metadata(country='NZ', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    if country == 'US':
        return read_station_metadata(country='US', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
    raise AssertionError(f'unsupported test country: {country}')

def _download_daily_fixture(country: str) -> pd.DataFrame:
    if country == 'CZ':
        station_metadata = _read_station_metadata_fixture('CZ')
        query = ObservationQuery(country='CZ', dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date='1865-06-01', end_date='1865-06-03', elements=['tas_max'])
        sample_daily_csv = Path('tests/data/sample_daily_tma.csv').read_text(encoding='utf-8')
        with patch('weatherdownload.observations.download_daily_csv', return_value=sample_daily_csv):
            return download_observations(query, country='CZ', station_metadata=station_metadata)
    if country == 'AT':
        station_metadata = _read_station_metadata_fixture('AT')
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='daily', station_ids=['1'], start_date='2024-01-01', end_date='2024-01-03', elements=['tas_mean', 'precipitation'])
        with patch('weatherdownload.providers.at.daily.requests.get', return_value=_MockTextResponse(SAMPLE_GEOSPHERE_CSV_TEXT)):
            return download_observations(query, country='AT', station_metadata=station_metadata)
    if country == 'BE':
        station_metadata = _read_station_metadata_fixture('BE')
        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='daily', station_ids=['6414'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        with patch('weatherdownload.providers.be.daily.requests.get', return_value=_MockTextResponse(SAMPLE_BE_DAILY_TEXT)):
            return download_observations(query, country='BE', station_metadata=station_metadata)
    if country == 'CH':
        station_metadata = _read_station_metadata_fixture('CH')
        query = ObservationQuery(country='CH', dataset_scope='historical', resolution='daily', station_ids=['AIG'], start_date='2025-12-31', end_date='2026-01-02', elements=['tas_mean', 'pressure', 'precipitation'])

        def fake_get(url: str, timeout: int = 60):
            if url == 'https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn/items/aig':
                return _MockTextResponse(content=SAMPLE_CH_ITEM_PATH.read_bytes())
            if url == SAMPLE_CH_ITEM_ASSETS['ogd-smn_aig_d_historical.csv']['href']:
                return _MockTextResponse(content=SAMPLE_CH_DAILY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_CH_ITEM_ASSETS['ogd-smn_aig_d_recent.csv']['href']:
                return _MockTextResponse(content=SAMPLE_CH_DAILY_RECENT_PATH.read_bytes())
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.ch.daily.requests.get', side_effect=fake_get):
            return download_observations(query, country='CH', station_metadata=station_metadata)
    if country == 'DE':
        station_metadata = _read_station_metadata_fixture('DE')
        query = ObservationQuery(country='DE', dataset_scope='historical', resolution='daily', station_ids=['00003'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        directory_html = '<a href="tageswerte_KL_00003_20240101_20240102_hist.zip">zip</a>'
        zip_bytes = _build_sample_dwd_daily_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/daily/kl/historical/'):
                return _MockTextResponse(text=directory_html)
            if url.endswith('tageswerte_KL_00003_20240101_20240102_hist.zip'):
                return _MockTextResponse(content=zip_bytes)
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.de.daily.requests.get', side_effect=fake_get):
            return download_observations(query, country='DE', station_metadata=station_metadata)
    if country == 'DK':
        station_metadata = _read_station_metadata_fixture('DK')
        query = ObservationQuery(country='DK', dataset_scope='historical', resolution='daily', station_ids=['06180'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        sample_payload = json.loads(SAMPLE_DK_DAILY_TEXT)

        def fake_get(url, params=None, timeout=60):
            filtered = {
                'type': 'FeatureCollection',
                'features': [
                    feature for feature in sample_payload['features']
                    if feature['properties'].get('stationId') == params['stationId']
                    and feature['properties'].get('parameterId') == params['parameterId']
                ],
            }
            return _MockTextResponse(text=json.dumps(filtered))

        with patch('weatherdownload.providers.dk.daily.requests.get', side_effect=fake_get):
            return download_observations(query, country='DK', station_metadata=station_metadata)
    if country == 'HU':
        station_metadata = _read_station_metadata_fixture('HU')
        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='daily', station_ids=['13704'], start_date='2025-07-28', end_date='2026-01-02', elements=['tas_mean', 'precipitation'])
        historical_zip = _build_sample_hu_zip('HABP_1D_20050727_20251231_13704.csv', SAMPLE_HU_HISTORICAL_CSV)
        recent_zip = _build_sample_hu_zip('HABP_1D_20260101_20260328_13704.csv', SAMPLE_HU_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url.endswith('/daily/historical/'):
                return _MockTextResponse(text=SAMPLE_HU_HISTORICAL_INDEX_HTML)
            if url.endswith('HABP_1D_13704_20050727_20251231_hist.zip'):
                return _MockTextResponse(content=historical_zip)
            if url.endswith('HABP_1D_13704_akt.zip'):
                return _MockTextResponse(content=recent_zip)
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.hu.daily.requests.get', side_effect=fake_get):
            return download_observations(query, country='HU', station_metadata=station_metadata)
    if country == 'NL':
        station_metadata = _read_station_metadata_fixture('NL')
        query = ObservationQuery(country='NL', dataset_scope='historical', resolution='daily', station_ids=['0-20000-0-06260'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        parsed_payloads = iter([
            {
                'observation_date': pd.Timestamp('2024-01-01').date(),
                'stations': pd.DataFrame([
                    {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                    {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
                ]),
                'variables': {'TG': pd.Series([3.4, 5.6]), 'RH': pd.Series([1.2, 0.0])},
            },
            {
                'observation_date': pd.Timestamp('2024-01-02').date(),
                'stations': pd.DataFrame([
                    {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                    {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
                ]),
                'variables': {'TG': pd.Series([4.1, 6.2]), 'RH': pd.Series([0.5, 0.1])},
            },
        ])
        file_listing = {'files': [{'filename': 'daily-observations-20240101.nc'}, {'filename': 'daily-observations-20240102.nc'}]}

        with patch.dict('os.environ', {'WEATHERDOWNLOAD_KNMI_API_KEY': 'test-key'}, clear=False):
            with patch('weatherdownload.providers.nl.daily.list_knmi_files', return_value=file_listing):
                with patch('weatherdownload.providers.nl.daily.download_knmi_file_bytes', side_effect=[b'first', b'second']):
                    with patch('weatherdownload.providers.nl.daily.parse_knmi_daily_netcdf_bytes', side_effect=lambda payload: next(parsed_payloads)):
                        return download_observations(query, country='NL', station_metadata=station_metadata)
    if country == 'PL':
        station_metadata = _read_station_metadata_fixture('PL')
        query = ObservationQuery(country='PL', dataset_scope='historical', resolution='daily', station_ids=['00375'], start_date='2025-01-01', end_date='2026-01-02', elements=['tas_mean', 'precipitation'])
        station_zip = _build_sample_pl_zip('2025_375_s.csv', SAMPLE_PL_STATION_2025_CSV)
        month_zip = _build_sample_pl_zip('2026_01_s.csv', SAMPLE_PL_MONTH_2026_01_CSV)

        def fake_get(url, timeout=60):
            if url.endswith('/2025/2025_375_s.zip'):
                return _MockTextResponse(content=station_zip)
            if url.endswith('/2026/2026_01_s.zip'):
                return _MockTextResponse(content=month_zip)
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.pl.daily.requests.get', side_effect=fake_get):
            return download_observations(query, country='PL', station_metadata=station_metadata)

    if country == 'SE':
        station_metadata = _read_station_metadata_fixture('SE')
        query = ObservationQuery(country='SE', dataset_scope='historical', resolution='daily', station_ids=['98230'], start_date='1996-10-01', end_date='1996-10-02', elements=['tas_mean', 'precipitation'])

        def fake_get(url, timeout=60):
            if '/parameter/2/' in url:
                return _MockTextResponse((SAMPLE_SE_FIXTURE_DIR / 'daily_parameter_2.csv').read_text(encoding='utf-8'))
            if '/parameter/5/' in url:
                return _MockTextResponse((SAMPLE_SE_FIXTURE_DIR / 'daily_parameter_5.csv').read_text(encoding='utf-8'))
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.se.daily.requests.get', side_effect=fake_get):
            return download_observations(query, country='SE', station_metadata=station_metadata)
    if country == 'SK':
        station_metadata = _read_station_metadata_fixture('SK')
        query = ObservationQuery(country='SK', dataset_scope='recent', resolution='daily', station_ids=['11800'], start_date='2025-01-01', end_date='2025-01-02', elements=['tas_max', 'precipitation'])

        def fake_read_text(source: str, timeout: int) -> str:
            if source.endswith('/recent/data/daily/'):
                return SAMPLE_SHMU_INDEX_HTML
            if source.endswith('/recent/data/daily/2025-01/'):
                return SAMPLE_SHMU_MONTH_INDEX_HTML
            if source.endswith('kli-inter - 2025-01.json'):
                return SAMPLE_SHMU_PAYLOAD_TEXT
            raise AssertionError(f'unexpected source: {source}')

        with patch('weatherdownload.providers.sk.observations._read_text', side_effect=fake_read_text):
            return download_observations(query, country='SK', station_metadata=station_metadata)
    if country == 'CA':
        station_metadata = _read_station_metadata_fixture('CA')
        query = ObservationQuery(country='CA', dataset_scope='ghcnd', resolution='daily', station_ids=['CA000000001'], start_date='2020-06-01', end_date='2020-06-02', elements=['tas_max', 'precipitation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_CA_DLY_TEXT):
            return download_observations(query, country='CA', station_metadata=station_metadata)
    if country == 'FI':
        station_metadata = _read_station_metadata_fixture('FI')
        query = ObservationQuery(country='FI', dataset_scope='ghcnd', resolution='daily', station_ids=['FI000000001'], start_date='2020-08-01', end_date='2020-08-02', elements=['tas_max', 'precipitation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_FI_DLY_TEXT):
            return download_observations(query, country='FI', station_metadata=station_metadata)
    if country == 'FR':
        station_metadata = _read_station_metadata_fixture('FR')
        query = ObservationQuery(country='FR', dataset_scope='ghcnd', resolution='daily', station_ids=['FR000000001'], start_date='2020-10-01', end_date='2020-10-02', elements=['tas_max', 'precipitation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_FR_DLY_TEXT):
            return download_observations(query, country='FR', station_metadata=station_metadata)
    if country == 'IT':
        station_metadata = _read_station_metadata_fixture('IT')
        query = ObservationQuery(country='IT', dataset_scope='ghcnd', resolution='daily', station_ids=['IT000000001'], start_date='2020-11-01', end_date='2020-11-02', elements=['tas_max', 'precipitation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_IT_DLY_TEXT):
            return download_observations(query, country='IT', station_metadata=station_metadata)
    if country == 'MX':
        station_metadata = _read_station_metadata_fixture('MX')
        query = ObservationQuery(country='MX', dataset_scope='ghcnd', resolution='daily', station_ids=['MX000000001'], start_date='2020-07-01', end_date='2020-07-02', elements=['tas_max', 'precipitation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_MX_DLY_TEXT):
            return download_observations(query, country='MX', station_metadata=station_metadata)
    if country == 'NO':
        station_metadata = _read_station_metadata_fixture('NO')
        query = ObservationQuery(country='NO', dataset_scope='ghcnd', resolution='daily', station_ids=['NO000000001'], start_date='2020-09-01', end_date='2020-09-02', elements=['tas_max', 'precipitation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_NO_DLY_TEXT):
            return download_observations(query, country='NO', station_metadata=station_metadata)
    if country == 'NZ':
        station_metadata = _read_station_metadata_fixture('NZ')
        query = ObservationQuery(country='NZ', dataset_scope='ghcnd', resolution='daily', station_ids=['NZ000000001'], start_date='1998-01-01', end_date='1998-01-02', elements=['tas_max', 'precipitation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_NZ_DLY_TEXT):
            return download_observations(query, country='NZ', station_metadata=station_metadata)
    if country == 'US':
        station_metadata = _read_station_metadata_fixture('US')
        query = ObservationQuery(country='US', dataset_scope='ghcnd', resolution='daily', station_ids=['USC00000001'], start_date='2020-05-01', end_date='2020-05-02', elements=['open_water_evaporation'])
        with patch('weatherdownload.providers.ghcnd.observations._read_text', return_value=SAMPLE_GHCND_DLY_TEXT):
            return download_observations(query, country='US', station_metadata=station_metadata)
    raise AssertionError(f'unsupported test country: {country}')

def _download_hourly_fixture(country: str) -> pd.DataFrame:
    if country == 'AT':
        station_metadata = _read_station_metadata_fixture('AT')
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='1hour', station_ids=['1'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.providers.at.hourly.requests.get', return_value=_MockTextResponse(SAMPLE_GEOSPHERE_HOURLY_CSV_TEXT)):
            return download_observations(query, country='AT', station_metadata=station_metadata)
    if country == 'BE':
        station_metadata = _read_station_metadata_fixture('BE')
        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.providers.be.hourly.requests.get', return_value=_MockTextResponse(SAMPLE_BE_HOURLY_TEXT)):
            return download_observations(query, country='BE', station_metadata=station_metadata)
    if country == 'CH':
        station_metadata = _read_station_metadata_fixture('CH')
        query = ObservationQuery(country='CH', dataset_scope='historical', resolution='1hour', station_ids=['AIG'], start='2025-12-31T23:00:00Z', end='2026-01-01T01:00:00Z', elements=['tas_mean', 'pressure', 'vapour_pressure'])

        def fake_get(url: str, timeout: int = 60):
            if url == 'https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn/items/aig':
                return _MockTextResponse(content=SAMPLE_CH_ITEM_PATH.read_bytes())
            if url == SAMPLE_CH_ITEM_ASSETS['ogd-smn_aig_h_historical_2020-2029.csv']['href']:
                return _MockTextResponse(content=SAMPLE_CH_HOURLY_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_CH_ITEM_ASSETS['ogd-smn_aig_h_recent.csv']['href']:
                return _MockTextResponse(content=SAMPLE_CH_HOURLY_RECENT_PATH.read_bytes())
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.ch.hourly.requests.get', side_effect=fake_get):
            return download_observations(query, country='CH', station_metadata=station_metadata)
    if country == 'DK':
        station_metadata = _read_station_metadata_fixture('DK')
        query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        sample_payload = json.loads(SAMPLE_DK_HOURLY_TEXT)

        def fake_get(url, params=None, timeout=60):
            filtered = {
                'type': 'FeatureCollection',
                'features': [
                    feature for feature in sample_payload['features']
                    if feature['properties'].get('stationId') == params['stationId']
                    and feature['properties'].get('parameterId') == params['parameterId']
                ],
            }
            return _MockTextResponse(text=json.dumps(filtered))

        with patch('weatherdownload.providers.dk.hourly.requests.get', side_effect=fake_get):
            return download_observations(query, country='DK', station_metadata=station_metadata)
    if country == 'HU':
        station_metadata = _read_station_metadata_fixture('HU')
        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='1hour', station_ids=['13704'], start='2025-12-31T23:00:00Z', end='2026-01-01T00:00:00Z', elements=['tas_mean', 'pressure'])
        historical_zip = _build_sample_hu_zip('HABP_1H_20020101_20251231_13704.csv', SAMPLE_HU_HOURLY_HISTORICAL_CSV)
        recent_zip = _build_sample_hu_zip('HABP_1H_20260101_20260329_13704.csv', SAMPLE_HU_HOURLY_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url.endswith('/hourly/historical/'):
                return _MockTextResponse(text=SAMPLE_HU_HOURLY_HISTORICAL_INDEX_HTML)
            if url.endswith('HABP_1H_13704_20020101_20251231_hist.zip'):
                return _MockTextResponse(content=historical_zip)
            if url.endswith('HABP_1H_13704_akt.zip'):
                return _MockTextResponse(content=recent_zip)
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.hu.hourly.requests.get', side_effect=fake_get):
            return download_observations(query, country='HU', station_metadata=station_metadata)
    if country == 'PL':
        station_metadata = _read_station_metadata_fixture('PL')
        query = ObservationQuery(country='PL', dataset_scope='historical', resolution='1hour', station_ids=['00375'], start='2025-01-01T00:00:00Z', end='2025-01-01T01:00:00Z', elements=['tas_mean', 'pressure'])
        hourly_zip = _build_sample_pl_zip('2025_375_s.csv', SAMPLE_PL_HOURLY_STATION_2025_CSV)

        def fake_get(url, timeout=60):
            if url.endswith('/2025/2025_375_s.zip'):
                return _MockTextResponse(content=hourly_zip)
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.pl.hourly.requests.get', side_effect=fake_get):
            return download_observations(query, country='PL', station_metadata=station_metadata)

    if country == 'SE':
        station_metadata = _read_station_metadata_fixture('SE')
        query = ObservationQuery(country='SE', dataset_scope='historical', resolution='1hour', station_ids=['98230'], start='2012-11-29T11:00:00Z', end='2012-11-29T12:00:00Z', elements=['tas_mean', 'pressure'])

        def fake_get(url, timeout=60):
            if '/parameter/1/' in url:
                return _MockTextResponse((SAMPLE_SE_FIXTURE_DIR / 'hourly_parameter_1.csv').read_text(encoding='utf-8'))
            if '/parameter/9/' in url:
                return _MockTextResponse((SAMPLE_SE_FIXTURE_DIR / 'hourly_parameter_9.csv').read_text(encoding='utf-8'))
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.se.hourly.requests.get', side_effect=fake_get):
            return download_observations(query, country='SE', station_metadata=station_metadata)
    raise AssertionError(f'unsupported test country: {country}')


def _download_tenmin_fixture(country: str) -> pd.DataFrame:
    if country == 'AT':
        station_metadata = _read_station_metadata_fixture('AT')
        query = ObservationQuery(country='AT', dataset_scope='historical', resolution='10min', station_ids=['1'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.providers.at.tenmin.requests.get', return_value=_MockTextResponse(SAMPLE_GEOSPHERE_TENMIN_CSV_TEXT)):
            return download_observations(query, country='AT', station_metadata=station_metadata)
    if country == 'BE':
        station_metadata = _read_station_metadata_fixture('BE')
        query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        with patch('weatherdownload.providers.be.tenmin.requests.get', return_value=_MockTextResponse(SAMPLE_BE_TENMIN_TEXT)):
            return download_observations(query, country='BE', station_metadata=station_metadata)
    if country == 'CH':
        station_metadata = _read_station_metadata_fixture('CH')
        query = ObservationQuery(country='CH', dataset_scope='historical', resolution='10min', station_ids=['AIG'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:10:00Z', elements=['tas_mean', 'pressure', 'wind_speed_max'])

        def fake_get(url: str, timeout: int = 60):
            if url == 'https://data.geo.admin.ch/api/stac/v1/collections/ch.meteoschweiz.ogd-smn/items/aig':
                return _MockTextResponse(content=SAMPLE_CH_ITEM_PATH.read_bytes())
            if url == SAMPLE_CH_ITEM_ASSETS['ogd-smn_aig_t_historical_2020-2029.csv']['href']:
                return _MockTextResponse(content=SAMPLE_CH_TENMIN_HISTORICAL_PATH.read_bytes())
            if url == SAMPLE_CH_ITEM_ASSETS['ogd-smn_aig_t_recent.csv']['href']:
                return _MockTextResponse(content=SAMPLE_CH_TENMIN_RECENT_PATH.read_bytes())
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.ch.tenmin.requests.get', side_effect=fake_get):
            return download_observations(query, country='CH', station_metadata=station_metadata)
    if country == 'DK':
        station_metadata = _read_station_metadata_fixture('DK')
        query = ObservationQuery(country='DK', dataset_scope='historical', resolution='10min', station_ids=['06180'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        sample_payload = json.loads(SAMPLE_DK_TENMIN_TEXT)

        def fake_get(url, params=None, timeout=60):
            filtered = {
                'type': 'FeatureCollection',
                'features': [
                    feature for feature in sample_payload['features']
                    if feature['properties'].get('stationId') == params['stationId']
                    and feature['properties'].get('parameterId') == params['parameterId']
                ],
            }
            return _MockTextResponse(text=json.dumps(filtered))

        with patch('weatherdownload.providers.dk.tenmin.requests.get', side_effect=fake_get):
            return download_observations(query, country='DK', station_metadata=station_metadata)
    if country == 'HU':
        station_metadata = _read_station_metadata_fixture('HU')
        query = ObservationQuery(country='HU', dataset_scope='historical', resolution='10min', station_ids=['13704'], start='2025-12-31T23:50:00Z', end='2026-01-01T00:00:00Z', elements=['tas_mean', 'pressure'])
        historical_zip = _build_sample_hu_zip('HABP_10M_20020101_20251231_13704.csv', SAMPLE_HU_TENMIN_HISTORICAL_CSV)
        recent_zip = _build_sample_hu_zip('HABP_10M_20260101_20260329_13704.csv', SAMPLE_HU_TENMIN_RECENT_CSV)

        def fake_get(url, timeout=60):
            if url.endswith('/10_minutes/historical/'):
                return _MockTextResponse(text=SAMPLE_HU_TENMIN_HISTORICAL_INDEX_HTML)
            if url.endswith('HABP_10M_13704_20020101_20251231_hist.zip'):
                return _MockTextResponse(content=historical_zip)
            if url.endswith('HABP_10M_13704_akt.zip'):
                return _MockTextResponse(content=recent_zip)
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.providers.hu.tenmin.requests.get', side_effect=fake_get):
            return download_observations(query, country='HU', station_metadata=station_metadata)
    raise AssertionError(f'unsupported test country: {country}')

def test_read_station_metadata_contract_is_stable_across_countries() -> None:
    expected_station_ids = {
        'AT': ['1', '2'],
        'BE': ['6414', '6438'],
        'CA': ['CA000000001', 'CA000000002'],
        'CH': ['ABO', 'AEG', 'AIG', 'ALT', 'AND'],
        'CZ': ['0-20000-0-11406', '0-20000-0-11414'],
        'DE': ['00003', '00044'],
        'DK': ['06030', '06180'],
        'FI': ['FI000000001', 'FI000000002'],
        'FR': ['FR000000001', 'FR000000002'],
        'HU': ['13704', '13704', '13711'],
        'IT': ['IT000000001', 'IT000000002'],
        'MX': ['MX000000001', 'MX000000002'],
        'NL': ['0-20000-0-06260', '0-20000-0-06310'],
        'NO': ['NO000000001', 'NO000000002'],
        'NZ': ['NZ000000001', 'NZ000000002'],
        'PL': ['00375', '00400', '00600'],
        'SE': ['98230'],
        'SK': ['11800', '11999'],
        'US': ['USC00000001', 'USC00000002', 'USC00000003'],
    }

    for country in ['AT', 'BE', 'CA', 'CH', 'CZ', 'DE', 'DK', 'FI', 'FR', 'HU', 'IT', 'MX', 'NL', 'NO', 'NZ', 'PL', 'SE', 'SK', 'US']:
        stations = _read_station_metadata_fixture(country)
        assert list(stations.columns) == STATION_METADATA_COLUMNS
        actual_station_ids = stations['station_id'].tolist()
        if country == 'CH':
            assert actual_station_ids[:5] == expected_station_ids[country]
        else:
            assert actual_station_ids == expected_station_ids[country]
        assert stations['station_id'].is_monotonic_increasing


def test_daily_download_contract_is_stable_across_supported_countries() -> None:
    expected_columns = ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'dataset_scope', 'resolution']
    expected_dataset_scopes = {'AT': 'historical', 'BE': 'historical', 'CA': 'ghcnd', 'CH': 'historical', 'CZ': 'historical_csv', 'DE': 'historical', 'DK': 'historical', 'FI': 'ghcnd', 'FR': 'ghcnd', 'HU': 'historical', 'IT': 'ghcnd', 'MX': 'ghcnd', 'NL': 'historical', 'NO': 'ghcnd', 'NZ': 'ghcnd', 'PL': 'historical', 'SE': 'historical', 'SK': 'recent', 'US': 'ghcnd'}

    for country in ['AT', 'BE', 'CA', 'CH', 'CZ', 'DE', 'DK', 'FI', 'FR', 'HU', 'IT', 'MX', 'NL', 'NO', 'NZ', 'PL', 'SE', 'SK', 'US']:
        observations = _download_daily_fixture(country)
        assert list(observations.columns) == expected_columns
        assert observations['element'].str.match(r'^[a-z0-9_]+$').all()
        assert observations['element_raw'].notna().all()
        assert observations['observation_date'].map(lambda value: hasattr(value, 'isoformat')).all()
        assert observations['dataset_scope'].eq(expected_dataset_scopes[country]).all()
        assert observations['resolution'].eq('daily').all()

        if country in {'AT', 'BE', 'CA', 'CH', 'DE', 'DK', 'FI', 'FR', 'HU', 'IT', 'MX', 'NL', 'NO', 'NZ', 'SE', 'SK', 'US'}:
            assert observations['gh_id'].isna().all()
        elif country == 'PL':
            assert observations['gh_id'].notna().all()
            assert observations['gh_id'].str.isnumeric().all()

        if country == 'BE':
            assert observations['flag'].notna().all()
            assert observations['flag'].str.startswith('{"validated"').all()
        if country == 'CH':
            assert observations['flag'].isna().all()
        if country == 'DK':
            assert observations['flag'].notna().all()
            assert observations['flag'].str.contains('qcStatus').all()
            assert observations['flag'].str.contains('validity').all()
        if country == 'US':
            assert observations['flag'].notna().all()
            assert observations['flag'].str.contains('source_flag').all()
        if country == 'SE':
            assert observations['flag'].notna().all()
            assert set(observations['flag'].dropna().unique()) <= {'G', 'Y'}
        if country == 'AT':
            assert observations['quality'].notna().all()
            assert set(observations['quality'].dropna().astype(str).unique()) <= {'20', '21', '22'}
        elif country == 'CZ':
            assert observations['quality'].notna().all()
            assert set(observations['quality'].dropna().astype(str).unique()) <= {'0', '1'}
        elif country == 'DE':
            assert observations['quality'].notna().all()
            assert set(observations['quality'].dropna().astype(str).unique()) <= {'2'}
        elif country == 'US':
            assert set(observations['quality'].dropna().astype(str).unique()) <= {'X'}
        elif country in {'CA', 'FI', 'FR', 'IT', 'MX', 'NO', 'NZ'}:
            assert observations['quality'].isna().all()
        else:
            assert observations['quality'].isna().all()
        assert str(observations['quality'].dtype) in {'Int64', 'object'}


def _assert_subdaily_contract(observations: pd.DataFrame, resolution: str, gh_expected: str, flag_mode: str) -> None:
    expected_columns = ['station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'dataset_scope', 'resolution']
    assert list(observations.columns) == expected_columns
    assert observations['element'].str.match(r'^[a-z0-9_]+$').all()
    assert observations['element_raw'].notna().all()
    assert observations['timestamp'].map(lambda value: hasattr(value, 'isoformat')).all()
    assert observations['dataset_scope'].eq('historical').all()
    assert observations['resolution'].eq(resolution).all()
    if gh_expected == 'null':
        assert observations['gh_id'].isna().all()
    elif gh_expected == 'imgw':
        assert observations['gh_id'].notna().all()
        assert observations['gh_id'].str.isnumeric().all()
    else:
        assert observations['gh_id'].notna().all()
        assert observations['gh_id'].str.startswith('0-20000-0-').all()
    if flag_mode == 'null':
        assert observations['flag'].isna().all()
    elif flag_mode == 'be':
        assert observations['flag'].notna().all()
        assert observations['flag'].str.startswith('{"validated"').all()
    elif flag_mode == 'dk':
        assert observations['flag'].notna().all()
        assert observations['flag'].str.contains('qcStatus').all()
        assert observations['flag'].str.contains('validity').all()
    elif flag_mode == 'se':
        assert observations['flag'].notna().all()
        assert set(observations['flag'].dropna().unique()) <= {'G', 'Y'}
    elif flag_mode == 'at_hourly':
        assert observations['flag'].notna().all()
        assert set(observations['flag'].dropna().unique()) <= {'20', '21', '22'}
    elif flag_mode == 'at_tenmin':
        assert observations['flag'].notna().all()
        assert set(observations['flag'].dropna().unique()) <= {'12'}
    assert observations['quality'].isna().all()
    assert str(observations['quality'].dtype) == 'Int64'

def test_hourly_download_contract_is_stable_for_supported_austria_path() -> None:
    _assert_subdaily_contract(_download_hourly_fixture('AT'), '1hour', 'null', 'at_hourly')


def test_hourly_download_contract_is_stable_for_supported_belgium_path() -> None:
    _assert_subdaily_contract(_download_hourly_fixture('BE'), '1hour', 'null', 'be')


def test_hourly_download_contract_is_stable_for_supported_switzerland_path() -> None:
    _assert_subdaily_contract(_download_hourly_fixture('CH'), '1hour', 'null', 'null')


def test_hourly_download_contract_is_stable_for_supported_denmark_path() -> None:
    _assert_subdaily_contract(_download_hourly_fixture('DK'), '1hour', 'null', 'dk')


def test_hourly_download_contract_is_stable_for_supported_hungary_path() -> None:
    _assert_subdaily_contract(_download_hourly_fixture('HU'), '1hour', 'null', 'null')


def test_hourly_download_contract_is_stable_for_supported_poland_path() -> None:
    _assert_subdaily_contract(_download_hourly_fixture('PL'), '1hour', 'imgw', 'null')


def test_hourly_download_contract_is_stable_for_supported_sweden_path() -> None:
    _assert_subdaily_contract(_download_hourly_fixture('SE'), '1hour', 'null', 'se')


def test_tenmin_download_contract_is_stable_for_supported_austria_path() -> None:
    _assert_subdaily_contract(_download_tenmin_fixture('AT'), '10min', 'null', 'at_tenmin')


def test_tenmin_download_contract_is_stable_for_supported_belgium_path() -> None:
    _assert_subdaily_contract(_download_tenmin_fixture('BE'), '10min', 'null', 'be')


def test_tenmin_download_contract_is_stable_for_supported_switzerland_path() -> None:
    _assert_subdaily_contract(_download_tenmin_fixture('CH'), '10min', 'null', 'null')


def test_tenmin_download_contract_is_stable_for_supported_denmark_path() -> None:
    _assert_subdaily_contract(_download_tenmin_fixture('DK'), '10min', 'null', 'null')


def test_tenmin_download_contract_is_stable_for_supported_hungary_path() -> None:
    _assert_subdaily_contract(_download_tenmin_fixture('HU'), '10min', 'null', 'null')

def test_download_fao_bundle_shape_is_stable_across_supported_fao_countries() -> None:
    expected_data_info_keys = {'created_at', 'dataset_type', 'source', 'country', 'elements', 'provider_element_mapping', 'min_complete_days', 'num_stations'}
    expected_station_columns = ['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'num_complete_days', 'first_complete_date', 'last_complete_date']
    expected_series_columns = ['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'date', *download_fao.FINAL_SERIES_COLUMNS]

    for country in ['CZ', 'DE', 'AT', 'BE', 'CH', 'DK', 'HU', 'PL', 'NL', 'SE']:
        config = download_fao.get_fao_country_config(country)
        station_rows = [{'station_id': 'TEST1', 'full_name': 'Test Station', 'latitude': 50.0, 'longitude': 14.0, 'elevation_m': 250.0, 'num_complete_days': 2, 'first_complete_date': '2024-01-01', 'last_complete_date': '2024-01-02'}]
        series = [{'station_id': 'TEST1', 'full_name': 'Test Station', 'latitude': 50.0, 'longitude': 14.0, 'elevation_m': 250.0, 'date': ['2024-01-01', '2024-01-02'], 'tas_mean': [1.0, 2.0], 'tas_max': [3.0, 4.0], 'tas_min': [-1.0, 0.0], 'wind_speed': [pd.NA, pd.NA] if country in {'PL', 'SE'} else [2.5, 3.0], 'vapour_pressure': [pd.NA, pd.NA] if country in {'AT', 'BE', 'DK', 'HU', 'NL', 'PL', 'SE'} else [7.0, 8.0], 'sunshine_duration': [pd.NA, pd.NA] if country == 'SE' else [0.5, 0.8]}]

        data_info = download_fao.build_data_info(config, station_rows, min_complete_days=3650)
        station_table = download_fao.build_station_table(station_rows)
        series_table = download_fao.build_series_table(series)

        assert expected_data_info_keys.issubset(set(data_info))
        assert data_info['country'] == country
        assert data_info['elements'] == download_fao.FINAL_SERIES_COLUMNS
        assert list(data_info['provider_element_mapping']) == download_fao.FINAL_SERIES_COLUMNS
        assert list(station_table.columns) == expected_station_columns
        assert list(series_table.columns) == expected_series_columns
        assert list(series_table['date']) == ['2024-01-01', '2024-01-02']
        assert list(series_table['station_id']) == ['TEST1', 'TEST1']

        if country in {'AT', 'BE', 'DK', 'HU', 'NL', 'PL', 'SE'}:
            assert series_table['vapour_pressure'].isna().all()
            assert data_info['provider_element_mapping']['vapour_pressure']['status'] == 'unavailable'
            if country in {'PL', 'SE'}:
                assert series_table['wind_speed'].isna().all()
                assert data_info['provider_element_mapping']['wind_speed']['status'] == 'unavailable'
        else:
            assert series_table['vapour_pressure'].notna().all()
            assert data_info['provider_element_mapping']['vapour_pressure']['status'] == 'observed'


def test_download_fao_bundle_shape_marks_sweden_missing_fields_as_unavailable() -> None:
    config = download_fao.get_fao_country_config('SE')
    station_rows = [{'station_id': 'TEST1', 'full_name': 'Test Station', 'latitude': 50.0, 'longitude': 14.0, 'elevation_m': 250.0, 'num_complete_days': 2, 'first_complete_date': '2024-01-01', 'last_complete_date': '2024-01-02'}]
    series = [{'station_id': 'TEST1', 'full_name': 'Test Station', 'latitude': 50.0, 'longitude': 14.0, 'elevation_m': 250.0, 'date': ['2024-01-01', '2024-01-02'], 'tas_mean': [1.0, 2.0], 'tas_max': [3.0, 4.0], 'tas_min': [-1.0, 0.0], 'wind_speed': [pd.NA, pd.NA], 'vapour_pressure': [pd.NA, pd.NA], 'sunshine_duration': [pd.NA, pd.NA]}]

    data_info = download_fao.build_data_info(config, station_rows, min_complete_days=3650)
    series_table = download_fao.build_series_table(series)

    assert series_table['wind_speed'].isna().all()
    assert series_table['vapour_pressure'].isna().all()
    assert series_table['sunshine_duration'].isna().all()
    assert data_info['provider_element_mapping']['wind_speed']['status'] == 'unavailable'
    assert data_info['provider_element_mapping']['vapour_pressure']['status'] == 'unavailable'
    assert data_info['provider_element_mapping']['sunshine_duration']['status'] == 'unavailable'



























