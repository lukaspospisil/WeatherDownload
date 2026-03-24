import importlib.util
import io
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import ObservationQuery, download_observations, read_station_metadata
from weatherdownload.metadata import STATION_METADATA_COLUMNS


FAO_MODULE_PATH = Path('examples/download_fao.py')
FAO_SPEC = importlib.util.spec_from_file_location('download_fao_contract_example', FAO_MODULE_PATH)
download_fao = importlib.util.module_from_spec(FAO_SPEC)
sys.modules[FAO_SPEC.name] = download_fao
assert FAO_SPEC.loader is not None
FAO_SPEC.loader.exec_module(download_fao)

SAMPLE_META1_PATH = Path('tests/data/sample_meta1.csv')
SAMPLE_META1_TEXT = SAMPLE_META1_PATH.read_text(encoding='utf-8')
SAMPLE_GEOSPHERE_METADATA_PATH = Path('tests/data/sample_geosphere_klima_v2_1d_metadata.json')
SAMPLE_GEOSPHERE_METADATA_TEXT = SAMPLE_GEOSPHERE_METADATA_PATH.read_text(encoding='utf-8')
SAMPLE_GEOSPHERE_CSV_PATH = Path('tests/data/sample_geosphere_klima_v2_1d.csv')
SAMPLE_GEOSPHERE_CSV_TEXT = SAMPLE_GEOSPHERE_CSV_PATH.read_text(encoding='utf-8')
SAMPLE_KNMI_STATIONS_PATH = Path('tests/data/sample_knmi_station_metadata.csv')
SAMPLE_SHMU_PAYLOAD_PATH = Path('tests/data/sample_shmu_kli_inter_2025-01.json')
SAMPLE_SHMU_PAYLOAD_TEXT = SAMPLE_SHMU_PAYLOAD_PATH.read_text(encoding='utf-8')
SAMPLE_SHMU_INDEX_HTML = Path('tests/data/sample_shmu_recent_daily_index.html').read_text(encoding='utf-8')
SAMPLE_SHMU_MONTH_INDEX_HTML = Path('tests/data/sample_shmu_recent_daily_month_index.html').read_text(encoding='utf-8')
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


def _read_station_metadata_fixture(country: str) -> pd.DataFrame:
    if country == 'CZ':
        with patch('weatherdownload.metadata.requests.get', return_value=_MockTextResponse(SAMPLE_META1_TEXT)):
            return read_station_metadata(country='CZ')
    if country == 'AT':
        with patch('weatherdownload.geosphere_metadata.requests.get', return_value=_MockTextResponse(SAMPLE_GEOSPHERE_METADATA_TEXT)):
            return read_station_metadata(country='AT')
    if country == 'DE':
        with patch('weatherdownload.dwd_metadata.requests.get', return_value=_MockTextResponse(content=SAMPLE_DWD_STATIONS)):
            return read_station_metadata(country='DE')
    if country == 'NL':
        return read_station_metadata(country='NL', source_url=str(SAMPLE_KNMI_STATIONS_PATH))
    if country == 'SK':
        return read_station_metadata(country='SK', source_url=str(SAMPLE_SHMU_PAYLOAD_PATH))
    raise AssertionError(f'unsupported test country: {country}')


def _download_daily_fixture(country: str) -> pd.DataFrame:
    if country == 'CZ':
        station_metadata = _read_station_metadata_fixture('CZ')
        query = ObservationQuery(
            country='CZ',
            dataset_scope='historical_csv',
            resolution='daily',
            station_ids=['0-20000-0-11406'],
            start_date='1865-06-01',
            end_date='1865-06-03',
            elements=['tas_max'],
        )
        sample_daily_csv = Path('tests/data/sample_daily_tma.csv').read_text(encoding='utf-8')
        with patch('weatherdownload.observations.download_daily_csv', return_value=sample_daily_csv):
            return download_observations(query, country='CZ', station_metadata=station_metadata)
    if country == 'AT':
        station_metadata = _read_station_metadata_fixture('AT')
        query = ObservationQuery(
            country='AT',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['1'],
            start_date='2024-01-01',
            end_date='2024-01-03',
            elements=['tas_mean', 'precipitation'],
        )
        with patch('weatherdownload.geosphere_daily.requests.get', return_value=_MockTextResponse(SAMPLE_GEOSPHERE_CSV_TEXT)):
            return download_observations(query, country='AT', station_metadata=station_metadata)
    if country == 'DE':
        station_metadata = _read_station_metadata_fixture('DE')
        query = ObservationQuery(
            country='DE',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['00003'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation'],
        )
        directory_html = '<a href="tageswerte_KL_00003_20240101_20240102_hist.zip">zip</a>'
        zip_bytes = _build_sample_dwd_daily_zip()

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('/daily/kl/historical/'):
                return _MockTextResponse(text=directory_html)
            if url.endswith('tageswerte_KL_00003_20240101_20240102_hist.zip'):
                return _MockTextResponse(content=zip_bytes)
            raise AssertionError(f'unexpected URL: {url}')

        with patch('weatherdownload.dwd_daily.requests.get', side_effect=fake_get):
            return download_observations(query, country='DE', station_metadata=station_metadata)
    if country == 'NL':
        station_metadata = _read_station_metadata_fixture('NL')
        query = ObservationQuery(
            country='NL',
            dataset_scope='historical',
            resolution='daily',
            station_ids=['0-20000-0-06260'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation'],
        )
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
            with patch('weatherdownload.knmi_daily.list_knmi_files', return_value=file_listing):
                with patch('weatherdownload.knmi_daily.download_knmi_file_bytes', side_effect=[b'first', b'second']):
                    with patch('weatherdownload.knmi_daily.parse_knmi_daily_netcdf_bytes', side_effect=lambda payload: next(parsed_payloads)):
                        return download_observations(query, country='NL', station_metadata=station_metadata)
    if country == 'SK':
        station_metadata = _read_station_metadata_fixture('SK')
        query = ObservationQuery(
            country='SK',
            dataset_scope='recent',
            resolution='daily',
            station_ids=['11800'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_max', 'precipitation'],
        )

        def fake_read_text(source: str, timeout: int) -> str:
            if source.endswith('/recent/data/daily/'):
                return SAMPLE_SHMU_INDEX_HTML
            if source.endswith('/recent/data/daily/2025-01/'):
                return SAMPLE_SHMU_MONTH_INDEX_HTML
            if source.endswith('kli-inter - 2025-01.json'):
                return SAMPLE_SHMU_PAYLOAD_TEXT
            raise AssertionError(f'unexpected source: {source}')

        with patch('weatherdownload.shmu_observations._read_text', side_effect=fake_read_text):
            return download_observations(query, country='SK', station_metadata=station_metadata)
    raise AssertionError(f'unsupported test country: {country}')


def test_read_station_metadata_contract_is_stable_across_countries() -> None:
    expected_station_ids = {
        'AT': ['1', '2'],
        'CZ': ['0-20000-0-11406', '0-20000-0-11414'],
        'DE': ['00003', '00044'],
        'NL': ['0-20000-0-06260', '0-20000-0-06310'],
        'SK': ['11800', '11999'],
    }

    for country in ['AT', 'CZ', 'DE', 'NL', 'SK']:
        stations = _read_station_metadata_fixture(country)
        assert list(stations.columns) == STATION_METADATA_COLUMNS
        assert stations['station_id'].tolist() == expected_station_ids[country]
        assert stations['station_id'].is_monotonic_increasing


def test_daily_download_contract_is_stable_across_supported_countries() -> None:
    expected_columns = [
        'station_id',
        'gh_id',
        'element',
        'element_raw',
        'observation_date',
        'time_function',
        'value',
        'flag',
        'quality',
        'dataset_scope',
        'resolution',
    ]
    expected_dataset_scopes = {
        'AT': 'historical',
        'CZ': 'historical_csv',
        'DE': 'historical',
        'NL': 'historical',
        'SK': 'recent',
    }

    for country in ['AT', 'CZ', 'DE', 'NL', 'SK']:
        observations = _download_daily_fixture(country)
        assert list(observations.columns) == expected_columns
        assert observations['element'].str.match(r'^[a-z0-9_]+$').all()
        assert observations['element_raw'].notna().all()
        assert observations['observation_date'].map(lambda value: hasattr(value, 'isoformat')).all()
        assert observations['dataset_scope'].eq(expected_dataset_scopes[country]).all()
        assert observations['resolution'].eq('daily').all()

        if country in {'AT', 'DE', 'NL', 'SK'}:
            assert observations['gh_id'].isna().all()


def test_download_fao_bundle_shape_is_stable_across_supported_fao_countries() -> None:
    expected_data_info_keys = {
        'created_at',
        'dataset_type',
        'source',
        'country',
        'elements',
        'provider_element_mapping',
        'min_complete_days',
        'num_stations',
    }
    expected_station_columns = [
        'station_id',
        'full_name',
        'latitude',
        'longitude',
        'elevation_m',
        'num_complete_days',
        'first_complete_date',
        'last_complete_date',
    ]
    expected_series_columns = [
        'station_id',
        'full_name',
        'latitude',
        'longitude',
        'elevation_m',
        'date',
        *download_fao.FINAL_SERIES_COLUMNS,
    ]

    for country in ['CZ', 'DE', 'AT']:
        config = download_fao.get_fao_country_config(country)
        station_rows = [{
            'station_id': 'TEST1',
            'full_name': 'Test Station',
            'latitude': 50.0,
            'longitude': 14.0,
            'elevation_m': 250.0,
            'num_complete_days': 2,
            'first_complete_date': '2024-01-01',
            'last_complete_date': '2024-01-02',
        }]
        series = [{
            'station_id': 'TEST1',
            'full_name': 'Test Station',
            'latitude': 50.0,
            'longitude': 14.0,
            'elevation_m': 250.0,
            'date': ['2024-01-01', '2024-01-02'],
            'tas_mean': [1.0, 2.0],
            'tas_max': [3.0, 4.0],
            'tas_min': [-1.0, 0.0],
            'wind_speed': [2.5, 3.0],
            'vapour_pressure': [pd.NA, pd.NA] if country == 'AT' else [7.0, 8.0],
            'sunshine_duration': [0.5, 0.8],
        }]

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

        if country == 'AT':
            assert series_table['vapour_pressure'].isna().all()
            assert data_info['provider_element_mapping']['vapour_pressure']['status'] == 'unavailable'
        else:
            assert series_table['vapour_pressure'].notna().all()
            assert data_info['provider_element_mapping']['vapour_pressure']['status'] == 'observed'
