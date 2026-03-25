import os
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import ObservationQuery, download_observations, read_station_metadata

SAMPLE_STATIONS_PATH = Path('tests/data/sample_knmi_station_metadata.csv')


def test_nl_hourly_download_contract_matches_shared_subdaily_schema() -> None:
    station_metadata = read_station_metadata(country='NL', source_url=str(SAMPLE_STATIONS_PATH))
    query = ObservationQuery(
        country='NL',
        dataset_scope='historical',
        resolution='1hour',
        station_ids=['0-20000-0-06260'],
        start='2024-01-01T01:00:00Z',
        end='2024-01-01T02:00:00Z',
        elements=['tas_mean', 'pressure'],
    )
    parsed_payloads = iter([
        {
            'timestamp': pd.Timestamp('2024-01-01T01:00:00Z'),
            'stations': pd.DataFrame([
                {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
            ]),
            'variables': {'T': pd.Series([3.1, 4.2]), 'P': pd.Series([1012.1, 1011.2])},
        },
        {
            'timestamp': pd.Timestamp('2024-01-01T02:00:00Z'),
            'stations': pd.DataFrame([
                {'station_id': '0-20000-0-06260', 'full_name': 'De Bilt', 'latitude': 52.1, 'longitude': 5.18, 'elevation_m': 4.0},
                {'station_id': '0-20000-0-06310', 'full_name': 'Vlissingen', 'latitude': 51.442, 'longitude': 3.596, 'elevation_m': 8.0},
            ]),
            'variables': {'T': pd.Series([3.8, 4.9]), 'P': pd.Series([1011.7, 1010.8])},
        },
    ])
    file_listing = {'files': [{'filename': 'hourly-observations-20240101-01.nc'}, {'filename': 'hourly-observations-20240101-02.nc'}]}

    with patch.dict(os.environ, {'WEATHERDOWNLOAD_KNMI_API_KEY': 'test-key'}, clear=False):
        with patch('weatherdownload.knmi_hourly.list_knmi_files', return_value=file_listing):
            with patch('weatherdownload.knmi_hourly.download_knmi_file_bytes', side_effect=[b'first', b'second']):
                with patch('weatherdownload.knmi_hourly.parse_knmi_hourly_netcdf_bytes', side_effect=lambda payload: next(parsed_payloads)):
                    observations = download_observations(query, country='NL', station_metadata=station_metadata)

    assert list(observations.columns) == ['station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'dataset_scope', 'resolution']
    assert observations['element'].str.match(r'^[a-z0-9_]+$').all()
    assert observations['element_raw'].notna().all()
    assert observations['timestamp'].map(lambda value: hasattr(value, 'isoformat')).all()
    assert observations['dataset_scope'].eq('historical').all()
    assert observations['resolution'].eq('1hour').all()
    assert observations['gh_id'].isna().all()
    assert observations['flag'].isna().all()
    assert observations['quality'].isna().all()
    assert str(observations['quality'].dtype) == 'Int64'
