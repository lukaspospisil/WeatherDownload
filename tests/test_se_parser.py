from pathlib import Path

import pandas as pd

from weatherdownload.se_parser import normalize_se_observation_metadata, normalize_se_station_metadata, parse_se_daily_csv, parse_se_parameter_json
from weatherdownload.se_registry import get_dataset_spec

FIXTURE_DIR = Path('tests/data/smhi_se')


def test_parse_se_parameter_json_reads_station_list() -> None:
    payload = parse_se_parameter_json((FIXTURE_DIR / 'parameter_2.json').read_text(encoding='utf-8'))
    assert payload['key'] == '2'
    assert payload['station'][0]['id'] == 98230


def test_normalize_se_station_metadata_merges_supported_parameter_station_lists() -> None:
    payloads = [
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_2.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_5.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_19.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_20.json').read_text(encoding='utf-8')),
    ]
    stations = normalize_se_station_metadata(payloads)
    assert list(stations.columns) == ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m']
    assert stations['station_id'].tolist() == ['98230']
    assert stations.iloc[0]['full_name'] == 'Stockholm-Observatoriekullen A'
    assert stations.iloc[0]['elevation_m'] == 43.133
    assert stations['gh_id'].isna().all()


def test_normalize_se_observation_metadata_daily_rows_are_source_backed() -> None:
    payloads = [
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_2.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_5.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_19.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_20.json').read_text(encoding='utf-8')),
    ]
    metadata = normalize_se_observation_metadata(payloads, get_dataset_spec('historical', 'daily'))
    assert list(metadata.columns) == ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height']
    assert metadata['station_id'].tolist() == ['98230', '98230', '98230', '98230']
    assert metadata['element'].tolist() == ['2', '5', '19', '20']
    assert metadata['schedule'].eq('P1D SMHI metObs corrected-archive').all()
    assert metadata['height'].isna().all()


def test_parse_se_daily_csv_extracts_representative_day_value_and_raw_quality_flag() -> None:
    parsed = parse_se_daily_csv((FIXTURE_DIR / 'daily_parameter_5.csv').read_text(encoding='utf-8'))
    records = parsed['records']
    assert parsed['station_id'] == '98230'
    assert parsed['parameter_name'] == 'Nederbordsmangd'
    assert parsed['unit'] == 'millimeter'
    assert isinstance(records, pd.DataFrame)
    assert records['observation_date'].tolist() == [pd.Timestamp('1996-10-01').date(), pd.Timestamp('1996-10-02').date(), pd.Timestamp('1996-10-03').date()]
    assert records['value'].tolist() == [0.1, 0.6, 0.2]
    assert records['flag'].tolist() == ['G', 'G', 'Y']
