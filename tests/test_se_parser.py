from pathlib import Path

import pandas as pd

from weatherdownload.se_parser import (
    normalize_se_observation_metadata,
    normalize_se_station_metadata,
    parse_se_daily_csv,
    parse_se_hourly_csv,
    parse_se_parameter_json,
)

FIXTURE_DIR = Path('tests/data/smhi_se')


def test_parse_se_parameter_json_reads_station_list() -> None:
    payload = parse_se_parameter_json((FIXTURE_DIR / 'parameter_2.json').read_text(encoding='utf-8'))
    assert payload['key'] == '2'
    assert payload['station'][0]['id'] == 98230


def test_normalize_se_station_metadata_merges_supported_parameter_station_lists() -> None:
    payloads = [
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_1.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_2.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_5.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_9.json').read_text(encoding='utf-8')),
    ]
    stations = normalize_se_station_metadata(payloads)
    assert list(stations.columns) == ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m']
    assert stations['station_id'].tolist() == ['98230']
    assert stations.iloc[0]['full_name'] == 'Stockholm-Observatoriekullen A'
    assert stations.iloc[0]['elevation_m'] == 43.133
    assert stations['gh_id'].isna().all()


def test_normalize_se_observation_metadata_exposes_daily_and_hourly_rows() -> None:
    payloads = [
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_1.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_2.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_4.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_5.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_6.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_7.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_9.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_19.json').read_text(encoding='utf-8')),
        parse_se_parameter_json((FIXTURE_DIR / 'parameter_20.json').read_text(encoding='utf-8')),
    ]
    metadata = normalize_se_observation_metadata(payloads)
    assert list(metadata.columns) == ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height']
    assert metadata['station_id'].tolist() == ['98230'] * 9
    assert sorted(metadata['element'].tolist()) == ['1', '19', '2', '20', '4', '5', '6', '7', '9']
    assert set(metadata['schedule']) == {'P1D SMHI metObs corrected-archive', 'PT1H SMHI metObs corrected-archive'}
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


def test_parse_se_hourly_csv_extracts_timestamp_value_and_raw_quality_flag() -> None:
    parsed = parse_se_hourly_csv((FIXTURE_DIR / 'hourly_parameter_9.csv').read_text(encoding='utf-8'))
    records = parsed['records']
    assert parsed['station_id'] == '98230'
    assert parsed['parameter_name'] == 'Lufttryck reducerat havsytans niva'
    assert parsed['unit'] == 'hektopascal'
    assert isinstance(records, pd.DataFrame)
    assert records['timestamp'].tolist() == [
        pd.Timestamp('2012-11-29T11:00:00Z'),
        pd.Timestamp('2012-11-29T12:00:00Z'),
        pd.Timestamp('2012-11-29T13:00:00Z'),
    ]
    assert records['value'].tolist() == [1006.6, 1006.2, 1005.9]
    assert records['flag'].tolist() == ['G', 'G', 'Y']
