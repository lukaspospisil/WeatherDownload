from datetime import date
from pathlib import Path

import pandas as pd

from weatherdownload.providers.ca.eccc_parser import (
    CA_ECCC_NORMALIZED_DAILY_COLUMNS,
    CA_ECCC_NORMALIZED_HOURLY_COLUMNS,
    parse_ca_eccc_daily_feature_collection,
    parse_ca_eccc_hourly_feature_collection,
    parse_ca_eccc_local_date,
    normalize_ca_eccc_daily_observations,
    normalize_ca_eccc_hourly_observations,
    normalize_ca_eccc_station_id,
)


FIXTURE_PATH = Path('tests/data/sample_ca_eccc_daily.json')
HOURLY_FIXTURE_PATH = Path('tests/data/sample_ca_eccc_hourly.json')


def test_parse_ca_eccc_local_date_returns_python_date() -> None:
    assert parse_ca_eccc_local_date('2025-01-02 00:00:00') == date(2025, 1, 2)


def test_normalize_ca_eccc_station_id_uses_climate_identifier_as_string() -> None:
    assert normalize_ca_eccc_station_id('1021330') == '1021330'
    assert normalize_ca_eccc_station_id(1021330) == '1021330'


def test_parse_ca_eccc_daily_feature_collection_extracts_station_and_dates() -> None:
    parsed = parse_ca_eccc_daily_feature_collection(FIXTURE_PATH.read_text(encoding='utf-8'))

    assert parsed['station_id'].tolist() == ['1021330', '1021330', '1021330']
    assert parsed['observation_date'].tolist() == [
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 1, 3),
    ]
    assert parsed['MEAN_TEMPERATURE'].tolist()[0] == 5.4
    assert pd.isna(parsed['MEAN_TEMPERATURE'].tolist()[1])
    assert parsed['MEAN_TEMPERATURE'].tolist()[2] == 1.5
    assert parsed['TOTAL_PRECIPITATION'].tolist()[0] == 12.3
    assert parsed['TOTAL_PRECIPITATION'].tolist()[1] == 0.0
    assert pd.isna(parsed['TOTAL_PRECIPITATION'].tolist()[2])


def test_normalize_ca_eccc_daily_observations_maps_raw_fields_to_canonical_values() -> None:
    parsed = parse_ca_eccc_daily_feature_collection(FIXTURE_PATH.read_text(encoding='utf-8'))
    normalized = normalize_ca_eccc_daily_observations(parsed)

    assert list(normalized.columns) == CA_ECCC_NORMALIZED_DAILY_COLUMNS
    assert normalized['station_id'].unique().tolist() == ['1021330']
    assert normalized['provider'].unique().tolist() == ['eccc']
    assert normalized['resolution'].unique().tolist() == ['daily']
    assert str(normalized['quality'].dtype) == 'Int64'
    assert sorted(normalized['element'].unique().tolist()) == ['precipitation', 'tas_max', 'tas_mean', 'tas_min']
    assert sorted(normalized['element_raw'].unique().tolist()) == [
        'MAX_TEMPERATURE',
        'MEAN_TEMPERATURE',
        'MIN_TEMPERATURE',
        'TOTAL_PRECIPITATION',
    ]

    lookup = normalized.set_index(['element', 'observation_date'])['value']
    assert float(lookup[('tas_mean', date(2025, 1, 1))]) == 5.4
    assert float(lookup[('tas_min', date(2025, 1, 2))]) == -2.0
    assert float(lookup[('tas_max', date(2025, 1, 3))]) == 4.0
    assert float(lookup[('precipitation', date(2025, 1, 2))]) == 0.0


def test_normalize_ca_eccc_daily_observations_drops_missing_values_and_keeps_flags() -> None:
    parsed = parse_ca_eccc_daily_feature_collection(FIXTURE_PATH.read_text(encoding='utf-8'))
    normalized = normalize_ca_eccc_daily_observations(parsed)

    records = normalized[['element', 'observation_date']].to_dict('records')
    assert {'element': 'tas_mean', 'observation_date': date(2025, 1, 2)} not in records
    assert {'element': 'precipitation', 'observation_date': date(2025, 1, 3)} not in records

    flag_lookup = normalized.set_index(['element', 'observation_date'])['flag']
    assert pd.isna(flag_lookup[('tas_mean', date(2025, 1, 1))])
    assert flag_lookup[('tas_min', date(2025, 1, 1))] == 'E'
    assert flag_lookup[('precipitation', date(2025, 1, 1))] == 'T'


def test_normalize_ca_eccc_daily_observations_can_filter_station_range_and_raw_elements() -> None:
    parsed = parse_ca_eccc_daily_feature_collection(FIXTURE_PATH.read_text(encoding='utf-8'))
    normalized = normalize_ca_eccc_daily_observations(
        parsed,
        station_ids=['1021330'],
        raw_elements=['TOTAL_PRECIPITATION', 'MAX_TEMPERATURE'],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 3),
    )

    assert normalized[['element', 'observation_date']].to_dict('records') == [
        {'element': 'precipitation', 'observation_date': date(2025, 1, 2)},
        {'element': 'tas_max', 'observation_date': date(2025, 1, 2)},
        {'element': 'tas_max', 'observation_date': date(2025, 1, 3)},
    ]


def test_parse_ca_eccc_hourly_feature_collection_extracts_station_and_timestamps() -> None:
    parsed = parse_ca_eccc_hourly_feature_collection(HOURLY_FIXTURE_PATH.read_text(encoding='utf-8'))
    assert parsed['station_id'].tolist() == ['1017101', '1017101']
    assert str(parsed['timestamp'].iloc[0]).startswith('2024-10-02 09:00:00')
    assert pd.isna(parsed['TEMP'].iloc[1])
    assert float(parsed['RELATIVE_HUMIDITY'].iloc[0]) == 95.0


def test_normalize_ca_eccc_hourly_observations_maps_raw_fields_to_canonical_values_and_keeps_missing() -> None:
    parsed = parse_ca_eccc_hourly_feature_collection(HOURLY_FIXTURE_PATH.read_text(encoding='utf-8'))
    normalized = normalize_ca_eccc_hourly_observations(
        parsed,
        station_ids=['1017101'],
        raw_elements=['TEMP', 'RELATIVE_HUMIDITY'],
        start='2024-10-02T09:00:00Z',
        end='2024-10-02T10:00:00Z',
    )

    assert list(normalized.columns) == CA_ECCC_NORMALIZED_HOURLY_COLUMNS
    assert sorted(normalized['element'].unique().tolist()) == ['relative_humidity', 'tas_mean']
    # Missing TEMP stays present as a row with value NaN/NA.
    lookup = normalized.set_index(['element', 'timestamp'])['value']
    first_ts = pd.to_datetime('2024-10-02T09:00:00Z')
    second_ts = pd.to_datetime('2024-10-02T10:00:00Z')
    assert float(lookup[('tas_mean', first_ts)]) == 9.8
    assert pd.isna(lookup[('tas_mean', second_ts)])
    flag_lookup = normalized.set_index(['element', 'timestamp'])['flag']
    assert pd.isna(flag_lookup[('tas_mean', first_ts)])
    assert flag_lookup[('relative_humidity', second_ts)] == 'E'
