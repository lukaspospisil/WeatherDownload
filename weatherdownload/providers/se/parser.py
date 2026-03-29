from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import SE_PARAMETER_METADATA, SMHI_DAILY_PERIOD_KEY, SMHI_HOURLY_PERIOD_KEY

SE_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]

SE_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]


def parse_se_parameter_json(json_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('SMHI response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('SMHI response must be a top-level JSON object.')
    stations = payload.get('station')
    if not isinstance(stations, list):
        raise ValueError('SMHI response is missing a station list.')
    return payload


def normalize_se_station_metadata(payloads: list[dict[str, object]]) -> pd.DataFrame:
    merged: dict[str, dict[str, object]] = {}
    for payload in payloads:
        for station in payload.get('station', []):
            if not isinstance(station, dict):
                continue
            station_id = normalize_se_station_id(station.get('id') or station.get('key'))
            if not station_id:
                continue
            begin_date = normalize_se_metadata_datetime(station.get('from'))
            end_date = normalize_se_metadata_datetime(station.get('to'))
            row = merged.get(
                station_id,
                {
                    'station_id': station_id,
                    'gh_id': pd.NA,
                    'begin_date': begin_date,
                    'end_date': end_date,
                    'full_name': _clean_string(station.get('name')) or pd.NA,
                    'longitude': _parse_float(station.get('longitude')),
                    'latitude': _parse_float(station.get('latitude')),
                    'elevation_m': _parse_float(station.get('height')),
                },
            )
            row['begin_date'] = _min_datetime_string(row.get('begin_date'), begin_date)
            row['end_date'] = _max_datetime_string(row.get('end_date'), end_date)
            if pd.isna(row.get('full_name')) and _clean_string(station.get('name')):
                row['full_name'] = _clean_string(station.get('name'))
            if row.get('longitude') is None:
                row['longitude'] = _parse_float(station.get('longitude'))
            if row.get('latitude') is None:
                row['latitude'] = _parse_float(station.get('latitude'))
            if row.get('elevation_m') is None:
                row['elevation_m'] = _parse_float(station.get('height'))
            merged[station_id] = row

    frame = pd.DataFrame.from_records(list(merged.values()), columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['_sort_id'] = frame['station_id'].map(_station_sort_key)
    frame = frame.sort_values(['_sort_id', 'station_id']).drop(columns=['_sort_id']).reset_index(drop=True)
    return frame


def normalize_se_observation_metadata(payloads: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for payload in payloads:
        parameter_id = _clean_string(payload.get('key'))
        metadata = SE_PARAMETER_METADATA.get(parameter_id, {})
        if not metadata:
            continue
        for station in payload.get('station', []):
            if not isinstance(station, dict):
                continue
            station_id = normalize_se_station_id(station.get('id') or station.get('key'))
            if not station_id:
                continue
            rows.append(
                {
                    'obs_type': metadata.get('obs_type', ''),
                    'station_id': station_id,
                    'begin_date': normalize_se_metadata_datetime(station.get('from')),
                    'end_date': normalize_se_metadata_datetime(station.get('to')),
                    'element': parameter_id,
                    'schedule': metadata.get('schedule', pd.NA),
                    'name': metadata.get('name', payload.get('title') or parameter_id),
                    'description': metadata.get('description', pd.NA),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_se_daily_csv(csv_text: str) -> dict[str, object]:
    rows = list(csv.reader(io.StringIO(csv_text.lstrip('\ufeff')), delimiter=';'))
    station_values = _value_row_after_header(rows, 'Stationsnamn')
    parameter_values = _value_row_after_header(rows, 'Parameternamn')
    period_values = _value_row_after_header(rows, 'Tidsperiod (fr.o.m)')
    data_header_index = _header_index(rows, 'Fran Datum Tid (UTC)')
    data_rows = _parse_daily_data_rows(rows[data_header_index + 1 :])

    return {
        'station_name': _value_at(station_values, 0),
        'station_id': normalize_se_station_id(_value_at(station_values, 1)),
        'station_network': _value_at(station_values, 2) or pd.NA,
        'measurement_height_m': _parse_float(_value_at(station_values, 3)),
        'parameter_name': _value_at(parameter_values, 0) or pd.NA,
        'parameter_summary': _value_at(parameter_values, 1) or pd.NA,
        'unit': _value_at(parameter_values, 2) or pd.NA,
        'period_from': _value_at(period_values, 0) or '',
        'period_to': _value_at(period_values, 1) or '',
        'elevation_m': _parse_float(_value_at(period_values, 2)),
        'latitude': _parse_float(_value_at(period_values, 3)),
        'longitude': _parse_float(_value_at(period_values, 4)),
        'records': pd.DataFrame.from_records(
            data_rows,
            columns=['from_utc', 'to_utc', 'observation_date', 'value', 'flag'],
        ),
    }


def parse_se_hourly_csv(csv_text: str) -> dict[str, object]:
    parsed = _parse_se_hourly_sections(csv_text)
    return {
        **parsed,
        'records': pd.DataFrame.from_records(
            _parse_hourly_data_rows(parsed['data_rows']),
            columns=['timestamp', 'value', 'flag'],
        ),
    }


def normalize_se_station_id(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    cleaned = str(value).strip()
    if not cleaned:
        return ''
    if cleaned.endswith('.0'):
        cleaned = cleaned[:-2]
    return cleaned


def normalize_se_metadata_datetime(value: object) -> str:
    if value is None or pd.isna(value) or str(value).strip() == '':
        return ''
    timestamp = pd.to_datetime(int(str(value).strip()), unit='ms', utc=True, errors='coerce')
    if pd.isna(timestamp):
        return ''
    return timestamp.strftime('%Y-%m-%dT%H:%MZ')


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def build_se_flag(value: object) -> object:
    cleaned = _clean_string(value)
    return cleaned or pd.NA


def _parse_se_hourly_sections(csv_text: str) -> dict[str, object]:
    rows = list(csv.reader(io.StringIO(csv_text.lstrip('\ufeff')), delimiter=';'))
    station_values = _value_row_after_header(rows, 'Stationsnamn')
    parameter_values = _value_row_after_header(rows, 'Parameternamn')
    period_values = _value_row_after_header(rows, 'Tidsperiod (fr.o.m)')
    data_header_index = _find_hourly_data_header_index(rows)
    return {
        'station_name': _value_at(station_values, 0),
        'station_id': normalize_se_station_id(_value_at(station_values, 1)),
        'station_network': _value_at(station_values, 2) or pd.NA,
        'measurement_height_m': _parse_float(_value_at(station_values, 3)),
        'parameter_name': _value_at(parameter_values, 0) or pd.NA,
        'parameter_summary': _value_at(parameter_values, 1) or pd.NA,
        'unit': _value_at(parameter_values, 2) or pd.NA,
        'period_from': _value_at(period_values, 0) or '',
        'period_to': _value_at(period_values, 1) or '',
        'elevation_m': _parse_float(_value_at(period_values, 2)),
        'latitude': _parse_float(_value_at(period_values, 3)),
        'longitude': _parse_float(_value_at(period_values, 4)),
        'data_rows': rows[data_header_index + 1 :],
    }


def _value_row_after_header(rows: list[list[str]], header_name: str) -> list[str]:
    header_index = _header_index(rows, header_name)
    for row in rows[header_index + 1 :]:
        if any(_clean_string(cell) for cell in row):
            return row
    raise ValueError(f'SMHI CSV is missing a values row for header {header_name!r}.')


def _header_index(rows: list[list[str]], first_cell: str) -> int:
    for index, row in enumerate(rows):
        if row and _clean_string(row[0]) == first_cell:
            return index
    raise ValueError(f'SMHI CSV is missing the header row {first_cell!r}.')


def _find_hourly_data_header_index(rows: list[list[str]]) -> int:
    for index, row in enumerate(rows):
        if len(row) >= 2 and _clean_string(row[0]) == 'Datum' and _clean_string(row[1]) == 'Tid (UTC)':
            return index
    raise ValueError('SMHI hourly CSV is missing the data header row.')


def _parse_daily_data_rows(rows: list[list[str]]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        if len(row) < 5:
            continue
        from_utc = _clean_string(row[0])
        to_utc = _clean_string(row[1])
        observation_date = _parse_date(_clean_string(row[2]))
        if not from_utc or not to_utc or observation_date is None:
            continue
        records.append(
            {
                'from_utc': pd.to_datetime(from_utc, utc=True, errors='coerce'),
                'to_utc': pd.to_datetime(to_utc, utc=True, errors='coerce'),
                'observation_date': observation_date,
                'value': _parse_float(row[3]),
                'flag': build_se_flag(row[4]),
            }
        )
    return records


def _parse_hourly_data_rows(rows: list[list[str]]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for row in rows:
        if len(row) < 4:
            continue
        day = _clean_string(row[0])
        time_value = _clean_string(row[1])
        if not day or not time_value:
            continue
        timestamp = pd.to_datetime(f'{day}T{time_value}Z', utc=True, errors='coerce')
        if pd.isna(timestamp):
            continue
        records.append(
            {
                'timestamp': timestamp,
                'value': _parse_float(row[2]),
                'flag': build_se_flag(row[3]),
            }
        )
    return records


def _value_at(row: list[str], index: int) -> str:
    if index >= len(row):
        return ''
    return _clean_string(row[index])


def _parse_date(value: str) -> object:
    if not value:
        return None
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    return float(cleaned)


def _station_sort_key(station_id: str) -> tuple[int, str]:
    return (int(station_id), station_id) if station_id.isdigit() else (10**9, station_id)


def _min_datetime_string(left: object, right: object) -> str:
    left_cleaned = _clean_string(left)
    right_cleaned = _clean_string(right)
    if not left_cleaned:
        return right_cleaned
    if not right_cleaned:
        return left_cleaned
    return min(left_cleaned, right_cleaned)


def _max_datetime_string(left: object, right: object) -> str:
    left_cleaned = _clean_string(left)
    right_cleaned = _clean_string(right)
    if not left_cleaned:
        return right_cleaned
    if not right_cleaned:
        return left_cleaned
    return max(left_cleaned, right_cleaned)

