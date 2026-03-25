from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd

from .metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

GEOSPHERE_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]

GEOSPHERE_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]


def parse_geosphere_metadata_json(json_text: str) -> dict[str, object]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('GeoSphere metadata response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('GeoSphere metadata response must be a top-level JSON object.')
    if 'stations' not in payload or not isinstance(payload['stations'], list):
        raise ValueError('GeoSphere metadata response is missing a stations list.')
    if 'parameters' not in payload or not isinstance(payload['parameters'], list):
        raise ValueError('GeoSphere metadata response is missing a parameters list.')
    return payload


def normalize_geosphere_station_metadata(payload: dict[str, object]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in payload.get('stations', []):
        if not isinstance(station, dict):
            continue
        station_id = normalize_geosphere_station_id(station.get('id'))
        if not station_id:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': normalize_geosphere_metadata_datetime(station.get('valid_from')),
                'end_date': normalize_geosphere_metadata_datetime(station.get('valid_to')),
                'full_name': _clean_string(station.get('name')) or pd.NA,
                'longitude': _parse_float(station.get('lon')),
                'latitude': _parse_float(station.get('lat')),
                'elevation_m': _parse_float(station.get('altitude')),
            }
        )
    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['_sort_id'] = frame['station_id'].map(_station_sort_key)
    frame = frame.sort_values(['_sort_id', 'station_id']).drop(columns=['_sort_id']).reset_index(drop=True)
    return frame


def normalize_geosphere_observation_metadata(payload: dict[str, object], spec: object) -> pd.DataFrame:
    supported_elements = tuple(getattr(spec, 'supported_elements', ()))
    schedule, obs_type = _metadata_schedule_and_type(getattr(spec, 'resolution', 'daily'))
    parameter_lookup = {
        _clean_string(parameter.get('name')): parameter
        for parameter in payload.get('parameters', [])
        if isinstance(parameter, dict) and _clean_string(parameter.get('name'))
    }
    station_frame = normalize_geosphere_station_metadata(payload)
    rows: list[dict[str, object]] = []
    for station in station_frame.itertuples(index=False):
        for raw_code in supported_elements:
            parameter = parameter_lookup.get(raw_code, {})
            rows.append(
                {
                    'obs_type': obs_type,
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': schedule,
                    'name': _clean_string(parameter.get('long_name')) or raw_code,
                    'description': _compose_parameter_description(parameter),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_geosphere_station_csv(csv_text: str, resolution_label: str) -> pd.DataFrame:
    table = pd.read_csv(io.StringIO(csv_text), dtype=str)
    expected_columns = {'time', 'station'}
    missing = expected_columns.difference(table.columns)
    if missing:
        raise ValueError(f'GeoSphere {resolution_label} CSV is missing required columns: {sorted(missing)}')
    return table


def parse_geosphere_daily_csv(csv_text: str) -> pd.DataFrame:
    return parse_geosphere_station_csv(csv_text, 'daily')


def parse_geosphere_hourly_csv(csv_text: str) -> pd.DataFrame:
    return parse_geosphere_station_csv(csv_text, 'hourly')


def normalize_geosphere_station_id(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def normalize_geosphere_metadata_datetime(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    timestamp = pd.Timestamp(cleaned)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    else:
        timestamp = timestamp.tz_convert('UTC')
    return timestamp.strftime('%Y-%m-%dT%H:%MZ')


def _compose_parameter_description(parameter: dict[str, object]) -> object:
    description = _clean_string(parameter.get('description')) or _clean_string(parameter.get('desc'))
    unit = _clean_string(parameter.get('unit'))
    if description and unit:
        return f'{description} [{unit}]'
    if description:
        return description
    if unit:
        return f'Unit: {unit}'
    return pd.NA


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    return float(cleaned)


def _station_sort_key(station_id: str) -> tuple[int, str]:
    return (int(station_id), station_id) if station_id.isdigit() else (10**9, station_id)


def build_geosphere_flag(value: object) -> object:
    cleaned = _clean_string(value)
    return cleaned or pd.NA


def _metadata_schedule_and_type(resolution: str) -> tuple[str, str]:
    if resolution == '1hour':
        return 'PT1H GeoSphere station API', 'HISTORICAL_HOURLY'
    return 'P1D GeoSphere station API', 'HISTORICAL_DAILY'


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text
