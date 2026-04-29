from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

BE_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]

BE_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'provider', 'resolution',
]


def parse_be_feature_collection_json(json_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('RMI/KMI AWS response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('RMI/KMI AWS response must be a top-level JSON object.')
    features = payload.get('features')
    if not isinstance(features, list):
        raise ValueError('RMI/KMI AWS response is missing a features list.')
    return payload


def normalize_be_station_metadata(payload: dict[str, object]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for feature in payload.get('features', []):
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        geometry = feature.get('geometry')
        if not isinstance(properties, dict):
            continue
        station_id = normalize_be_station_id(properties.get('code'))
        if not station_id:
            continue
        longitude, latitude = _extract_coordinates(geometry)
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': normalize_be_metadata_datetime(properties.get('date_begin')),
                'end_date': normalize_be_metadata_datetime(properties.get('date_end')),
                'full_name': _clean_string(properties.get('name')) or pd.NA,
                'longitude': longitude,
                'latitude': latitude,
                'elevation_m': _parse_float(properties.get('altitude')),
            }
        )
    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['_sort_id'] = frame['station_id'].map(_station_sort_key)
    frame = frame.sort_values(['_sort_id', 'station_id']).drop(columns=['_sort_id']).reset_index(drop=True)
    return frame


def normalize_be_observation_metadata(stations: pd.DataFrame, spec: Any, parameter_metadata: dict[str, dict[str, str]]) -> pd.DataFrame:
    schedule, obs_type = _metadata_schedule_and_type(spec.resolution)
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in spec.supported_elements:
            metadata = parameter_metadata.get(raw_code, {})
            rows.append(
                {
                    'obs_type': obs_type,
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': schedule,
                    'name': metadata.get('name', raw_code),
                    'description': metadata.get('description', pd.NA),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def normalize_be_station_id(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    cleaned = str(value).strip()
    if not cleaned:
        return ''
    if cleaned.endswith('.0'):
        cleaned = cleaned[:-2]
    return cleaned


def normalize_be_metadata_datetime(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    timestamp = pd.Timestamp(cleaned)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    else:
        timestamp = timestamp.tz_convert('UTC')
    return timestamp.strftime('%Y-%m-%dT%H:%MZ')


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _metadata_schedule_and_type(resolution: str) -> tuple[str, str]:
    if resolution == 'daily':
        return 'P1D RMI/KMI AWS WFS', 'HISTORICAL_DAILY'
    if resolution == '1hour':
        return 'PT1H RMI/KMI AWS WFS', 'HISTORICAL_HOURLY'
    return 'PT10M RMI/KMI AWS WFS', 'HISTORICAL_10MIN'


def _extract_coordinates(geometry: object) -> tuple[float | None, float | None]:
    if not isinstance(geometry, dict):
        return None, None
    coordinates = geometry.get('coordinates')
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return None, None
    return _parse_float(coordinates[0]), _parse_float(coordinates[1])


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

