from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

IPMA_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id',
    'gh_id',
    'element',
    'element_raw',
    'timestamp',
    'value',
    'flag',
    'quality',
    'provider',
    'resolution',
]

IPMA_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'temperatura': {
        'name': 'Hourly mean air temperature',
        'description': 'Official IPMA hourly mean air temperature at 1.5 m in degrees Celsius.',
    },
    'precAcumulada': {
        'name': 'Hourly accumulated precipitation',
        'description': 'Official IPMA hourly accumulated precipitation in millimeters.',
    },
    'intensidadeVento': {
        'name': 'Hourly mean wind speed',
        'description': 'Official IPMA hourly mean wind speed at 10 m in meters per second.',
    },
    'humidade': {
        'name': 'Hourly mean relative humidity',
        'description': 'Official IPMA hourly mean relative humidity in percent.',
    },
}

_NULLABLE_INT_DTYPE = pd.Int64Dtype()
_IPMA_NODATA = -99.0


def read_text_from_source(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def parse_ipma_stations_json(json_text: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('IPMA stations response is not valid JSON.') from exc
    if not isinstance(payload, list):
        raise ValueError('IPMA stations response must be a top-level JSON list.')
    return payload


def parse_ipma_observations_json(json_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('IPMA observations response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('IPMA observations response must be a top-level JSON object.')
    return payload


def normalize_ipma_station_metadata(json_text: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for feature in parse_ipma_stations_json(json_text):
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        geometry = feature.get('geometry')
        if not isinstance(properties, dict):
            continue
        station_id = _normalize_station_id(properties.get('idEstacao'))
        if not station_id:
            continue
        longitude, latitude = _extract_coordinates(geometry)
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': '',
                'end_date': '',
                'full_name': _clean_string(properties.get('localEstacao')) or pd.NA,
                'longitude': longitude,
                'latitude': latitude,
                'elevation_m': pd.NA,
            }
        )
    return pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)


def normalize_ipma_observation_metadata(stations: pd.DataFrame, spec: Any) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in spec.supported_elements:
            metadata = IPMA_PARAMETER_METADATA.get(raw_code, {})
            rows.append(
                {
                    'obs_type': 'RECENT_HOURLY',
                    'station_id': station.station_id,
                    'begin_date': '',
                    'end_date': '',
                    'element': raw_code,
                    'schedule': 'PT1H IPMA observations.json recent feed',
                    'name': metadata.get('name', raw_code),
                    'description': metadata.get('description', pd.NA),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def normalize_ipma_hourly_observations(
    json_text: str,
    *,
    station_ids: list[str],
    raw_elements: list[str],
    provider: str,
    resolution: str,
    start: object,
    end: object,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    payload = parse_ipma_observations_json(json_text)
    requested_station_ids = {str(station_id).strip() for station_id in station_ids}
    requested_raw_elements = list(raw_elements)
    start_ts = _normalize_query_timestamp(start)
    end_ts = _normalize_query_timestamp(end)

    metadata_lookup = None
    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])

    rows: list[dict[str, object]] = []
    for timestamp_key, station_map in payload.items():
        if not isinstance(station_map, dict):
            continue
        timestamp = pd.to_datetime(timestamp_key, errors='coerce')
        if pd.isna(timestamp):
            continue
        if timestamp < start_ts or timestamp > end_ts:
            continue
        for station_id_key, values in station_map.items():
            station_id = _normalize_station_id(station_id_key)
            if station_id not in requested_station_ids:
                continue
            if not isinstance(values, dict):
                continue
            for raw_code in requested_raw_elements:
                if raw_code not in values:
                    continue
                value = _normalize_value(values.get(raw_code))
                rows.append(
                    {
                        'station_id': station_id,
                        'gh_id': pd.NA,
                        'element': raw_code,
                        'element_raw': raw_code,
                        'timestamp': timestamp,
                        'value': value,
                        'flag': pd.NA,
                        'quality': pd.Series([pd.NA], dtype=_NULLABLE_INT_DTYPE).iloc[0],
                        'provider': provider,
                        'resolution': resolution,
                    }
                )

    if not rows:
        return pd.DataFrame(columns=IPMA_NORMALIZED_SUBDAILY_COLUMNS)

    table = pd.DataFrame.from_records(rows)
    table['quality'] = pd.Series(table['quality'], dtype=_NULLABLE_INT_DTYPE)
    if metadata_lookup is not None:
        table = table.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
    return table.loc[:, IPMA_NORMALIZED_SUBDAILY_COLUMNS].reset_index(drop=True)


def sort_station_metadata(stations: pd.DataFrame) -> pd.DataFrame:
    if stations.empty:
        return stations
    stations = stations.copy()
    stations['_sort_id'] = stations['station_id'].map(_station_sort_key)
    stations = stations.sort_values(['_sort_id', 'station_id']).drop(columns=['_sort_id']).reset_index(drop=True)
    return stations


def _normalize_station_id(value: object) -> str:
    cleaned = _clean_string(value)
    if cleaned.endswith('.0'):
        cleaned = cleaned[:-2]
    return cleaned


def _extract_coordinates(geometry: object) -> tuple[float | None, float | None]:
    if not isinstance(geometry, dict):
        return None, None
    coordinates = geometry.get('coordinates')
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        return None, None
    return _parse_float(coordinates[0]), _parse_float(coordinates[1])


def _normalize_value(value: object) -> object:
    numeric = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return pd.NA
    if float(numeric) == _IPMA_NODATA:
        return pd.NA
    return float(numeric)


def _normalize_query_timestamp(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_localize(None)
    return timestamp


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
