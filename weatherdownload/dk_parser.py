from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from .metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .dk_registry import DMI_DENMARK_COUNTRY_CODE

DK_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]

_COPENHAGEN = ZoneInfo('Europe/Copenhagen')


def parse_dk_feature_collection_json(json_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('DMI Climate Data response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('DMI Climate Data response must be a top-level JSON object.')
    features = payload.get('features')
    if not isinstance(features, list):
        raise ValueError('DMI Climate Data response is missing a features list.')
    return payload



def normalize_dk_station_metadata(payload: dict[str, object]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for feature in payload.get('features', []):
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        geometry = feature.get('geometry')
        if not isinstance(properties, dict):
            continue
        if _clean_string(properties.get('country')) != DMI_DENMARK_COUNTRY_CODE:
            continue
        station_id = normalize_dk_station_id(properties.get('stationId'))
        if not station_id:
            continue
        longitude, latitude = _extract_coordinates(geometry)
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': normalize_dk_metadata_datetime(properties.get('validFrom') or properties.get('operationFrom')),
                'end_date': normalize_dk_metadata_datetime(properties.get('validTo') or properties.get('operationTo')),
                'full_name': _clean_string(properties.get('name')) or pd.NA,
                'longitude': longitude,
                'latitude': latitude,
                'elevation_m': _parse_float(properties.get('stationHeight')),
            }
        )
    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['_sort_id'] = frame['station_id'].map(_station_sort_key)
    frame = frame.sort_values(['_sort_id', 'station_id']).drop(columns=['_sort_id']).reset_index(drop=True)
    return frame



def normalize_dk_observation_metadata(payload: dict[str, object], spec: Any, parameter_metadata: dict[str, dict[str, str]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for feature in payload.get('features', []):
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        if not isinstance(properties, dict):
            continue
        if _clean_string(properties.get('country')) != DMI_DENMARK_COUNTRY_CODE:
            continue
        station_id = normalize_dk_station_id(properties.get('stationId'))
        if not station_id:
            continue
        supported_here = {
            _clean_string(raw_code)
            for raw_code in properties.get('parameterId', [])
            if _clean_string(raw_code) in spec.supported_elements
        }
        for raw_code in spec.supported_elements:
            if raw_code not in supported_here:
                continue
            metadata = parameter_metadata.get(raw_code, {})
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station_id,
                    'begin_date': normalize_dk_metadata_datetime(properties.get('validFrom') or properties.get('operationFrom')),
                    'end_date': normalize_dk_metadata_datetime(properties.get('validTo') or properties.get('operationTo')),
                    'element': raw_code,
                    'schedule': 'P1D DMI climateData stationValue',
                    'name': metadata.get('name', raw_code),
                    'description': metadata.get('description', pd.NA),
                    'height': _parse_float(properties.get('stationHeight')),
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)



def normalize_dk_station_id(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    cleaned = str(value).strip()
    if not cleaned:
        return ''
    if cleaned.endswith('.0'):
        cleaned = cleaned[:-2]
    return cleaned



def normalize_dk_metadata_datetime(value: object) -> str:
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



def observation_date_from_interval_start(value: object) -> pd.Timestamp | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    timestamp = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(timestamp):
        return None
    return timestamp.tz_convert(_COPENHAGEN).date()



def build_dk_flag(properties: dict[str, object]) -> object:
    flag_payload: dict[str, object] = {}
    qc_status = _clean_string(properties.get('qcStatus'))
    if qc_status:
        flag_payload['qcStatus'] = qc_status
    validity = properties.get('validity')
    if validity is not None and not pd.isna(validity):
        flag_payload['validity'] = bool(validity)
    if not flag_payload:
        return pd.NA
    return json.dumps(flag_payload, separators=(',', ':'), sort_keys=True)



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
