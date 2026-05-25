from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import AEMET_DAILY_PARAMETER_METADATA

ES_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]

KM_H_TO_M_S = 1 / 3.6


def parse_aemet_payload_json(json_text: str) -> object:
    try:
        return json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('AEMET response is not valid JSON.') from exc


def parse_aemet_station_inventory_json(json_text: str) -> pd.DataFrame:
    payload = parse_aemet_payload_json(json_text)
    if not isinstance(payload, list):
        raise ValueError('AEMET station inventory must be a JSON array.')

    rows: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        station_id = _clean_string(item.get('indicativo')).upper()
        if not station_id:
            continue
        name = _clean_string(item.get('nombre'))
        province = _clean_string(item.get('provincia'))
        full_name = name if not province else f'{name}, {province}'
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': '',
                'end_date': '',
                'full_name': full_name or pd.NA,
                'longitude': parse_aemet_coordinate(item.get('longitud')),
                'latitude': parse_aemet_coordinate(item.get('latitud')),
                'elevation_m': _parse_float(item.get('altitud')),
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.drop_duplicates(subset=['station_id']).sort_values('station_id').reset_index(drop=True)


def normalize_aemet_observation_metadata(stations: pd.DataFrame, spec: object) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in getattr(spec, 'supported_elements', ()):
            parameter_metadata = AEMET_DAILY_PARAMETER_METADATA[raw_code]
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': 'P1D AEMET daily climatology',
                    'name': parameter_metadata['name'],
                    'description': parameter_metadata['description'],
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_aemet_daily_data_json(json_text: str) -> pd.DataFrame:
    payload = parse_aemet_payload_json(json_text)
    if not isinstance(payload, list):
        raise ValueError('AEMET daily data must be a JSON array.')
    return pd.DataFrame.from_records(payload)


def parse_aemet_coordinate(value: object) -> float | None:
    cleaned = _clean_string(value).upper()
    if not cleaned:
        return None
    hemisphere = cleaned[-1]
    digits = cleaned[:-1]
    if hemisphere not in {'N', 'S', 'E', 'W'} or len(digits) < 5 or not digits.isdigit():
        return None
    seconds = int(digits[-2:])
    minutes = int(digits[-4:-2])
    degrees = int(digits[:-4])
    decimal = degrees + (minutes / 60.0) + (seconds / 3600.0)
    if hemisphere in {'S', 'W'}:
        decimal *= -1.0
    return decimal


def parse_aemet_numeric(raw_code: str, value: object) -> object:
    cleaned = _clean_string(value)
    if not cleaned:
        return pd.NA
    if raw_code == 'prec' and cleaned.casefold() == 'ip':
        return 0.0
    normalized = cleaned.replace(',', '.')
    parsed = pd.to_numeric(pd.Series([normalized]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return pd.NA
    if raw_code == 'velmedia':
        return float(parsed) * KM_H_TO_M_S
    return float(parsed)


def parse_aemet_observation_date(value: object):
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    parsed = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()
