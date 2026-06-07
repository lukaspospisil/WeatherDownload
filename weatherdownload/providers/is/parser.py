from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import IS_VEDUR_PARAMETER_METADATA_BY_SOURCE

VEDUR_NORMALIZED_DAILY_COLUMNS = [
    'station_id',
    'gh_id',
    'element',
    'element_raw',
    'observation_date',
    'time_function',
    'value',
    'flag',
    'quality',
    'provider',
    'resolution',
]

_OPEN_END = '3999-12-31T23:59Z'


def read_json_response(source: str, timeout: int, *, params: dict[str, object] | None = None):
    local_path = Path(source)
    if local_path.exists():
        text = local_path.read_text(encoding='utf-8')
    else:
        response = requests.get(source, params=params, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        text = response.text
    return parse_vedur_payload_json(text)


def parse_vedur_payload_json(text: str):
    try:
        return json.loads(text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('Vedur response is not valid JSON.') from exc


def normalize_vedur_station_metadata(records: list[dict[str, object]], spec: object) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    station_elements: dict[str, list[str]] = {}
    supported = set(getattr(spec, 'supported_elements', ()))
    for record in records:
        if not isinstance(record, dict):
            continue
        station_id = _normalize_station_id(record.get('station'))
        if not station_id:
            continue
        raw_elements = [raw_code for raw_code in _station_supported_raw_elements(record) if raw_code in supported]
        if not raw_elements:
            continue
        station_elements[station_id] = raw_elements
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': _begin_date_from_station(record),
                'end_date': _end_date_from_station(record),
                'full_name': _clean_string(record.get('name')) or pd.NA,
                'longitude': _parse_optional_float(record.get('lon')),
                'latitude': _parse_optional_float(record.get('lat')),
                'elevation_m': _parse_optional_float(record.get('ele')),
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.drop_duplicates(subset=['station_id']).sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        (spec.provider, spec.resolution): station_elements,
    }
    return frame


def normalize_vedur_observation_metadata(
    records: list[dict[str, object]],
    spec: object,
    parameter_metadata_by_source: dict[str, dict[str, dict[str, str]]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()
    supported = set(getattr(spec, 'supported_elements', ()))
    for record in records:
        if not isinstance(record, dict):
            continue
        station_id = _normalize_station_id(record.get('station'))
        if not station_id:
            continue
        source_kind = vedur_station_source_kind(record)
        parameter_metadata = parameter_metadata_by_source[source_kind]
        for raw_code in _station_supported_raw_elements(record):
            if raw_code not in supported or raw_code not in parameter_metadata:
                continue
            key = (station_id, raw_code)
            if key in seen:
                continue
            seen.add(key)
            metadata = parameter_metadata[raw_code]
            rows.append(
                {
                    'obs_type': metadata['obs_type'],
                    'station_id': station_id,
                    'begin_date': _begin_date_from_station(record),
                    'end_date': _end_date_from_station(record),
                    'element': raw_code,
                    'schedule': metadata['schedule'],
                    'name': metadata['name'],
                    'description': f"{metadata['description']} Source unit: {metadata['unit']}.",
                    'height': pd.NA,
                }
            )

    frame = pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(['station_id', 'element'], kind='stable').reset_index(drop=True)


def normalize_vedur_daily_rows(
    records: list[dict[str, object]],
    *,
    station_id: str,
    raw_elements: list[str],
    provider: str,
    resolution: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for record in records:
        if _normalize_station_id(record.get('station')) != station_id:
            continue
        observation_date = _parse_observation_date(record.get('time'))
        if observation_date is None:
            continue
        for raw_code in raw_elements:
            if raw_code not in record:
                continue
            rows.append(
                {
                    'station_id': station_id,
                    'gh_id': pd.NA,
                    'element': raw_code,
                    'element_raw': raw_code,
                    'observation_date': observation_date,
                    'time_function': pd.NA,
                    'value': _parse_observation_value(record.get(raw_code)),
                    'flag': pd.NA,
                    'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                    'provider': provider,
                    'resolution': resolution,
                }
            )

    frame = pd.DataFrame.from_records(rows, columns=VEDUR_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series([pd.NA] * len(frame.index), dtype='Int64')
    return frame.sort_values(['station_id', 'observation_date', 'element_raw'], kind='stable').reset_index(drop=True)


def vedur_station_source_kind(record: dict[str, object]) -> str:
    station_type = _clean_string(record.get('type')).lower()
    if station_type == 'sj':
        return 'aws'
    return 'synop'


def station_supported_raw_elements_for_type(station_type: str) -> list[str]:
    source_kind = 'aws' if station_type.strip().lower() == 'sj' else 'synop'
    return list(IS_VEDUR_PARAMETER_METADATA_BY_SOURCE[source_kind].keys())


def _station_supported_raw_elements(record: dict[str, object]) -> list[str]:
    return station_supported_raw_elements_for_type(_clean_string(record.get('type')))


def _begin_date_from_station(record: dict[str, object]) -> str:
    start_year = _parse_optional_int(record.get('start'))
    if start_year is None:
        return ''
    return f'{start_year:04d}-01-01T00:00Z'


def _end_date_from_station(record: dict[str, object]) -> str:
    end_year = _parse_optional_int(record.get('ending'))
    if end_year is None:
        return _OPEN_END
    return f'{end_year:04d}-12-31T23:59Z'


def _normalize_station_id(value: object) -> str:
    cleaned = _clean_string(value)
    return cleaned


def _parse_observation_date(value: object):
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    timestamp = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(timestamp):
        return None
    return timestamp.date()


def _parse_observation_value(value: object):
    if value is None or value is pd.NA:
        return pd.NA
    numeric = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return pd.NA
    return float(numeric)


def _parse_optional_float(value: object) -> float | None:
    if value is None or value is pd.NA:
        return None
    numeric = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _parse_optional_int(value: object) -> int | None:
    if value is None or value is pd.NA:
        return None
    numeric = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return None
    return int(numeric)


def _clean_string(value: object) -> str:
    if value is None or value is pd.NA:
        return ''
    try:
        if pd.isna(value):
            return ''
    except TypeError:
        pass
    return str(value).strip()
