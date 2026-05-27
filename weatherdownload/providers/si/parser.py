from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import (
    SI_ARSO_DAILY_CANONICAL_ELEMENTS,
    SI_ARSO_EPOCH,
    SI_ARSO_LOCATIONS_URL,
    SI_ARSO_MIN_DATE,
    SI_ARSO_OPEN_END_DATE,
    SI_ARSO_RAW_BY_PID,
    SI_ARSO_SETTINGS_URL,
    SI_ARSO_STATION_TYPE_RAW_ELEMENTS,
)

SI_ARSO_NORMALIZED_DAILY_COLUMNS = [
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

SI_ARSO_LOCATIONS_FILENAME = 'locations.xml'
SI_ARSO_SETTINGS_FILENAME = 'settings.xml'
SI_ARSO_CANONICAL_BY_RAW = {
    raw_codes[0]: canonical
    for canonical, raw_codes in SI_ARSO_DAILY_CANONICAL_ELEMENTS.items()
}
SI_ARSO_VALUE_CONVERTERS = {
    't2m_klima': lambda value: value,
    'tmax': lambda value: value,
    'tmin': lambda value: value,
    'padavine_klima': lambda value: value,
    'sneg_skupni': lambda value: value * 10.0,
    'trajanje_so': lambda value: value,
}
SI_ARSO_MISSING_SENTINELS = {'', 'nan', 'NaN', 'null', 'None', '-999', '-9999'}
SI_ARSO_EPOCH_DATETIME = datetime.fromisoformat(SI_ARSO_EPOCH)


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def read_si_arso_station_metadata(source: str, timeout: int = 60) -> pd.DataFrame:
    locations_text = read_text_from_source(_locations_source(source), timeout, requests)
    payload = parse_si_arso_payload(locations_text)
    rows: list[dict[str, object]] = []
    station_elements: dict[str, list[str]] = {}

    for station_id, station_data in iter_si_arso_locations(payload):
        station_type = _parse_int(station_data.get('type'))
        raw_elements = list(SI_ARSO_STATION_TYPE_RAW_ELEMENTS.get(station_type, ()))
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': SI_ARSO_MIN_DATE,
                'end_date': SI_ARSO_OPEN_END_DATE,
                'full_name': _clean_string(station_data.get('name')) or pd.NA,
                'longitude': _parse_float(station_data.get('lon')),
                'latitude': _parse_float(station_data.get('lat')),
                'elevation_m': _parse_float(station_data.get('alt')),
            }
        )
        station_elements[station_id] = raw_elements

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        ('arso', 'daily'): station_elements,
    }
    return frame


def normalize_si_arso_observation_metadata(
    stations: pd.DataFrame,
    spec,
    parameter_metadata: dict[str, dict[str, str]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    station_raw_elements = stations.attrs.get('station_provider_raw_elements_by_path', {}).get((spec.provider, spec.resolution), {})
    for station in stations.itertuples(index=False):
        station_id = str(station.station_id)
        for raw_code in station_raw_elements.get(station_id, []):
            metadata = parameter_metadata[raw_code]
            rows.append(
                {
                    'obs_type': metadata['obs_type'],
                    'station_id': station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
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


def normalize_si_arso_daily_observation(
    payload_text: str,
    *,
    station_id: str,
    provider: str,
    resolution: str,
    start_date,
    end_date,
    allowed_raw_elements: set[str] | None = None,
) -> pd.DataFrame:
    payload = parse_si_arso_payload(payload_text)
    params = payload.get('params', [])
    points = payload.get('points', {})
    station_point = points.get(f'_{station_id}') or points.get(station_id) or {}
    data_points = station_point.get('data', station_point)
    index_to_raw = _build_param_index_to_raw(params)

    rows: list[dict[str, object]] = []
    for time_key, point_values in data_points.items():
        if not isinstance(point_values, dict):
            continue
        observation_date = _decode_observation_date(time_key)
        if observation_date is None:
            continue
        if start_date is not None and observation_date < start_date:
            continue
        if end_date is not None and observation_date > end_date:
            continue
        for point_key, raw_value in point_values.items():
            raw_name = index_to_raw.get(str(point_key))
            if raw_name is None:
                continue
            if allowed_raw_elements is not None and raw_name not in allowed_raw_elements:
                continue
            value = _parse_measurement_value(raw_name, raw_value)
            if value is None:
                continue
            rows.append(
                {
                    'station_id': station_id,
                    'gh_id': pd.NA,
                    'element': SI_ARSO_CANONICAL_BY_RAW[raw_name],
                    'element_raw': raw_name,
                    'observation_date': observation_date,
                    'time_function': pd.NA,
                    'value': value,
                    'flag': pd.NA,
                    'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                    'provider': provider,
                    'resolution': resolution,
                }
            )

    frame = pd.DataFrame.from_records(rows, columns=SI_ARSO_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series([pd.NA] * len(frame), dtype='Int64')
    return frame.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)


def parse_si_arso_payload(text: str) -> dict[str, object]:
    cleaned = text.strip()
    cdata_match = re.search(r'<!\[CDATA\[(.*)\]\]>', cleaned, flags=re.DOTALL)
    if cdata_match is not None:
        cleaned = cdata_match.group(1).strip()
    wrapper_match = re.search(r'AcademaPUJS\.set\((.*)\)\s*;?\s*$', cleaned, flags=re.DOTALL)
    if wrapper_match is not None:
        cleaned = wrapper_match.group(1)

    cleaned = re.sub(
        r'([{\[,]\s*)([A-Za-z_][A-Za-z0-9_]*|_[A-Za-z0-9_]+)\s*:',
        lambda match: f'{match.group(1)}"{match.group(2)}":',
        cleaned,
    )
    cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)
    return json.loads(cleaned)


def iter_si_arso_locations(payload: dict[str, object]) -> list[tuple[str, dict[str, object]]]:
    container = payload.get('locations')
    if not isinstance(container, dict):
        container = payload
    rows: list[tuple[str, dict[str, object]]] = []
    for station_key, station_data in container.items():
        if not isinstance(station_data, dict):
            continue
        if not {'name', 'lon', 'lat'}.intersection(station_data):
            continue
        station_id = str(station_key).lstrip('_')
        rows.append((station_id, station_data))
    return rows


def _locations_source(source: str) -> str:
    local_path = Path(source)
    if local_path.is_dir():
        return str(local_path / SI_ARSO_LOCATIONS_FILENAME)
    return source or SI_ARSO_LOCATIONS_URL


def _build_param_index_to_raw(params: object) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not isinstance(params, list):
        return mapping
    for index, param in enumerate(params):
        if not isinstance(param, dict):
            continue
        raw_name = _clean_string(param.get('name'))
        pid = _clean_string(param.get('pid'))
        canonical_raw = raw_name or SI_ARSO_RAW_BY_PID.get(pid, '')
        if canonical_raw:
            mapping[f'p{index}'] = canonical_raw
    return mapping


def _decode_observation_date(time_key: object):
    cleaned = _clean_string(time_key).lstrip('_')
    minutes = _parse_int(cleaned)
    if minutes is None:
        return None
    return (SI_ARSO_EPOCH_DATETIME + timedelta(minutes=minutes)).date()


def _parse_measurement_value(raw_name: str, value: object) -> float | None:
    cleaned = _clean_string(value)
    if cleaned in SI_ARSO_MISSING_SENTINELS:
        return None
    numeric = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return None
    return float(SI_ARSO_VALUE_CONVERTERS[raw_name](float(numeric)))


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    numeric = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _parse_int(value: object) -> int | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    numeric = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
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
