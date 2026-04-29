from __future__ import annotations

import csv
import gzip
import io
import json
from pathlib import Path

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

FR_NORMALIZED_DAILY_COLUMNS = [
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

FR_SUPPORTED_PARAMETER_NAMES = {
    'RR': 'HAUTEUR DE PRECIPITATIONS QUOTIDIENNE',
    'TN': 'TEMPERATURE MINIMALE SOUS ABRI QUOTIDIENNE',
    'TX': 'TEMPERATURE MAXIMALE SOUS ABRI QUOTIDIENNE',
    'TM': 'MOYENNE DES TM',
}

_FR_MISSING_SENTINELS = {'', 'mq', 'M', 'NA', 'nan', 'NaN'}


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return _decode_french_text(local_path.read_bytes())
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    content = getattr(response, 'content', None)
    if content is not None:
        return _decode_french_text(content)
    response.encoding = 'utf-8'
    return response.text


def parse_fr_station_metadata_json(json_text: str) -> pd.DataFrame:
    payload = json.loads(json_text.lstrip('\ufeff'))
    if not isinstance(payload, list):
        raise ValueError('Meteo-France station metadata must be a top-level JSON list.')

    rows: list[dict[str, object]] = []
    station_raw_elements: dict[str, list[str]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        station_id = _clean_string(item.get('id'))
        if len(station_id) != 8 or not station_id.isdigit():
            continue
        parameter_ranges = _supported_parameter_ranges(item.get('parametres'))
        if not parameter_ranges:
            continue
        position = _select_position(item.get('positions'))
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': normalize_fr_metadata_datetime(item.get('dateDebut'), default=''),
                'end_date': normalize_fr_metadata_datetime(item.get('dateFin'), default='3999-12-31T23:59Z'),
                'full_name': _clean_string(item.get('nom')) or pd.NA,
                'longitude': _parse_float(position.get('longitude')),
                'latitude': _parse_float(position.get('latitude')),
                'elevation_m': _parse_float(position.get('altitude')),
            }
        )
        station_raw_elements[station_id] = sorted(parameter_ranges)

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        ('meteo_france', 'daily'): station_raw_elements,
    }
    return frame


def normalize_fr_observation_metadata(
    payload: list[dict[str, object]],
    spec,
    parameter_metadata: dict[str, dict[str, str]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        station_id = _clean_string(item.get('id'))
        if len(station_id) != 8 or not station_id.isdigit():
            continue
        parameter_ranges = _supported_parameter_ranges(item.get('parametres'))
        for raw_code, ranges in sorted(parameter_ranges.items()):
            metadata = parameter_metadata.get(raw_code, {})
            begin_date = ''
            end_date = ''
            if ranges:
                begin_values = [normalize_fr_metadata_datetime(entry.get('dateDebut'), default='') for entry in ranges]
                end_values = [
                    normalize_fr_metadata_datetime(entry.get('dateFin'), default='3999-12-31T23:59Z')
                    for entry in ranges
                ]
                begin_values = [value for value in begin_values if value]
                end_values = [value for value in end_values if value]
                begin_date = min(begin_values) if begin_values else ''
                end_date = max(end_values) if end_values else ''
            rows.append(
                {
                    'obs_type': metadata.get('obs_type', 'HISTORICAL_DAILY'),
                    'station_id': station_id,
                    'begin_date': begin_date,
                    'end_date': end_date,
                    'element': raw_code,
                    'schedule': metadata.get('schedule', 'P1D Meteo-France RR-T-Vent daily'),
                    'name': metadata.get('name', raw_code),
                    'description': metadata.get('description', pd.NA),
                    'height': pd.NA,
                }
            )
    frame = pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(['station_id', 'element', 'begin_date'], kind='stable').reset_index(drop=True)


def parse_fr_daily_csv(csv_text: str) -> pd.DataFrame:
    reader = csv.DictReader(io.StringIO(csv_text.lstrip('\ufeff')), delimiter=';')
    rows = []
    for row in reader:
        cleaned = {_clean_string(key): _clean_string(value) for key, value in row.items() if key is not None}
        if not cleaned.get('NUM_POSTE'):
            continue
        rows.append(cleaned)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame.from_records(rows)


def normalize_fr_daily_observations(
    table: pd.DataFrame,
    query,
) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=FR_NORMALIZED_DAILY_COLUMNS)

    filtered = table.copy()
    filtered['NUM_POSTE'] = filtered['NUM_POSTE'].astype('string').str.strip()
    filtered = filtered[filtered['NUM_POSTE'].isin(query.station_ids)]
    if filtered.empty:
        return pd.DataFrame(columns=FR_NORMALIZED_DAILY_COLUMNS)

    observation_dates = pd.to_datetime(filtered['AAAAMMJJ'], format='%Y%m%d', errors='coerce').dt.date
    filtered = filtered.assign(_observation_date=observation_dates)
    filtered = filtered[filtered['_observation_date'].notna()]
    if filtered.empty:
        return pd.DataFrame(columns=FR_NORMALIZED_DAILY_COLUMNS)

    if query.start_date is not None:
        filtered = filtered[filtered['_observation_date'] >= query.start_date]
    if query.end_date is not None:
        filtered = filtered[filtered['_observation_date'] <= query.end_date]
    if filtered.empty:
        return pd.DataFrame(columns=FR_NORMALIZED_DAILY_COLUMNS)

    rows: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        if raw_code not in filtered.columns:
            continue
        normalized = pd.DataFrame(
            {
                'station_id': filtered['NUM_POSTE'].astype('string'),
                'gh_id': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                'element': _canonical_element_for_raw(raw_code),
                'element_raw': raw_code,
                'observation_date': filtered['_observation_date'],
                'time_function': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                'value': _to_numeric_series(filtered[raw_code]),
                'flag': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                'quality': pd.Series(pd.NA, index=filtered.index, dtype='Int64'),
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )
        normalized = normalized[normalized['value'].notna()]
        if not normalized.empty:
            rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=FR_NORMALIZED_DAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, FR_NORMALIZED_DAILY_COLUMNS]


def decode_fr_daily_payload(payload: bytes) -> str:
    raw_bytes = gzip.decompress(payload) if payload[:2] == b'\x1f\x8b' else payload
    return _decode_french_text(raw_bytes)


def normalize_fr_metadata_datetime(value: object, *, default: str) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return default
    parsed = pd.to_datetime(cleaned, errors='coerce', utc=True)
    if pd.isna(parsed):
        return default
    return parsed.strftime('%Y-%m-%dT%H:%MZ')


def _supported_parameter_ranges(parametres: object) -> dict[str, list[dict[str, object]]]:
    supported: dict[str, list[dict[str, object]]] = {}
    if not isinstance(parametres, list):
        return supported
    by_name = {label: raw_code for raw_code, label in FR_SUPPORTED_PARAMETER_NAMES.items()}
    for item in parametres:
        if not isinstance(item, dict):
            continue
        raw_code = by_name.get(_clean_string(item.get('nom')).upper())
        if raw_code is None:
            continue
        supported.setdefault(raw_code, []).append(item)
    return supported


def _select_position(positions: object) -> dict[str, object]:
    if not isinstance(positions, list) or not positions:
        return {}
    candidates = [item for item in positions if isinstance(item, dict)]
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda item: (
            normalize_fr_metadata_datetime(item.get('dateFin'), default='3999-12-31T23:59Z'),
            normalize_fr_metadata_datetime(item.get('dateDebut'), default=''),
        ),
    )


def _canonical_element_for_raw(raw_code: str) -> str:
    return {
        'RR': 'precipitation',
        'TN': 'tas_min',
        'TX': 'tas_max',
        'TM': 'tas_mean',
    }[raw_code]


def _to_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype('string').str.strip()
    cleaned = cleaned.where(~cleaned.isin(_FR_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned, errors='coerce')


def _decode_french_text(payload: bytes) -> str:
    for encoding in ('utf-8-sig', 'cp1252', 'latin-1'):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode('utf-8', errors='replace')


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    return float(cleaned)
