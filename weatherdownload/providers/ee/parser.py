from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

EE_ILMATEENISTUS_NORMALIZED_DAILY_COLUMNS = [
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
_MISSING_VALUES = {'', 'nan', 'NaN', '-999', '-9999'}


def read_text_from_source(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def parse_ee_payload_json(text: str) -> list[dict[str, object]]:
    try:
        payload = json.loads(text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('Estonian Environment Agency response is not valid JSON.') from exc
    if not isinstance(payload, list):
        raise ValueError('Estonian Environment Agency response must be a JSON array.')
    return [item for item in payload if isinstance(item, dict)]


def normalize_ee_station_metadata(records: list[dict[str, object]], spec: object) -> pd.DataFrame:
    rows_by_station: dict[str, dict[str, object]] = {}
    station_elements: dict[str, set[str]] = {}
    supported = set(getattr(spec, 'supported_elements', ()))
    for record in records:
        station_id = _normalize_station_id(record.get('jaam_kood'))
        raw_code = _clean_string(record.get('element_kood')).upper()
        if not station_id or raw_code not in supported:
            continue
        station_elements.setdefault(station_id, set()).add(raw_code)
        row = rows_by_station.setdefault(
            station_id,
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': '',
                'end_date': '',
                'full_name': _station_name(record) or pd.NA,
                'longitude': _parse_optional_float(record.get('pikkuskraad')),
                'latitude': _parse_optional_float(record.get('laiuskraad')),
                'elevation_m': _parse_optional_float(record.get('korgus_merepinnast_m')),
            },
        )
        begin_date = _normalize_metadata_datetime(
            record.get('jaam_periood_algus') or record.get('vaatlus_periood_algus'),
            default='',
        )
        end_date = _normalize_metadata_datetime(
            record.get('jaam_periood_lopp') or record.get('vaatlus_periood_lopp'),
            default=_OPEN_END,
        )
        if begin_date and (not row['begin_date'] or begin_date < row['begin_date']):
            row['begin_date'] = begin_date
        if end_date and (not row['end_date'] or end_date > row['end_date']):
            row['end_date'] = end_date

    frame = pd.DataFrame.from_records(list(rows_by_station.values()), columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        (spec.provider, spec.resolution): {
            station_id: sorted(raw_elements)
            for station_id, raw_elements in station_elements.items()
        }
    }
    return frame


def normalize_ee_observation_metadata(
    records: list[dict[str, object]],
    spec: object,
    parameter_metadata: dict[str, dict[str, str]],
) -> pd.DataFrame:
    rows_by_station_element: dict[tuple[str, str], dict[str, object]] = {}
    supported = set(getattr(spec, 'supported_elements', ()))
    for record in records:
        station_id = _normalize_station_id(record.get('jaam_kood'))
        raw_code = _clean_string(record.get('element_kood')).upper()
        if not station_id or raw_code not in supported:
            continue
        metadata = parameter_metadata[raw_code]
        key = (station_id, raw_code)
        row = rows_by_station_element.setdefault(
            key,
            {
                'obs_type': metadata['obs_type'],
                'station_id': station_id,
                'begin_date': '',
                'end_date': '',
                'element': raw_code,
                'schedule': metadata['schedule'],
                'name': metadata['name'],
                'description': f"{metadata['description']} Source unit: {metadata['unit']}.",
                'height': pd.NA,
            },
        )
        begin_date = _normalize_metadata_datetime(
            record.get('vaatlus_periood_algus') or record.get('jaam_periood_algus'),
            default='',
        )
        end_date = _normalize_metadata_datetime(
            record.get('vaatlus_periood_lopp') or record.get('jaam_periood_lopp'),
            default=_OPEN_END,
        )
        if begin_date and (not row['begin_date'] or begin_date < row['begin_date']):
            row['begin_date'] = begin_date
        if end_date and (not row['end_date'] or end_date > row['end_date']):
            row['end_date'] = end_date

    frame = pd.DataFrame.from_records(
        list(rows_by_station_element.values()),
        columns=STATION_OBSERVATION_METADATA_COLUMNS,
    )
    if frame.empty:
        return frame
    return frame.sort_values(['station_id', 'element'], kind='stable').reset_index(drop=True)


def normalize_ee_daily_rows(
    records: list[dict[str, object]],
    *,
    raw_code: str,
    provider: str,
    resolution: str,
    station_id: str,
    start_date,
    end_date,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for record in records:
        if _normalize_station_id(record.get('jaam_kood')) != station_id:
            continue
        if _clean_string(record.get('element_kood')).upper() != raw_code:
            continue
        observation_date = _parse_observation_date(record)
        if observation_date is None:
            continue
        if start_date is not None and observation_date < start_date:
            continue
        if end_date is not None and observation_date > end_date:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': _parse_observation_value(record.get('vaartus')),
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': provider,
                'resolution': resolution,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=EE_ILMATEENISTUS_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series([pd.NA] * len(frame), dtype='Int64')
    return frame.sort_values(['station_id', 'observation_date', 'element_raw'], kind='stable').reset_index(drop=True)


def _station_name(record: dict[str, object]) -> str:
    return _clean_string(record.get('jaam_nimi_eng')) or _clean_string(record.get('jaam_nimi'))


def _normalize_station_id(value: object) -> str:
    return _clean_string(value).upper()


def _normalize_metadata_datetime(value: object, *, default: str) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return default
    timestamp = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(timestamp):
        return default
    return timestamp.strftime('%Y-%m-%dT%H:%MZ')


def _parse_observation_date(record: dict[str, object]):
    year = _parse_optional_int(record.get('aasta'))
    month = _parse_optional_int(record.get('kuu'))
    day = _parse_optional_int(record.get('paev'))
    if year is None or month is None or day is None:
        return None
    return pd.Timestamp(year=year, month=month, day=day).date()


def _parse_observation_value(value: object):
    cleaned = _clean_string(value)
    if cleaned in _MISSING_VALUES:
        return pd.NA
    numeric = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
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
