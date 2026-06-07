from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

LV_NORMALIZED_HOURLY_COLUMNS = [
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

LV_NORMALIZED_DAILY_COLUMNS = [
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
_AGGREGATION_KIND_BY_RAW = {
    'HTDRY': 'mean',
    'HATMX': 'max',
    'HATMN': 'min',
    'HPRAB': 'sum',
    'HWNDS': 'mean',
    'HWSMX': 'max',
    'HRLH': 'mean',
    'HPRSL': 'mean',
    'HSNOW': 'last',
}


def read_text_from_source(source: str, timeout: int, params: dict[str, object] | None = None) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def parse_lvgmc_payload_json(text: str) -> object:
    try:
        return json.loads(text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('LVGMC response is not valid JSON.') from exc


def normalize_lvgmc_parameter_metadata(records: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    metadata_by_abbreviation: dict[str, dict[str, object]] = {}
    for record in records:
        abbreviation = _clean_string(record.get('ABBREVIATION')).upper()
        if not abbreviation:
            continue
        metadata_by_abbreviation[abbreviation] = {
            'abbreviation': abbreviation,
            'name': _clean_string(record.get('EN_DESCRIPTION')) or _clean_string(record.get('LV_DESCRIPTION')),
            'description_en': _clean_string(record.get('EN_DESCRIPTION')),
            'description_lv': _clean_string(record.get('LV_DESCRIPTION')),
            'scale': _parse_optional_float(record.get('SCALE')),
            'lower_limit': _parse_optional_float(record.get('LOWER_LIMIT')),
            'upper_limit': _parse_optional_float(record.get('UPPER_LIMIT')),
            'unit': _clean_string(record.get('MEASUREMENT_UNIT')),
        }
    return metadata_by_abbreviation


def normalize_lvgmc_station_metadata(
    records: list[dict[str, object]],
    spec: object,
    *,
    parameter_metadata: dict[str, dict[str, object]],
    active_only: bool,
) -> pd.DataFrame:
    supported_raw_elements = [raw_code for raw_code in getattr(spec, 'supported_elements', ()) if raw_code in parameter_metadata]
    rows: list[dict[str, object]] = []
    station_elements: dict[str, list[str]] = {}
    for record in records:
        station_id = _clean_string(record.get('STATION_ID'))
        if not station_id:
            continue
        end_date = _normalize_metadata_datetime(record.get('END_DATE'), default=_OPEN_END)
        if active_only and not end_date.startswith('3999-12-31'):
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': _normalize_metadata_datetime(record.get('BEGIN_DATE'), default=''),
                'end_date': end_date,
                'full_name': _clean_string(record.get('NAME')) or pd.NA,
                'longitude': _parse_optional_float(record.get('GEOGR1')),
                'latitude': _parse_optional_float(record.get('GEOGR2')),
                'elevation_m': _parse_optional_float(record.get('ELEVATION')),
            }
        )
        station_elements[station_id] = supported_raw_elements

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    existing_attrs = frame.attrs.get('station_provider_raw_elements_by_path', {})
    frame.attrs['station_provider_raw_elements_by_path'] = {
        **existing_attrs,
        (spec.provider, spec.resolution): station_elements,
    }
    return frame


def normalize_lvgmc_observation_metadata(
    station_records: list[dict[str, object]],
    spec: object,
    *,
    parameter_metadata: dict[str, dict[str, object]],
    active_only: bool,
) -> pd.DataFrame:
    if spec.resolution == '1hour':
        obs_type = 'HISTORICAL_1HOUR'
        schedule = 'PT1H LVGMC recent archive hourly observations using UTC period-end timestamps that represent the preceding hour'
    else:
        obs_type = 'HISTORICAL_DAILY'
        schedule = 'P1D LVGMC recent archive daily aggregation from hourly UTC period-end records'
    rows: list[dict[str, object]] = []
    for record in station_records:
        station_id = _clean_string(record.get('STATION_ID'))
        if not station_id:
            continue
        end_date = _normalize_metadata_datetime(record.get('END_DATE'), default=_OPEN_END)
        if active_only and not end_date.startswith('3999-12-31'):
            continue
        begin_date = _normalize_metadata_datetime(record.get('BEGIN_DATE'), default='')
        for raw_code in getattr(spec, 'supported_elements', ()):
            metadata = parameter_metadata.get(raw_code)
            if metadata is None:
                continue
            rows.append(
                {
                    'obs_type': obs_type,
                    'station_id': station_id,
                    'begin_date': begin_date,
                    'end_date': end_date,
                    'element': raw_code,
                    'schedule': schedule,
                    'name': metadata['name'],
                    'description': _metadata_description(raw_code, metadata, spec.resolution),
                    'height': pd.NA,
                }
            )

    frame = pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(['station_id', 'element'], kind='stable').reset_index(drop=True)


def normalize_lvgmc_hourly_records(
    records: list[dict[str, object]],
    *,
    query,
    station_id: str,
    start_timestamp,
    end_timestamp,
) -> pd.DataFrame:
    start = pd.to_datetime(start_timestamp, utc=True, errors='coerce')
    end = pd.to_datetime(end_timestamp, utc=True, errors='coerce')
    rows: list[dict[str, object]] = []
    for record in records:
        if _clean_string(record.get('STATION_ID')).upper() != station_id.upper():
            continue
        raw_code = _clean_string(record.get('ABBREVIATION')).upper()
        if raw_code not in query.elements:
            continue
        timestamp = pd.to_datetime(record.get('DATETIME'), utc=True, errors='coerce')
        if pd.isna(timestamp):
            continue
        represented_timestamp = hourly_period_timestamp(timestamp)
        if represented_timestamp < start or represented_timestamp > end:
            continue
        value = _parse_optional_float(record.get('VALUE'))
        if value is None:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'timestamp': represented_timestamp,
                'value': float(value),
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=LV_NORMALIZED_HOURLY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series([pd.NA] * len(frame), dtype='Int64')
    frame = frame.drop_duplicates(subset=['station_id', 'element_raw', 'timestamp'], keep='last')
    return frame.sort_values(['station_id', 'timestamp', 'element_raw'], kind='stable').reset_index(drop=True)


def normalize_lvgmc_daily_records(
    records: list[dict[str, object]],
    *,
    query,
    station_id: str,
    observation_date,
) -> pd.DataFrame:
    grouped_values: dict[str, list[tuple[pd.Timestamp, float]]] = {raw_code: [] for raw_code in query.elements}
    for record in records:
        if _clean_string(record.get('STATION_ID')).upper() != station_id.upper():
            continue
        raw_code = _clean_string(record.get('ABBREVIATION')).upper()
        if raw_code not in grouped_values:
            continue
        timestamp = pd.to_datetime(record.get('DATETIME'), utc=True, errors='coerce')
        if pd.isna(timestamp):
            continue
        if hourly_period_date(timestamp) != observation_date:
            continue
        value = _parse_optional_float(record.get('VALUE'))
        if value is None:
            continue
        grouped_values[raw_code].append((timestamp, value))

    rows: list[dict[str, object]] = []
    for raw_code in query.elements:
        aggregated_value = _aggregate_raw_values(raw_code, grouped_values.get(raw_code, []))
        if aggregated_value is None:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': float(aggregated_value),
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=LV_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series([pd.NA] * len(frame), dtype='Int64')
    return frame.sort_values(['station_id', 'observation_date', 'element_raw'], kind='stable').reset_index(drop=True)


def hourly_period_date(timestamp: pd.Timestamp):
    return hourly_period_timestamp(timestamp).date()


def hourly_period_timestamp(timestamp: pd.Timestamp) -> pd.Timestamp:
    return timestamp - pd.Timedelta(hours=1)


def _aggregate_raw_values(raw_code: str, values: list[tuple[pd.Timestamp, float]]) -> float | None:
    if not values:
        return None
    kind = _AGGREGATION_KIND_BY_RAW[raw_code]
    numeric_values = [value for _, value in values]
    if kind == 'mean':
        return float(sum(numeric_values) / len(numeric_values))
    if kind == 'max':
        return float(max(numeric_values))
    if kind == 'min':
        return float(min(numeric_values))
    if kind == 'sum':
        return float(sum(numeric_values))
    if kind == 'last':
        return float(sorted(values, key=lambda item: item[0])[-1][1])
    return None


def _metadata_description(raw_code: str, metadata: dict[str, object], resolution: str) -> str:
    unit = _clean_string(metadata.get('unit'))
    if resolution == 'daily' and raw_code == 'HSNOW':
        return f"{metadata['name']}. WeatherDownload keeps the last non-null value assigned to the UTC day. Source unit: {unit}."
    if resolution == 'daily':
        return f"{metadata['name']}. Source unit: {unit}."
    if raw_code == 'HSNOW':
        return f"{metadata['name']}. LVGMC timestamps are period-end timestamps, so WeatherDownload exposes the preceding UTC hour as the hourly timestamp. Source unit: {unit}."
    return f"{metadata['name']}. Source unit: {unit}."


def _normalize_metadata_datetime(value: object, *, default: str) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return default
    timestamp = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(timestamp):
        return default
    return timestamp.strftime('%Y-%m-%dT%H:%MZ')


def _parse_optional_float(value: object) -> float | None:
    if value is None or value is pd.NA:
        return None
    numeric = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def _clean_string(value: object) -> str:
    if value is None or value is pd.NA:
        return ''
    try:
        if pd.isna(value):
            return ''
    except TypeError:
        pass
    return str(value).strip()
