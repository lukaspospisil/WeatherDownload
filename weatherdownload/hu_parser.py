from __future__ import annotations

import csv
import io
import re
from pathlib import Path

import pandas as pd

from .metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

HU_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
HU_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]

_HU_MISSING_SENTINELS = {'', '-999', '-999.0'}


def parse_hu_station_metadata_csv(csv_text: str) -> pd.DataFrame:
    reader = csv.reader(io.StringIO(csv_text.lstrip('\ufeff')), delimiter=';')
    try:
        raw_header = next(reader)
    except StopIteration:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)

    header = [_clean_string(cell) for cell in raw_header]
    rows: list[dict[str, object]] = []
    for row in reader:
        if len(row) < len(header):
            continue
        record = {column: _clean_string(value) for column, value in zip(header, row)}
        station_id = normalize_hu_station_id(record.get('StationNumber'))
        if not station_id:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': normalize_hu_metadata_date(record.get('StartDate')),
                'end_date': normalize_hu_metadata_date(record.get('EndDate')),
                'full_name': record.get('StationName') or pd.NA,
                'longitude': _parse_float(record.get('Longitude')),
                'latitude': _parse_float(record.get('Latitude')),
                'elevation_m': _parse_float(record.get('Elevation')),
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['_sort_id'] = frame['station_id'].map(_station_sort_key)
    frame = frame.sort_values(['_sort_id', 'begin_date', 'end_date']).drop(columns=['_sort_id']).reset_index(drop=True)
    return frame


def normalize_hu_observation_metadata(
    stations: pd.DataFrame,
    specs_and_metadata: list[tuple[object, dict[str, dict[str, str]]]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for spec, parameter_metadata in specs_and_metadata:
            for raw_code in getattr(spec, 'supported_elements', ()):  # pragma: no branch - tiny tuples
                metadata = parameter_metadata.get(raw_code, {})
                rows.append(
                    {
                        'obs_type': metadata.get('obs_type', 'HISTORICAL_DAILY'),
                        'station_id': station.station_id,
                        'begin_date': station.begin_date,
                        'end_date': station.end_date,
                        'element': raw_code,
                        'schedule': metadata.get('schedule', 'P1D HungaroMet HABP_1D'),
                        'name': metadata.get('name', raw_code),
                        'description': metadata.get('description', pd.NA),
                        'height': pd.NA,
                    }
                )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_hu_daily_csv(csv_text: str) -> pd.DataFrame:
    return _parse_hu_delimited_csv(csv_text)


def parse_hu_subdaily_csv(csv_text: str) -> pd.DataFrame:
    return _parse_hu_delimited_csv(csv_text)


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def parse_hu_directory_listing(html: str, pattern: re.Pattern[str]) -> list[dict[str, str]]:
    seen: set[tuple[tuple[str, str], ...]] = set()
    entries: list[dict[str, str]] = []
    for match in pattern.finditer(html):
        entry = dict(match.groupdict())
        key = tuple(sorted(entry.items()))
        if key in seen:
            continue
        seen.add(key)
        entries.append(entry)
    return entries


def normalize_hu_station_id(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    if cleaned.endswith('.0'):
        cleaned = cleaned[:-2]
    return cleaned


def normalize_hu_metadata_date(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    if len(cleaned) != 8 or not cleaned.isdigit():
        return cleaned
    return f'{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]}T00:00Z'


def normalize_hu_observation_date(value: object) -> object:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, format='%Y%m%d', errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def normalize_hu_observation_timestamp(value: object) -> pd.Timestamp | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, format='%Y%m%d%H%M', errors='coerce', utc=True)
    if pd.isna(parsed):
        return None
    return parsed


def normalize_hu_query_timestamp(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize('UTC')
    return timestamp.tz_convert('UTC')


def quality_column_for_element(raw_code: str) -> str:
    return f'Q_{raw_code}'


def to_numeric_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype('string').str.strip()
    cleaned = cleaned.where(~cleaned.isin(_HU_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned, errors='coerce')


def flag_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype('string').str.strip()
    return cleaned.where(cleaned.ne(''), pd.NA)


def _parse_hu_delimited_csv(csv_text: str) -> pd.DataFrame:
    lines = [line for line in csv_text.lstrip('\ufeff').splitlines() if not line.startswith('#')]
    if not lines:
        return pd.DataFrame()
    table = pd.read_csv(io.StringIO('\n'.join(lines)), sep=';', dtype=str)
    table.columns = [_normalize_hu_column_name(column) for column in table.columns]
    return table


def _normalize_hu_column_name(value: object) -> str:
    return _clean_string(value)


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    return float(cleaned)


def _station_sort_key(station_id: str) -> tuple[int, str]:
    return (int(station_id), station_id) if station_id.isdigit() else (10**9, station_id)
