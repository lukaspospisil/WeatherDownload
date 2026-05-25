from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import (
    IE_AUDITED_DAILY_STATIONS_PATH,
    IE_DAILY_PARAMETER_METADATA,
)


IE_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]
KNOT_TO_M_S = 0.514444


@dataclass(frozen=True)
class ParsedIrelandDailyCsv:
    metadata: dict[str, str]
    table: pd.DataFrame


def parse_ie_station_details_csv(csv_text: str) -> pd.DataFrame:
    reader = csv.DictReader(StringIO(csv_text.lstrip('\ufeff')))
    rows: list[dict[str, object]] = []
    for record in reader:
        station_id = _clean_string(record.get('station name'))
        full_name = _clean_string(record.get('name'))
        if not station_id or not full_name:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': _year_to_begin_date(record.get('open year')),
                'end_date': _year_to_end_date(record.get('close year')),
                'full_name': _title_name(full_name),
                'longitude': _parse_float(record.get('longitude')),
                'latitude': _parse_float(record.get('latitude')),
                'elevation_m': _parse_float(record.get('height(m)')),
            }
        )
    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['station_id'] = frame['station_id'].astype('string').str.strip()
    return frame.drop_duplicates(subset=['station_id'], keep='first').sort_values('station_id').reset_index(drop=True)


def normalize_ie_station_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    audited_station_ids = set(load_ie_audited_station_ids())
    if frame.empty:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    filtered = frame[frame['station_id'].isin(audited_station_ids)].copy()
    if filtered.empty:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    return filtered.sort_values('station_id').reset_index(drop=True)


def load_ie_audited_stations(path: Path = IE_AUDITED_DAILY_STATIONS_PATH) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding='utf-8'))
    stations = payload.get('stations', [])
    frame = pd.DataFrame.from_records(stations, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['station_id'] = frame['station_id'].astype('string').str.strip()
    return frame.sort_values('station_id').reset_index(drop=True)


def load_ie_audited_station_ids(path: Path = IE_AUDITED_DAILY_STATIONS_PATH) -> list[str]:
    frame = load_ie_audited_stations(path)
    if frame.empty:
        return []
    return frame['station_id'].astype(str).tolist()


def normalize_ie_observation_metadata(stations: pd.DataFrame, spec: object) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in getattr(spec, 'supported_elements', ()):
            metadata = IE_DAILY_PARAMETER_METADATA[raw_code]
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': 'P1D Met Eireann daily CSV (00 to 00 UTC)',
                    'name': metadata['name'],
                    'description': metadata['description'],
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_ie_daily_csv_text(csv_text: str) -> ParsedIrelandDailyCsv:
    normalized_text = csv_text.lstrip('\ufeff').replace('\r\n', '\n')
    lines = [line for line in normalized_text.split('\n') if line.strip()]
    if not lines:
        raise ValueError('Met Eireann daily CSV response is empty.')

    header_index: int | None = None
    for index, line in enumerate(lines):
        first_cell = next(csv.reader([line]))[0].strip().casefold() if line.strip() else ''
        if first_cell == 'date':
            header_index = index
            break
    if header_index is None:
        raise ValueError('Met Eireann daily CSV response is missing a date header row.')

    metadata: dict[str, str] = {}
    for line in lines[:header_index]:
        row = next(csv.reader([line]))
        if len(row) < 2:
            continue
        key = _normalize_metadata_key(row[0])
        value = ','.join(row[1:]).strip()
        if key:
            metadata[key] = value

    reader = csv.reader(StringIO('\n'.join(lines[header_index:])))
    raw_header = next(reader)
    header = _deduplicate_headers([column.strip().casefold() for column in raw_header])
    table = pd.DataFrame(list(reader), columns=header, dtype='string')
    for column in table.columns:
        table[column] = table[column].astype('string').str.strip()
    return ParsedIrelandDailyCsv(metadata=metadata, table=table)


def normalize_ie_daily_rows(
    parsed: ParsedIrelandDailyCsv,
    *,
    raw_code: str,
    provider: str,
    resolution: str,
    station_id: str,
    expected_station_name: str | None = None,
) -> pd.DataFrame:
    if expected_station_name:
        source_station_name = parsed.metadata.get('station_name', '').strip()
        if source_station_name and source_station_name.casefold() != expected_station_name.casefold():
            raise ValueError(
                f"Met Eireann daily CSV station mismatch: expected '{expected_station_name}' but got '{source_station_name}'."
            )

    required_columns = {'date', raw_code}
    missing_columns = sorted(required_columns - set(parsed.table.columns))
    if missing_columns:
        raise ValueError(f'Met Eireann daily CSV is missing required columns: {missing_columns}')

    rows: list[dict[str, object]] = []
    for record in parsed.table.to_dict(orient='records'):
        observation_date = _coerce_observation_date(record.get('date'))
        if observation_date is None:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': _convert_value(raw_code, _parse_numeric(record.get(raw_code))),
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': provider,
                'resolution': resolution,
            }
        )

    normalized = pd.DataFrame.from_records(rows, columns=IE_NORMALIZED_DAILY_COLUMNS)
    if normalized.empty:
        return normalized
    normalized['quality'] = pd.Series(normalized['quality'], dtype='Int64')
    return normalized.sort_values(['station_id', 'observation_date', 'element_raw']).reset_index(drop=True)


def _normalize_metadata_key(value: object) -> str:
    cleaned = _clean_string(value).casefold()
    if not cleaned:
        return ''
    return cleaned.replace(' ', '_')


def _deduplicate_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    normalized: list[str] = []
    for header in headers:
        base = header or 'unnamed'
        count = seen.get(base, 0) + 1
        seen[base] = count
        normalized.append(base if count == 1 else f'{base}_{count}')
    return normalized


def _coerce_observation_date(value: object):
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    for kwargs in (
        {'format': '%Y/%m/%d'},
        {'format': '%Y-%m-%d'},
        {'format': '%d/%m/%Y'},
        {'dayfirst': True},
        {},
    ):
        parsed = pd.to_datetime(cleaned, errors='coerce', **kwargs)
        if not pd.isna(parsed):
            return parsed.date()
    return None


def _parse_numeric(value: object):
    cleaned = _clean_string(value)
    if not cleaned:
        return pd.NA
    normalized = cleaned.replace(',', '.')
    return pd.to_numeric(pd.Series([normalized]), errors='coerce').iloc[0]


def _convert_value(raw_code: str, value: object):
    if value is pd.NA or pd.isna(value):
        return value
    if raw_code == 'wdsp':
        return float(value) * KNOT_TO_M_S
    return value


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value)
    if not cleaned or cleaned == '(null)':
        return None
    normalized = cleaned.replace(',', '.')
    parsed = pd.to_numeric(pd.Series([normalized]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _year_to_begin_date(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned or cleaned == '(null)':
        return ''
    if not cleaned.isdigit():
        return ''
    return f'{cleaned}-01-01T00:00Z'


def _year_to_end_date(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned or cleaned == '(null)':
        return ''
    if not cleaned.isdigit():
        return ''
    return f'{cleaned}-12-31T23:59Z'


def _title_name(value: str) -> str:
    lowered = value.strip().lower()
    return ' '.join(part.capitalize() for part in lowered.split())
