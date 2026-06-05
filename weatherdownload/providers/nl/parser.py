from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import KNMI_PARAMETER_METADATA

KNMI_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]
KNMI_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'provider', 'resolution',
]

_STATION_HEADER_RE = re.compile(r'^#\s*STN\b', re.IGNORECASE)
_STATION_ROW_RE = re.compile(
    r'^#\s*(?P<station_id>\d+)\s+'
    r'(?P<longitude>-?\d+(?:\.\d+)?)\s+'
    r'(?P<latitude>-?\d+(?:\.\d+)?)\s+'
    r'(?P<elevation>-?\d+(?:\.\d+)?)\s+'
    r'(?P<name>.+?)\s*$'
)
_VARIABLE_RE = re.compile(r'^#\s*(?P<code>[A-Z0-9]+)\s*:\s*(?P<description>.+?)\s*$')


@dataclass(frozen=True)
class ParsedKnmiDailyText:
    stations: pd.DataFrame
    table: pd.DataFrame
    variable_metadata: dict[str, str]


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.post(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def parse_knmi_daily_text(text: str) -> ParsedKnmiDailyText:
    normalized_text = text.lstrip('\ufeff').replace('\r\n', '\n')
    lines = [line.rstrip() for line in normalized_text.split('\n') if line.strip()]
    if not lines:
        raise ValueError('KNMI daily response is empty.')

    station_lines: list[str] = []
    variable_metadata: dict[str, str] = {}
    data_header: list[str] | None = None
    data_rows: list[str] = []
    in_station_block = False
    in_data_block = False

    for line in lines:
        if re.match(r'^#\s*STN\s*,\s*YYYYMMDD\b', line, re.IGNORECASE):
            data_header = [column.strip() for column in next(csv.reader([line[1:].strip()]))]
            in_station_block = False
            in_data_block = True
            continue
        if _STATION_HEADER_RE.match(line):
            in_station_block = True
            continue
        variable_match = _VARIABLE_RE.match(line)
        if variable_match is not None:
            in_station_block = False
            variable_metadata[variable_match.group('code')] = variable_match.group('description').strip()
            continue
        if in_station_block and line.startswith('#'):
            station_lines.append(line)
            continue
        if in_data_block and not line.startswith('#'):
            data_rows.append(line)

    if data_header is None:
        raise ValueError('KNMI daily response is missing the STN,YYYYMMDD data header.')

    stations = _parse_station_lines(station_lines)
    table = _parse_data_rows(data_header, data_rows)
    return ParsedKnmiDailyText(stations=stations, table=table, variable_metadata=variable_metadata)


def normalize_knmi_station_metadata(parsed: ParsedKnmiDailyText) -> pd.DataFrame:
    frame = parsed.stations.copy()
    if frame.empty:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    return frame.sort_values('station_id').reset_index(drop=True)


def normalize_knmi_observation_metadata(parsed: ParsedKnmiDailyText, spec: object) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in parsed.stations.itertuples(index=False):
        for raw_code in getattr(spec, 'supported_elements', ()):
            metadata = KNMI_PARAMETER_METADATA.get(raw_code, {})
            description = metadata.get('description') or parsed.variable_metadata.get(raw_code, pd.NA)
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': 'P1D KNMI daggegevens public daily CSV',
                    'name': metadata.get('name', raw_code),
                    'description': description,
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def normalize_knmi_daily_rows(
    parsed: ParsedKnmiDailyText,
    *,
    raw_code: str,
    provider: str,
    resolution: str,
    station_ids: set[str],
    start_date,
    end_date,
) -> pd.DataFrame:
    required_columns = {'STN', 'YYYYMMDD', raw_code}
    missing_columns = sorted(required_columns - set(parsed.table.columns))
    if missing_columns:
        raise ValueError(f'KNMI daily response is missing required columns: {missing_columns}')

    table = parsed.table.copy()
    table['station_id'] = table['STN'].astype('string').str.strip()
    table = table[table['station_id'].isin(station_ids)].copy()
    if table.empty:
        return pd.DataFrame(columns=KNMI_NORMALIZED_DAILY_COLUMNS)

    table['observation_date'] = pd.to_datetime(table['YYYYMMDD'], format='%Y%m%d', errors='coerce').dt.date
    table = table[table['observation_date'].notna()].copy()
    if start_date is not None:
        table = table[table['observation_date'] >= start_date]
    if end_date is not None:
        table = table[table['observation_date'] <= end_date]
    if table.empty:
        return pd.DataFrame(columns=KNMI_NORMALIZED_DAILY_COLUMNS)

    rows: list[dict[str, object]] = []
    for row in table.itertuples(index=False):
        rows.append(
            {
                'station_id': row.station_id,
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'observation_date': row.observation_date,
                'time_function': pd.NA,
                'value': _convert_daily_value(raw_code, getattr(row, raw_code)),
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': provider,
                'resolution': resolution,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=KNMI_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series(frame['quality'], dtype='Int64')
    return frame.sort_values(['station_id', 'observation_date', 'element_raw']).reset_index(drop=True)


def _parse_station_lines(lines: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for line in lines:
        match = _STATION_ROW_RE.match(line)
        if match is None:
            continue
        rows.append(
            {
                'station_id': match.group('station_id'),
                'gh_id': pd.NA,
                'begin_date': '',
                'end_date': '',
                'full_name': match.group('name').strip() or pd.NA,
                'longitude': float(match.group('longitude')),
                'latitude': float(match.group('latitude')),
                'elevation_m': float(match.group('elevation')),
            }
        )
    return pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)


def _parse_data_rows(header: list[str], rows: list[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=header)
    table = pd.read_csv(StringIO('\n'.join(rows)), header=None, names=header, dtype='string', skipinitialspace=True)
    for column in table.columns:
        table[column] = table[column].astype('string').str.strip()
    return table


def _convert_daily_value(raw_code: str, value: object):
    cleaned = _clean_string(value)
    if not cleaned:
        return pd.NA
    numeric = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return pd.NA
    numeric = float(numeric)
    if raw_code in {'TG', 'TX', 'TN', 'FG', 'FXX', 'PG'}:
        return numeric * 0.1
    if raw_code == 'RH':
        return 0.0 if numeric == -1.0 else numeric * 0.1
    if raw_code == 'SQ':
        return 0.0 if numeric == -1.0 else numeric * 0.1
    if raw_code == 'Q':
        return numeric * 0.01
    return numeric


def _clean_string(value: object) -> str:
    if value is None or value is pd.NA:
        return ''
    try:
        if pd.isna(value):
            return ''
    except TypeError:
        pass
    return str(value).strip()
