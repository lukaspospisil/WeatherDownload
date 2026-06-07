from __future__ import annotations

import csv
import io
import re
from calendar import monthrange
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests


BG_NIMH_NORMALIZED_DAILY_COLUMNS = [
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


def read_text_from_source(source: str, timeout: int = 60) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def parse_bg_month_links(html_text: str, *, slug: str) -> dict[tuple[int, int], str]:
    pattern = re.compile(rf'href=["\'](?P<href>[^"\']*mosv_{slug}_(?P<yyyymm>\d{{6}})\.csv)["\']', re.IGNORECASE)
    links: dict[tuple[int, int], str] = {}
    for match in pattern.finditer(html_text):
        year = int(match.group('yyyymm')[:4])
        month = int(match.group('yyyymm')[4:])
        links[(year, month)] = match.group('href')
    return dict(sorted(links.items()))


def build_source_url(base_source: str, href: str) -> str:
    if '://' in base_source:
        base = base_source if base_source.endswith('/') else f'{base_source}/'
        return urljoin(base, href)
    return str((Path(base_source).parent / href).resolve())


def related_open_data_source(source: str | None, *, target: str) -> str | None:
    if source is None:
        return None
    if '://' in source:
        return re.sub(r'/(rain|snow)/?$', f'/{target}', source.rstrip('/'))
    path = Path(source)
    name = path.name
    if 'rain' in name:
        return str(path.with_name(name.replace('rain', target)))
    if 'snow' in name:
        return str(path.with_name(name.replace('snow', target)))
    return None


def parse_bg_station_table(csv_text: str) -> pd.DataFrame:
    table = _parse_bg_month_csv_table(csv_text)
    if table.empty:
        return pd.DataFrame(columns=['station_id', 'full_name'])
    return table.loc[:, ['station_id', 'full_name']].drop_duplicates(subset=['station_id']).reset_index(drop=True)


def parse_bg_precipitation_month(csv_text: str) -> tuple[pd.DataFrame, list[int]]:
    table = _parse_bg_month_csv_table(csv_text)
    return table, _day_columns(table)


def parse_bg_snow_month(csv_text: str) -> tuple[pd.DataFrame, list[int]]:
    table = _parse_bg_month_csv_table(csv_text)
    return table, _day_columns(table)


def detect_latest_reported_day(table: pd.DataFrame, day_columns: list[int]) -> int:
    for day in sorted(day_columns, reverse=True):
        series = table[str(day)].astype('string').str.strip()
        if series.replace({'': pd.NA}).notna().any():
            return day
    return 0


def normalize_bg_precipitation_month(
    table: pd.DataFrame,
    *,
    year: int,
    month: int,
    provider: str,
    resolution: str,
) -> pd.DataFrame:
    day_columns = _day_columns(table)
    latest_reported_day = detect_latest_reported_day(table, day_columns)
    month_days = monthrange(year, month)[1]
    records: list[dict[str, object]] = []
    for row in table.to_dict(orient='records'):
        for day in day_columns:
            if day > month_days:
                continue
            raw_cell = row.get(str(day), '')
            cleaned = str(raw_cell).strip()
            if not cleaned and day > latest_reported_day:
                continue
            if cleaned.lower() == 'n.a.':
                value = pd.NA
                flag = 'n.a.'
            elif not cleaned:
                value = 0.0
                flag = pd.NA
            else:
                value = float(cleaned.rstrip('.'))
                flag = pd.NA
            records.append(
                {
                    'station_id': row['station_id'],
                    'gh_id': pd.NA,
                    'element': 'precipitation',
                    'element_raw': 'precipitation',
                    'observation_date': pd.Timestamp(year=year, month=month, day=day).date(),
                    'time_function': pd.NA,
                    'value': value,
                    'flag': flag,
                    'quality': pd.NA,
                    'provider': provider,
                    'resolution': resolution,
                }
            )
    return pd.DataFrame.from_records(records, columns=BG_NIMH_NORMALIZED_DAILY_COLUMNS)


def normalize_bg_snow_month(
    table: pd.DataFrame,
    *,
    year: int,
    month: int,
    provider: str,
    resolution: str,
) -> pd.DataFrame:
    day_columns = _day_columns(table)
    latest_reported_day = detect_latest_reported_day(table, day_columns)
    month_days = monthrange(year, month)[1]
    records: list[dict[str, object]] = []
    for row in table.to_dict(orient='records'):
        for day in day_columns:
            if day > month_days:
                continue
            raw_cell = row.get(str(day), '')
            cleaned = str(raw_cell).strip()
            if not cleaned and day > latest_reported_day:
                continue
            value, flag = _parse_snow_depth_value(cleaned)
            records.append(
                {
                    'station_id': row['station_id'],
                    'gh_id': pd.NA,
                    'element': 'snow_depth',
                    'element_raw': 'snow_cover_depth',
                    'observation_date': pd.Timestamp(year=year, month=month, day=day).date(),
                    'time_function': pd.NA,
                    'value': value,
                    'flag': flag,
                    'quality': pd.NA,
                    'provider': provider,
                    'resolution': resolution,
                }
            )
    return pd.DataFrame.from_records(records, columns=BG_NIMH_NORMALIZED_DAILY_COLUMNS)


def _parse_bg_month_csv_table(csv_text: str) -> pd.DataFrame:
    reader = csv.reader(io.StringIO(csv_text.lstrip('\ufeff')))
    rows = list(reader)
    if not rows:
        return pd.DataFrame(columns=['station_id', 'full_name'])
    header = rows[0]
    day_columns: list[tuple[int, int]] = []
    for idx in range(2, len(header)):
        token = header[idx].strip()
        if token.isdigit():
            day_columns.append((idx, int(token)))

    records: list[dict[str, object]] = []
    for row in rows[1:]:
        if len(row) < 2:
            continue
        station_id = row[0].strip()
        station_name = row[1].strip()
        if not station_id:
            continue
        record: dict[str, object] = {
            'station_id': station_id,
            'full_name': station_name,
        }
        for idx, day in day_columns:
            record[str(day)] = row[idx] if idx < len(row) else ''
        records.append(record)

    columns = ['station_id', 'full_name', *(str(day) for _, day in day_columns)]
    return pd.DataFrame.from_records(records, columns=columns)


def _day_columns(table: pd.DataFrame) -> list[int]:
    return [int(column) for column in table.columns if str(column).isdigit()]


def _parse_snow_depth_value(cleaned_cell: str) -> tuple[object, object]:
    if not cleaned_cell:
        return pd.NA, pd.NA
    if 'n.a' in cleaned_cell.lower():
        return pd.NA, 'n.a'
    if 'err' in cleaned_cell.lower():
        return pd.NA, 'err'
    parts = [part.strip() for part in cleaned_cell.split('|')]
    snow_token = parts[2] if len(parts) >= 3 else ''
    if not snow_token:
        return 0.0, pd.NA
    if re.fullmatch(r'\d+', snow_token):
        return float(snow_token), pd.NA
    if re.fullmatch(r'[pP]%\d+', snow_token):
        return pd.NA, snow_token
    return pd.NA, snow_token
