from __future__ import annotations

import json
import re
from urllib.parse import unquote

import pandas as pd


# SHMU currently exposes simple Apache-style directory listings with href entries.
# We intentionally parse only href values and ignore decorative listing text.
_DIRECTORY_HREF_RE = re.compile(r'href="([^"]+)"')
_MONTH_DIRECTORY_RE = re.compile(r'^\d{4}-\d{2}/$')
_MONTH_FILE_RE = re.compile(r'^kli-inter - (?P<year_month>\d{4}-\d{2})\.json$')
_NOW_FILE_RE = re.compile(r'^aws1min - .*\.json$')

# Experimental SHMU recent/daily JSON assumptions are isolated here so any upstream
# field-name drift is detected in one place instead of being spread across provider code.
SHMU_RECENT_DAILY_STATION_ID_COLUMN = 'ind_kli'
SHMU_RECENT_DAILY_DATE_COLUMN = 'datum'
SHMU_RECENT_DAILY_REQUIRED_COLUMNS = (
    SHMU_RECENT_DAILY_STATION_ID_COLUMN,
    SHMU_RECENT_DAILY_DATE_COLUMN,
)

# Experimental SHMU now/aws1min JSON assumptions stay here even though that path is not
# implemented yet. This keeps source-format probing logic localized.
SHMU_NOW_STATION_ID_COLUMN = 'ind_kli'
SHMU_NOW_TIMESTAMP_COLUMN = 'minuta'
SHMU_NOW_REQUIRED_COLUMNS = (
    SHMU_NOW_STATION_ID_COLUMN,
    SHMU_NOW_TIMESTAMP_COLUMN,
)

# SHMU metadata JSON currently exposes column descriptions under these keys.
SHMU_METADATA_NAME_COLUMN = 'm_column_name'
SHMU_METADATA_DESCRIPTION_COLUMN = 'popis'
SHMU_METADATA_UNIT_COLUMN = 'unit'

# SHMU recent/daily numeric fields currently represent missing observations as blank strings.
# We do not guess any other missing-value sentinel until SHMU documents one explicitly.
SHMU_BLANK_MISSING_VALUES = {'', ' ', None}


def parse_apache_directory_listing(html_text: str) -> list[str]:
    entries: list[str] = []
    seen: set[str] = set()
    for href in _DIRECTORY_HREF_RE.findall(html_text):
        decoded = unquote(href)
        if decoded in {'../', '/'}:
            continue
        if decoded.startswith('?'):
            continue
        if decoded not in seen:
            seen.add(decoded)
            entries.append(decoded)
    return entries


def parse_recent_daily_month_directories(html_text: str) -> list[str]:
    return sorted(entry.rstrip('/') for entry in parse_apache_directory_listing(html_text) if _MONTH_DIRECTORY_RE.match(entry))


def parse_recent_daily_month_files(html_text: str) -> list[str]:
    return sorted(entry for entry in parse_apache_directory_listing(html_text) if _MONTH_FILE_RE.match(entry))


def parse_now_data_files(html_text: str) -> list[str]:
    return sorted(entry for entry in parse_apache_directory_listing(html_text) if _NOW_FILE_RE.match(entry))


def parse_shmu_metadata_json(json_text: str) -> pd.DataFrame:
    payload = _load_json_object(json_text, context='SHMU metadata')
    items = payload.get('data')
    if not isinstance(items, list):
        raise ValueError('SHMU metadata JSON is missing a "data" list.')
    rows: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_name = _clean_string(item.get(SHMU_METADATA_NAME_COLUMN))
        if not raw_name:
            continue
        rows.append(
            {
                'element_raw': raw_name,
                'description': _clean_string(item.get(SHMU_METADATA_DESCRIPTION_COLUMN)) or pd.NA,
                'unit': _clean_string(item.get(SHMU_METADATA_UNIT_COLUMN)) or pd.NA,
            }
        )
    return pd.DataFrame.from_records(rows, columns=['element_raw', 'description', 'unit'])


def parse_recent_daily_payload_json(json_text: str) -> tuple[dict[str, object], pd.DataFrame]:
    return _parse_shmu_payload_json(
        json_text,
        context='SHMU recent daily observations',
        required_columns=SHMU_RECENT_DAILY_REQUIRED_COLUMNS,
    )


def parse_now_payload_json(json_text: str) -> tuple[dict[str, object], pd.DataFrame]:
    return _parse_shmu_payload_json(
        json_text,
        context='SHMU now/aws1min observations',
        required_columns=SHMU_NOW_REQUIRED_COLUMNS,
    )


def extract_recent_daily_station_ids(table: pd.DataFrame) -> list[str]:
    _require_columns(table, SHMU_RECENT_DAILY_REQUIRED_COLUMNS, context='SHMU recent daily table')
    return sorted({station_id for station_id in table[SHMU_RECENT_DAILY_STATION_ID_COLUMN].map(normalize_shmu_station_id) if station_id})


def extract_recent_daily_begin_end_dates(table: pd.DataFrame) -> tuple[str, str]:
    _require_columns(table, SHMU_RECENT_DAILY_REQUIRED_COLUMNS, context='SHMU recent daily table')
    return normalize_shmu_begin_end_dates(table[SHMU_RECENT_DAILY_DATE_COLUMN])


def extract_recent_daily_station_date_ranges(table: pd.DataFrame) -> pd.DataFrame:
    """Derive per-station begin/end coverage from the SHMU recent/daily payload.

    The current SHMU source does not expose authoritative station metadata coverage.
    The only conservative coverage signal we use is the station's own observed `datum`
    range within the sampled recent/daily payload.
    """
    _require_columns(table, SHMU_RECENT_DAILY_REQUIRED_COLUMNS, context='SHMU recent daily table')
    working = table.copy()
    working['station_id'] = working[SHMU_RECENT_DAILY_STATION_ID_COLUMN].map(normalize_shmu_station_id)
    grouped_rows: list[dict[str, object]] = []
    for station_id, station_table in working.groupby('station_id', sort=True):
        if not station_id:
            continue
        begin_date, end_date = normalize_shmu_begin_end_dates(station_table[SHMU_RECENT_DAILY_DATE_COLUMN])
        grouped_rows.append(
            {
                'station_id': station_id,
                'begin_date': begin_date,
                'end_date': end_date,
            }
        )
    return pd.DataFrame.from_records(grouped_rows, columns=['station_id', 'begin_date', 'end_date'])


def normalize_recent_daily_long_table(raw_table: pd.DataFrame, raw_codes: list[str]) -> pd.DataFrame:
    _require_columns(raw_table, SHMU_RECENT_DAILY_REQUIRED_COLUMNS, context='SHMU recent daily table')
    working = raw_table.copy()

    # Station identifiers are currently carried in `ind_kli`. We normalize them to trimmed
    # strings and intentionally do not infer names or other metadata from them.
    working['station_id'] = working[SHMU_RECENT_DAILY_STATION_ID_COLUMN].map(normalize_shmu_station_id)

    # SHMU currently exposes the daily key under `datum`, which we interpret as a calendar
    # date without any time-of-day semantics.
    working['observation_date'] = normalize_shmu_observation_date(working[SHMU_RECENT_DAILY_DATE_COLUMN])

    frames: list[pd.DataFrame] = []
    for raw_code in raw_codes:
        if raw_code not in working.columns:
            continue
        subset = working[['station_id', 'observation_date', raw_code]].copy()
        subset['element_raw'] = raw_code
        subset['value'] = normalize_shmu_numeric_series(subset[raw_code])
        subset = subset.drop(columns=[raw_code])
        frames.append(subset)
    if not frames:
        return pd.DataFrame(columns=['station_id', 'observation_date', 'element_raw', 'value'])
    return pd.concat(frames, ignore_index=True)


def normalize_shmu_station_id(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(int(value)) if isinstance(value, float) and value.is_integer() else str(value).strip()


def normalize_shmu_observation_date(series: pd.Series) -> pd.Series:
    # SHMU recent/daily currently uses ISO-like YYYY-MM-DD strings in `datum`.
    # Any unparsable value is coerced to missing rather than guessed.
    return pd.to_datetime(series, errors='coerce').dt.date


def normalize_shmu_begin_end_dates(series: pd.Series) -> tuple[str, str]:
    timestamps = pd.to_datetime(series, errors='coerce')
    timestamps = timestamps.dropna()
    if timestamps.empty:
        return '', ''
    begin = timestamps.min().strftime('%Y-%m-%dT00:00Z')
    end = timestamps.max().strftime('%Y-%m-%dT00:00Z')
    return begin, end


def normalize_shmu_numeric_series(series: pd.Series) -> pd.Series:
    # SHMU recent/daily numeric payloads are JSON strings today. Blank strings are treated as
    # missing values; otherwise we rely on pandas numeric coercion and avoid guessing units.
    cleaned = series.map(lambda value: pd.NA if value in SHMU_BLANK_MISSING_VALUES else value)
    return pd.to_numeric(cleaned, errors='coerce')


def _parse_shmu_payload_json(
    json_text: str,
    *,
    context: str,
    required_columns: tuple[str, ...],
) -> tuple[dict[str, object], pd.DataFrame]:
    payload = _load_json_object(json_text, context=context)
    data_rows = payload.get('data')
    if not isinstance(data_rows, list):
        raise ValueError(f'{context} is missing a "data" list.')
    frame = pd.DataFrame(data_rows)
    if frame.empty:
        frame = pd.DataFrame(columns=list(required_columns))
    _require_columns(frame, required_columns, context=context)
    metadata = {
        'id': payload.get('id'),
        'dataset': payload.get('dataset'),
        'interval': payload.get('interval'),
        'frequency': payload.get('frequency'),
        'statistics': payload.get('statistics'),
    }
    return metadata, frame


def _require_columns(table: pd.DataFrame, required_columns: tuple[str, ...], *, context: str) -> None:
    missing = set(required_columns).difference(table.columns)
    if missing:
        raise ValueError(f'{context} is missing required columns: {sorted(missing)}')


def _load_json_object(json_text: str, *, context: str) -> dict[str, object]:
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f'{context} is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError(f'{context} must be a top-level JSON object.')
    return payload


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()

