from __future__ import annotations

import json
from io import StringIO
from typing import Any

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import (
    LU_DAILY_PARAMETER_METADATA,
    LU_METEOLUX_STATION_ID,
    LU_METEOLUX_STATION_ELEVATION_M,
    LU_METEOLUX_STATION_LATITUDE,
    LU_METEOLUX_STATION_LONGITUDE,
    LU_METEOLUX_STATION_NAME,
)


LU_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]


def parse_lu_feature_collection_json(json_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('MeteoLux INSPIRE WFS response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('MeteoLux INSPIRE WFS response must be a top-level JSON object.')
    features = payload.get('features')
    if not isinstance(features, list):
        raise ValueError('MeteoLux INSPIRE WFS response is missing a features list.')
    return payload


def normalize_lu_station_metadata() -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [
            {
                'station_id': LU_METEOLUX_STATION_ID,
                'gh_id': pd.NA,
                'begin_date': '1947-01-01T00:00Z',
                'end_date': '',
                'full_name': LU_METEOLUX_STATION_NAME,
                'longitude': LU_METEOLUX_STATION_LONGITUDE,
                'latitude': LU_METEOLUX_STATION_LATITUDE,
                'elevation_m': LU_METEOLUX_STATION_ELEVATION_M,
            }
        ],
        columns=STATION_METADATA_COLUMNS,
    )


def normalize_lu_observation_metadata(stations: pd.DataFrame, spec: Any) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in spec.supported_elements:
            metadata = LU_DAILY_PARAMETER_METADATA[raw_code]
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': 'P1D MeteoLux INSPIRE WFS',
                    'name': metadata['name'],
                    'description': metadata['description'],
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def normalize_lu_station_id(value: object) -> str:
    if value is None or pd.isna(value):
        return LU_METEOLUX_STATION_ID
    cleaned = str(value).strip()
    if not cleaned:
        return LU_METEOLUX_STATION_ID
    if cleaned == '06590':
        return LU_METEOLUX_STATION_ID
    if cleaned == LU_METEOLUX_STATION_ID:
        return LU_METEOLUX_STATION_ID
    return cleaned


def normalize_lu_daily_feature_rows(
    payload: dict[str, object],
    *,
    raw_code: str,
    provider: str,
    resolution: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for feature in payload.get('features', []):
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        if not isinstance(properties, dict):
            continue
        if not _is_findel_airport(properties):
            continue
        observation_date = _coerce_observation_date(properties)
        if observation_date is None:
            continue
        rows.append(
            {
                'station_id': _station_id_from_properties(properties),
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': pd.to_numeric(pd.Series([properties.get(raw_code)]), errors='coerce').iloc[0],
                'flag': _clean_optional_string(properties.get('qc_flag')) or pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': provider,
                'resolution': resolution,
            }
        )
    frame = pd.DataFrame.from_records(rows, columns=LU_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series(frame['quality'], dtype='Int64')
    frame = frame.sort_values(['station_id', 'observation_date', 'element_raw']).reset_index(drop=True)
    return frame


def parse_lu_daily_csv_text(csv_text: str) -> pd.DataFrame:
    normalized_text = csv_text.lstrip('\ufeff').replace('\r\n', '\n')
    lines = [line for line in normalized_text.split('\n') if line.strip()]
    if not lines:
        raise ValueError('MeteoLux daily CSV response is empty.')
    if lines[0].lower().startswith('sep='):
        lines = lines[1:]
    if not lines:
        raise ValueError('MeteoLux daily CSV response has no header row.')

    header_line = lines[0]
    delimiter = ';' if header_line.count(';') > header_line.count(',') else ','
    frame = pd.read_csv(StringIO('\n'.join(lines)), sep=delimiter, dtype='string')
    frame.columns = [_normalize_csv_header(column) for column in frame.columns]
    return frame


def normalize_lu_daily_csv_rows(
    frame: pd.DataFrame,
    *,
    raw_code: str,
    provider: str,
    resolution: str,
) -> pd.DataFrame:
    required_columns = {'DATE', raw_code}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(f'MeteoLux daily CSV is missing required columns: {missing_columns}')

    rows: list[dict[str, object]] = []
    for record in frame.to_dict(orient='records'):
        observation_date = _coerce_csv_observation_date(record.get('DATE'))
        if observation_date is None:
            continue
        rows.append(
            {
                'station_id': LU_METEOLUX_STATION_ID,
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': _parse_csv_numeric(record.get(raw_code)),
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': provider,
                'resolution': resolution,
            }
        )

    normalized = pd.DataFrame.from_records(rows, columns=LU_NORMALIZED_DAILY_COLUMNS)
    if normalized.empty:
        return normalized
    normalized['quality'] = pd.Series(normalized['quality'], dtype='Int64')
    return normalized.sort_values(['station_id', 'observation_date', 'element_raw']).reset_index(drop=True)


def _coerce_observation_date(properties: dict[str, object]) -> object | None:
    day_value = properties.get('day')
    if day_value not in (None, ''):
        day = pd.to_datetime(day_value, errors='coerce')
        if not pd.isna(day):
            return day.date()
    timestamp = pd.to_datetime(properties.get('datetime'), utc=True, errors='coerce')
    if pd.isna(timestamp):
        return None
    return timestamp.date()


def _coerce_csv_observation_date(value: object) -> object | None:
    cleaned = _clean_optional_string(value)
    if not cleaned:
        return None
    timestamp = pd.to_datetime(cleaned, format='%d.%m.%Y', errors='coerce')
    if pd.isna(timestamp):
        timestamp = pd.to_datetime(cleaned, errors='coerce')
    if pd.isna(timestamp):
        return None
    return timestamp.date()


def _station_id_from_properties(properties: dict[str, object]) -> str:
    for key in ('wigos_id', 'wigosid', 'station_id', 'stationid', 'wmo_id', 'wmoid'):
        value = properties.get(key)
        if value in (None, ''):
            continue
        station_id = normalize_lu_station_id(value)
        if station_id:
            return station_id
    return LU_METEOLUX_STATION_ID


def _is_findel_airport(properties: dict[str, object]) -> bool:
    station_name = _clean_optional_string(properties.get('name_descr'))
    if not station_name:
        return True
    normalized = station_name.casefold()
    return normalized in {'findel airport', 'luxembourg/findel airport'}


def _clean_optional_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _normalize_csv_header(value: object) -> str:
    header = _clean_optional_string(value)
    if not header:
        return header
    if ' (' in header:
        header = header.split(' (', 1)[0].strip()
    return header


def _parse_csv_numeric(value: object):
    cleaned = _clean_optional_string(value)
    if not cleaned:
        return pd.NA
    normalized = cleaned.replace(',', '.')
    return pd.to_numeric(pd.Series([normalized]), errors='coerce').iloc[0]
