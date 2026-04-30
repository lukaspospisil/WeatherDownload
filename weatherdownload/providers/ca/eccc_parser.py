from __future__ import annotations

import json
from datetime import date
from typing import Any

import pandas as pd


CA_ECCC_NORMALIZED_DAILY_COLUMNS = [
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

CA_ECCC_DAILY_RAW_TO_CANONICAL = {
    'MEAN_TEMPERATURE': 'tas_mean',
    'MAX_TEMPERATURE': 'tas_max',
    'MIN_TEMPERATURE': 'tas_min',
    'TOTAL_PRECIPITATION': 'precipitation',
}


def parse_ca_eccc_daily_feature_collection(json_text: str) -> pd.DataFrame:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('ECCC GeoMet response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('ECCC GeoMet response must be a top-level JSON object.')
    features = payload.get('features')
    if not isinstance(features, list):
        raise ValueError('ECCC GeoMet response is missing a features list.')

    rows: list[dict[str, object]] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        if not isinstance(properties, dict):
            continue
        station_id = normalize_ca_eccc_station_id(properties.get('CLIMATE_IDENTIFIER'))
        observation_date = parse_ca_eccc_local_date(properties.get('LOCAL_DATE'))
        if not station_id or observation_date is None:
            continue
        rows.append(
            {
                'station_id': station_id,
                'observation_date': observation_date,
                'MEAN_TEMPERATURE': _parse_float(properties.get('MEAN_TEMPERATURE')),
                'MEAN_TEMPERATURE_FLAG': _clean_string(properties.get('MEAN_TEMPERATURE_FLAG')) or pd.NA,
                'MAX_TEMPERATURE': _parse_float(properties.get('MAX_TEMPERATURE')),
                'MAX_TEMPERATURE_FLAG': _clean_string(properties.get('MAX_TEMPERATURE_FLAG')) or pd.NA,
                'MIN_TEMPERATURE': _parse_float(properties.get('MIN_TEMPERATURE')),
                'MIN_TEMPERATURE_FLAG': _clean_string(properties.get('MIN_TEMPERATURE_FLAG')) or pd.NA,
                'TOTAL_PRECIPITATION': _parse_float(properties.get('TOTAL_PRECIPITATION')),
                'TOTAL_PRECIPITATION_FLAG': _clean_string(properties.get('TOTAL_PRECIPITATION_FLAG')) or pd.NA,
            }
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame.from_records(rows)


def normalize_ca_eccc_daily_observations(
    table: pd.DataFrame,
    *,
    station_ids: list[str] | None = None,
    raw_elements: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    provider: str = 'eccc',
    resolution: str = 'daily',
) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=CA_ECCC_NORMALIZED_DAILY_COLUMNS)

    filtered = table.copy()
    filtered['station_id'] = filtered['station_id'].astype('string').str.strip()
    filtered['observation_date'] = pd.to_datetime(filtered['observation_date'], errors='coerce').dt.date
    filtered = filtered[filtered['observation_date'].notna()]
    if station_ids is not None:
        filtered = filtered[filtered['station_id'].isin(station_ids)]
    if start_date is not None:
        filtered = filtered[filtered['observation_date'] >= start_date]
    if end_date is not None:
        filtered = filtered[filtered['observation_date'] <= end_date]
    if filtered.empty:
        return pd.DataFrame(columns=CA_ECCC_NORMALIZED_DAILY_COLUMNS)

    selected_raw_elements = raw_elements or list(CA_ECCC_DAILY_RAW_TO_CANONICAL.keys())
    rows: list[pd.DataFrame] = []
    for raw_code in selected_raw_elements:
        canonical = CA_ECCC_DAILY_RAW_TO_CANONICAL.get(raw_code)
        if canonical is None or raw_code not in filtered.columns:
            continue
        flag_column = f'{raw_code}_FLAG'
        normalized = pd.DataFrame(
            {
                'station_id': filtered['station_id'].astype('string'),
                'gh_id': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                'element': canonical,
                'element_raw': raw_code,
                'observation_date': filtered['observation_date'],
                'time_function': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                'value': pd.to_numeric(filtered[raw_code], errors='coerce'),
                'flag': (
                    filtered[flag_column].astype('string').str.strip().replace({'': pd.NA})
                    if flag_column in filtered.columns
                    else pd.Series(pd.NA, index=filtered.index, dtype='string')
                ),
                'quality': pd.Series(pd.NA, index=filtered.index, dtype='Int64'),
                'provider': provider,
                'resolution': resolution,
            }
        )
        normalized = normalized[normalized['value'].notna()]
        if not normalized.empty:
            rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=CA_ECCC_NORMALIZED_DAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, CA_ECCC_NORMALIZED_DAILY_COLUMNS]


def normalize_ca_eccc_station_id(value: object) -> str:
    cleaned = _clean_string(value)
    if cleaned.endswith('.0'):
        cleaned = cleaned[:-2]
    return cleaned


def parse_ca_eccc_local_date(value: object) -> date | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: Any) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None
