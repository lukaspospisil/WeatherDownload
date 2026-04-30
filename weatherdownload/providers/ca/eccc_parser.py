from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS


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

CA_ECCC_NORMALIZED_HOURLY_COLUMNS = [
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

CA_ECCC_DAILY_RAW_TO_CANONICAL = {
    'MEAN_TEMPERATURE': 'tas_mean',
    'MAX_TEMPERATURE': 'tas_max',
    'MIN_TEMPERATURE': 'tas_min',
    'TOTAL_PRECIPITATION': 'precipitation',
}

CA_ECCC_HOURLY_RAW_TO_CANONICAL = {
    'TEMP': 'tas_mean',
    'RELATIVE_HUMIDITY': 'relative_humidity',
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


def parse_ca_eccc_hourly_feature_collection(json_text: str) -> pd.DataFrame:
    payload = _parse_feature_collection_payload(json_text)
    rows: list[dict[str, object]] = []
    for properties, _geometry in _feature_properties_and_geometry(payload):
        station_id = normalize_ca_eccc_station_id(properties.get('CLIMATE_IDENTIFIER'))
        timestamp = _parse_ca_eccc_utc_date(properties.get('UTC_DATE'))
        if not station_id or timestamp is None:
            continue
        rows.append(
            {
                'station_id': station_id,
                'timestamp': timestamp,
                'TEMP': _parse_float(properties.get('TEMP')),
                'TEMP_FLAG': _clean_string(properties.get('TEMP_FLAG')) or pd.NA,
                'RELATIVE_HUMIDITY': _parse_float(properties.get('RELATIVE_HUMIDITY')),
                'RELATIVE_HUMIDITY_FLAG': _clean_string(properties.get('RELATIVE_HUMIDITY_FLAG')) or pd.NA,
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


def normalize_ca_eccc_hourly_observations(
    table: pd.DataFrame,
    *,
    station_ids: list[str] | None = None,
    raw_elements: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    provider: str = 'eccc',
    resolution: str = '1hour',
) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=CA_ECCC_NORMALIZED_HOURLY_COLUMNS)

    filtered = table.copy()
    filtered['station_id'] = filtered['station_id'].astype('string').str.strip()
    filtered['timestamp'] = pd.to_datetime(filtered['timestamp'], utc=True, errors='coerce')
    filtered = filtered[filtered['timestamp'].notna()]
    if station_ids is not None:
        filtered = filtered[filtered['station_id'].isin(station_ids)]
    if start is not None:
        start_ts = pd.Timestamp(start)
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize('UTC')
        else:
            start_ts = start_ts.tz_convert('UTC')
        filtered = filtered[filtered['timestamp'] >= start_ts]
    if end is not None:
        end_ts = pd.Timestamp(end)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize('UTC')
        else:
            end_ts = end_ts.tz_convert('UTC')
        filtered = filtered[filtered['timestamp'] <= end_ts]
    if filtered.empty:
        return pd.DataFrame(columns=CA_ECCC_NORMALIZED_HOURLY_COLUMNS)

    selected_raw_elements = raw_elements or list(CA_ECCC_HOURLY_RAW_TO_CANONICAL.keys())
    rows: list[pd.DataFrame] = []
    for raw_code in selected_raw_elements:
        canonical = CA_ECCC_HOURLY_RAW_TO_CANONICAL.get(raw_code)
        if canonical is None or raw_code not in filtered.columns:
            continue
        flag_column = f'{raw_code}_FLAG'
        normalized = pd.DataFrame(
            {
                'station_id': filtered['station_id'].astype('string'),
                'gh_id': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                'element': canonical,
                'element_raw': raw_code,
                'timestamp': filtered['timestamp'],
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
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=CA_ECCC_NORMALIZED_HOURLY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined.sort_values(['station_id', 'timestamp', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, CA_ECCC_NORMALIZED_HOURLY_COLUMNS]

def parse_ca_eccc_station_metadata_feature_collection(json_text: str) -> pd.DataFrame:
    payload = _parse_feature_collection_payload(json_text)
    rows_by_station: dict[str, dict[str, object]] = {}
    hourly_station_ids: set[str] = set()
    for properties, geometry in _feature_properties_and_geometry(payload):
        station_id = normalize_ca_eccc_station_id(properties.get('CLIMATE_IDENTIFIER') or properties.get('id'))
        if not station_id:
            continue
        has_hourly = _clean_string(properties.get('HAS_HOURLY_DATA')).upper()
        if has_hourly in {'Y', 'YES', 'TRUE', '1'}:
            hourly_station_ids.add(station_id)
        row = rows_by_station.setdefault(
            station_id,
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': '',
                'end_date': '',
                'full_name': _clean_string(properties.get('STATION_NAME')) or pd.NA,
                'longitude': _geometry_longitude(geometry),
                'latitude': _geometry_latitude(geometry),
                'elevation_m': _parse_float(properties.get('ELEVATION')),
            },
        )
        begin_candidate = _station_begin_date(properties)
        end_candidate = _station_end_date(properties)
        row['begin_date'] = _min_datetime_string(row.get('begin_date'), begin_candidate)
        row['end_date'] = _max_datetime_string(row.get('end_date'), end_candidate)
        if pd.isna(row.get('full_name')) and _clean_string(properties.get('STATION_NAME')):
            row['full_name'] = _clean_string(properties.get('STATION_NAME'))
        if row.get('longitude') is None:
            row['longitude'] = _geometry_longitude(geometry)
        if row.get('latitude') is None:
            row['latitude'] = _geometry_latitude(geometry)
        if row.get('elevation_m') is None:
            row['elevation_m'] = _parse_float(properties.get('ELEVATION'))

    frame = pd.DataFrame.from_records(list(rows_by_station.values()), columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        ('eccc', 'daily'): {
            str(station_id): list(CA_ECCC_DAILY_RAW_TO_CANONICAL.keys())
            for station_id in frame['station_id'].astype(str).tolist()
        }
    }
    frame.attrs['station_provider_raw_elements_by_path'][('eccc', '1hour')] = {
        str(station_id): list(CA_ECCC_HOURLY_RAW_TO_CANONICAL.keys())
        for station_id in sorted(hourly_station_ids)
        if str(station_id) in set(frame['station_id'].astype(str).tolist())
    }
    return frame


def normalize_ca_eccc_observation_metadata(stations: pd.DataFrame) -> pd.DataFrame:
    if stations.empty:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)

    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in CA_ECCC_DAILY_RAW_TO_CANONICAL:
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': 'P1D ECCC GeoMet climate-daily',
                    'name': raw_code,
                    'description': _description_for_raw(raw_code),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS).sort_values(
        ['station_id', 'element'],
        kind='stable',
    ).reset_index(drop=True)


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


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


def _parse_ca_eccc_utc_date(value: object) -> pd.Timestamp | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed


def _parse_feature_collection_payload(json_text: str) -> dict[str, object]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('ECCC GeoMet response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('ECCC GeoMet response must be a top-level JSON object.')
    features = payload.get('features')
    if not isinstance(features, list):
        raise ValueError('ECCC GeoMet response is missing a features list.')
    return payload


def _feature_properties_and_geometry(payload: dict[str, object]) -> list[tuple[dict[str, object], dict[str, object] | None]]:
    rows: list[tuple[dict[str, object], dict[str, object] | None]] = []
    for feature in payload.get('features', []):
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        if not isinstance(properties, dict):
            continue
        geometry = feature.get('geometry')
        rows.append((properties, geometry if isinstance(geometry, dict) else None))
    return rows


def _station_begin_date(properties: dict[str, object]) -> str:
    return (
        _normalize_metadata_datetime(properties.get('DLY_FIRST_DATE'))
        or _normalize_observation_date_to_begin(properties.get('LOCAL_DATE'))
    )


def _station_end_date(properties: dict[str, object]) -> str:
    return (
        _normalize_metadata_datetime(properties.get('DLY_LAST_DATE'))
        or _normalize_observation_date_to_end(properties.get('LOCAL_DATE'))
    )


def _normalize_metadata_datetime(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    parsed = pd.to_datetime(cleaned, errors='coerce', utc=True)
    if pd.isna(parsed):
        return ''
    return parsed.strftime('%Y-%m-%dT%H:%MZ')


def _normalize_observation_date_to_begin(value: object) -> str:
    parsed = parse_ca_eccc_local_date(value)
    if parsed is None:
        return ''
    return f'{parsed.isoformat()}T00:00Z'


def _normalize_observation_date_to_end(value: object) -> str:
    parsed = parse_ca_eccc_local_date(value)
    if parsed is None:
        return ''
    return f'{parsed.isoformat()}T23:59Z'


def _geometry_longitude(geometry: dict[str, object] | None) -> float | None:
    coordinates = geometry.get('coordinates') if geometry else None
    if isinstance(coordinates, (list, tuple)) and len(coordinates) >= 2:
        return _parse_float(coordinates[0])
    return None


def _geometry_latitude(geometry: dict[str, object] | None) -> float | None:
    coordinates = geometry.get('coordinates') if geometry else None
    if isinstance(coordinates, (list, tuple)) and len(coordinates) >= 2:
        return _parse_float(coordinates[1])
    return None


def _min_datetime_string(current: object, candidate: str) -> str:
    current_cleaned = _clean_string(current)
    if not current_cleaned:
        return candidate
    if not candidate:
        return current_cleaned
    return min(current_cleaned, candidate)


def _max_datetime_string(current: object, candidate: str) -> str:
    current_cleaned = _clean_string(current)
    if not current_cleaned:
        return candidate
    if not candidate:
        return current_cleaned
    return max(current_cleaned, candidate)


def _description_for_raw(raw_code: str) -> str:
    return {
        'MEAN_TEMPERATURE': 'Official ECCC GeoMet daily mean air temperature.',
        'MAX_TEMPERATURE': 'Official ECCC GeoMet daily maximum air temperature.',
        'MIN_TEMPERATURE': 'Official ECCC GeoMet daily minimum air temperature.',
        'TOTAL_PRECIPITATION': 'Official ECCC GeoMet daily total precipitation.',
    }[raw_code]


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
