from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from ..ghcnd.parser import (
    GHCND_NORMALIZED_DAILY_COLUMNS,
    build_station_supported_raw_elements,
    normalize_daily_observations_ghcnd,
    normalize_ghcnd_observation_metadata,
    normalize_ghcnd_station_metadata,
    parse_ghcnd_dly_text,
    parse_ghcnd_inventory_text,
    parse_ghcnd_stations_text,
)

GB_METOFFICE_DATAHUB_NORMALIZED_SUBDAILY_COLUMNS = [
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

GB_METOFFICE_DATAHUB_RAW_TO_CANONICAL = {
    'temperature': 'tas_mean',
    'humidity': 'relative_humidity',
    'wind_speed': 'wind_speed',
    'mslp': 'pressure',
}

GB_METOFFICE_DATAHUB_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'temperature': {
        'name': 'Hourly air temperature',
        'description': 'Met Office Weather DataHub Land Observations screen temperature in degree Celsius.',
        'schedule': 'PT1H Met Office Weather DataHub Land Observations',
    },
    'humidity': {
        'name': 'Hourly relative humidity',
        'description': 'Met Office Weather DataHub Land Observations screen relative humidity in percent.',
        'schedule': 'PT1H Met Office Weather DataHub Land Observations',
    },
    'wind_speed': {
        'name': 'Hourly wind speed',
        'description': 'Met Office Weather DataHub Land Observations 10 m wind speed in m/s.',
        'schedule': 'PT1H Met Office Weather DataHub Land Observations',
    },
    'mslp': {
        'name': 'Hourly mean sea-level pressure',
        'description': 'Met Office Weather DataHub Land Observations mean sea-level pressure in hPa.',
        'schedule': 'PT1H Met Office Weather DataHub Land Observations',
    },
}


def parse_metoffice_datahub_station_metadata_json(json_text: str) -> pd.DataFrame:
    payload = _parse_json_list(json_text, 'Met Office Weather DataHub station metadata response')
    rows: list[dict[str, object]] = []
    for item in payload:
        station_id = _clean_string(item.get('geohash')).upper()
        if not station_id:
            continue
        full_name = (
            _clean_string(item.get('full_name'))
            or _clean_string(item.get('name'))
            or _clean_string(item.get('area'))
            or station_id
        )
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': '',
                'end_date': '',
                'full_name': full_name,
                'longitude': _parse_float(item.get('longitude')),
                'latitude': _parse_float(item.get('latitude')),
                'elevation_m': _parse_float(item.get('elevation_m')),
            }
        )
    if not rows:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        ('metoffice_datahub', '1hour'): {
            str(station_id): list(GB_METOFFICE_DATAHUB_RAW_TO_CANONICAL.keys())
            for station_id in frame['station_id'].astype(str).tolist()
        }
    }
    return frame


def normalize_metoffice_datahub_station_metadata(json_text: str) -> pd.DataFrame:
    return parse_metoffice_datahub_station_metadata_json(json_text)


def normalize_metoffice_datahub_observation_metadata(stations: pd.DataFrame, spec: Any) -> pd.DataFrame:
    if stations.empty:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in getattr(spec, 'supported_elements', ()):
            metadata = GB_METOFFICE_DATAHUB_PARAMETER_METADATA.get(str(raw_code), {})
            rows.append(
                {
                    'obs_type': 'RECENT_HOURLY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': metadata.get('schedule', 'PT1H Met Office Weather DataHub Land Observations'),
                    'name': metadata.get('name', raw_code),
                    'description': metadata.get('description', raw_code),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS).sort_values(
        ['station_id', 'element'],
        kind='stable',
    ).reset_index(drop=True)


def parse_metoffice_datahub_hourly_observations_json(json_text: str) -> pd.DataFrame:
    payload = _parse_json_list(json_text, 'Met Office Weather DataHub hourly observations response')
    rows: list[dict[str, object]] = []
    for item in payload:
        timestamp = pd.to_datetime(item.get('datetime'), utc=True, errors='coerce')
        if pd.isna(timestamp):
            continue
        row: dict[str, object] = {'timestamp': timestamp}
        for raw_code in GB_METOFFICE_DATAHUB_RAW_TO_CANONICAL:
            row[raw_code] = _parse_float(item.get(raw_code))
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame.from_records(rows)


def normalize_metoffice_datahub_hourly_observations(
    table: pd.DataFrame,
    *,
    station_ids: list[str] | None = None,
    raw_elements: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    provider: str = 'metoffice_datahub',
    resolution: str = '1hour',
) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=GB_METOFFICE_DATAHUB_NORMALIZED_SUBDAILY_COLUMNS)

    filtered = table.copy()
    filtered['timestamp'] = pd.to_datetime(filtered['timestamp'], utc=True, errors='coerce')
    filtered = filtered[filtered['timestamp'].notna()]
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
        return pd.DataFrame(columns=GB_METOFFICE_DATAHUB_NORMALIZED_SUBDAILY_COLUMNS)

    requested_station_ids = station_ids or ['']
    selected_raw_elements = raw_elements or list(GB_METOFFICE_DATAHUB_RAW_TO_CANONICAL.keys())
    rows: list[pd.DataFrame] = []
    for station_id in requested_station_ids:
        for raw_code in selected_raw_elements:
            canonical = GB_METOFFICE_DATAHUB_RAW_TO_CANONICAL.get(str(raw_code))
            if canonical is None or raw_code not in filtered.columns:
                continue
            normalized = pd.DataFrame(
                {
                    'station_id': station_id,
                    'gh_id': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                    'element': canonical,
                    'element_raw': raw_code,
                    'timestamp': filtered['timestamp'],
                    'value': pd.to_numeric(filtered[raw_code], errors='coerce'),
                    'flag': pd.Series(pd.NA, index=filtered.index, dtype='string'),
                    'quality': pd.Series(pd.NA, index=filtered.index, dtype='Int64'),
                    'provider': provider,
                    'resolution': resolution,
                }
            )
            normalized = normalized[normalized['value'].notna()]
            if not normalized.empty:
                rows.append(normalized)
    if not rows:
        return pd.DataFrame(columns=GB_METOFFICE_DATAHUB_NORMALIZED_SUBDAILY_COLUMNS)
    combined = pd.concat(rows, ignore_index=True)
    return combined.sort_values(['station_id', 'timestamp', 'element'], kind='stable').reset_index(drop=True)


def read_text_from_source(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _parse_json_list(json_text: str, label: str) -> list[dict[str, object]]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError(f'{label} is not valid JSON.') from exc
    if not isinstance(payload, list):
        raise ValueError(f'{label} must be a top-level JSON list.')
    return [item for item in payload if isinstance(item, dict)]


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


__all__ = [
    'GHCND_NORMALIZED_DAILY_COLUMNS',
    'GB_METOFFICE_DATAHUB_NORMALIZED_SUBDAILY_COLUMNS',
    'GB_METOFFICE_DATAHUB_PARAMETER_METADATA',
    'build_station_supported_raw_elements',
    'normalize_daily_observations_ghcnd',
    'normalize_ghcnd_observation_metadata',
    'normalize_ghcnd_station_metadata',
    'normalize_metoffice_datahub_hourly_observations',
    'normalize_metoffice_datahub_observation_metadata',
    'normalize_metoffice_datahub_station_metadata',
    'parse_ghcnd_dly_text',
    'parse_ghcnd_inventory_text',
    'parse_ghcnd_stations_text',
    'parse_metoffice_datahub_hourly_observations_json',
    'parse_metoffice_datahub_station_metadata_json',
    'read_text_from_source',
]
