from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

LT_NORMALIZED_DAILY_COLUMNS = [
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


def read_text_from_source(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def parse_lt_payload_json(text: str) -> object:
    try:
        return json.loads(text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('Meteo.lt response is not valid JSON.') from exc


def normalize_lt_station_metadata(
    records: list[dict[str, object]],
    spec: object,
    *,
    ranges_by_station: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    ranges_by_station = ranges_by_station or {}
    rows: list[dict[str, object]] = []
    station_elements = {}
    for record in records:
        station_id = _clean_string(record.get('code'))
        if not station_id:
            continue
        coordinates = record.get('coordinates')
        latitude = None
        longitude = None
        if isinstance(coordinates, dict):
            latitude = _parse_optional_float(coordinates.get('latitude'))
            longitude = _parse_optional_float(coordinates.get('longitude'))
        range_payload = ranges_by_station.get(station_id, {})
        begin_date, end_date = _extract_range_dates(range_payload)
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': begin_date,
                'end_date': end_date,
                'full_name': _clean_string(record.get('name')) or pd.NA,
                'longitude': longitude,
                'latitude': latitude,
                'elevation_m': pd.NA,
            }
        )
        station_elements[station_id] = list(getattr(spec, 'supported_elements', ()))

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        (spec.provider, spec.resolution): station_elements,
    }
    return frame


def normalize_lt_observation_metadata(
    records: list[dict[str, object]],
    spec: object,
    parameter_metadata: dict[str, dict[str, str]],
    *,
    ranges_by_station: dict[str, dict[str, object]] | None = None,
) -> pd.DataFrame:
    ranges_by_station = ranges_by_station or {}
    rows: list[dict[str, object]] = []
    for record in records:
        station_id = _clean_string(record.get('code'))
        if not station_id:
            continue
        begin_date, end_date = _extract_range_dates(ranges_by_station.get(station_id, {}))
        for raw_code in getattr(spec, 'supported_elements', ()):
            metadata = parameter_metadata[raw_code]
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station_id,
                    'begin_date': begin_date,
                    'end_date': end_date,
                    'element': raw_code,
                    'schedule': 'P1D Meteo.lt UTC-date aggregated meteorological observations',
                    'name': metadata['name'],
                    'description': f"{metadata['description']} Source unit: {metadata['unit']}.",
                    'height': pd.NA,
                }
            )

    frame = pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(['station_id', 'element'], kind='stable').reset_index(drop=True)


def normalize_lt_daily_payload(
    payload: dict[str, object],
    *,
    query,
    station_id: str,
    observation_date,
) -> pd.DataFrame:
    station = payload.get('station')
    if isinstance(station, dict):
        payload_station_id = _clean_string(station.get('code'))
        if payload_station_id and payload_station_id != station_id:
            return pd.DataFrame(columns=LT_NORMALIZED_DAILY_COLUMNS)
    observations = payload.get('observations')
    if not isinstance(observations, list):
        return pd.DataFrame(columns=LT_NORMALIZED_DAILY_COLUMNS)

    aggregated = aggregate_lt_daily_records(observations)
    requested_raw = set(query.elements)
    rows: list[dict[str, object]] = []
    for raw_code in query.elements:
        if raw_code not in requested_raw:
            continue
        value = aggregated.get(raw_code, pd.NA)
        if pd.isna(value):
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': raw_code,
                'element_raw': raw_code,
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': float(value),
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=LT_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series([pd.NA] * len(frame), dtype='Int64')
    return frame.sort_values(['station_id', 'observation_date', 'element_raw'], kind='stable').reset_index(drop=True)


def aggregate_lt_daily_records(observations: list[object]) -> dict[str, float]:
    numeric_values: dict[str, list[float]] = {
        'airTemperature': [],
        'precipitation': [],
        'windSpeed': [],
        'windGust': [],
        'relativeHumidity': [],
        'seaLevelPressure': [],
        'cloudCover': [],
    }
    snow_depth_candidates: list[tuple[pd.Timestamp, float]] = []

    for record in observations:
        if not isinstance(record, dict):
            continue
        observation_time = pd.to_datetime(record.get('observationTimeUtc'), utc=True, errors='coerce')
        for raw_code in numeric_values:
            numeric = _parse_optional_float(record.get(raw_code))
            if numeric is not None:
                numeric_values[raw_code].append(numeric)
        snow_depth = _parse_optional_float(record.get('snowDepth'))
        if snow_depth is not None and not pd.isna(observation_time):
            snow_depth_candidates.append((observation_time, snow_depth))

    aggregated: dict[str, float] = {}
    if numeric_values['airTemperature']:
        aggregated['airTemperature_mean'] = float(sum(numeric_values['airTemperature']) / len(numeric_values['airTemperature']))
        aggregated['airTemperature_max'] = float(max(numeric_values['airTemperature']))
        aggregated['airTemperature_min'] = float(min(numeric_values['airTemperature']))
    if numeric_values['precipitation']:
        aggregated['precipitation'] = float(sum(numeric_values['precipitation']))
    if numeric_values['windSpeed']:
        aggregated['windSpeed'] = float(sum(numeric_values['windSpeed']) / len(numeric_values['windSpeed']))
    if numeric_values['windGust']:
        aggregated['windGust'] = float(max(numeric_values['windGust']))
    if numeric_values['relativeHumidity']:
        aggregated['relativeHumidity'] = float(sum(numeric_values['relativeHumidity']) / len(numeric_values['relativeHumidity']))
    if numeric_values['seaLevelPressure']:
        aggregated['seaLevelPressure'] = float(sum(numeric_values['seaLevelPressure']) / len(numeric_values['seaLevelPressure']))
    if numeric_values['cloudCover']:
        aggregated['cloudCover'] = float(sum(numeric_values['cloudCover']) / len(numeric_values['cloudCover']))
    if snow_depth_candidates:
        aggregated['snowDepth'] = float(sorted(snow_depth_candidates, key=lambda item: item[0])[-1][1])
    return aggregated


def _extract_range_dates(payload: dict[str, object]) -> tuple[str, str]:
    range_data = payload.get('observationsDataRange') if isinstance(payload, dict) else None
    if not isinstance(range_data, dict):
        return '', _OPEN_END
    start = _normalize_metadata_datetime(range_data.get('startTimeUtc'), default='')
    end = _normalize_metadata_datetime(range_data.get('endTimeUtc'), default=_OPEN_END)
    return start, end


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
