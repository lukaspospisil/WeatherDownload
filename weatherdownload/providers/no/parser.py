from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import NO_FROST_DAILY_PARAMETER_METADATA

NO_FROST_NORMALIZED_DAILY_COLUMNS = [
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

FROST_DAILY_SPECIAL_VALUE_DESCRIPTIONS = {
    ('sum(precipitation_amount P1D)', -1.0): 'No precipitation',
    ('surface_snow_thickness', -3.0): 'Not possible to measure snow depth',
    ('surface_snow_thickness', -1.0): 'Zero snow depth or partial snow cover',
    ('surface_snow_thickness', 0.0): 'Snow depth less than 0.5 cm',
}


def parse_frost_payload_json(json_text: str) -> object:
    try:
        return json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('Frost response is not valid JSON.') from exc


def parse_frost_station_metadata_json(json_text: str) -> pd.DataFrame:
    payload = parse_frost_payload_json(json_text)
    if not isinstance(payload, dict):
        raise ValueError('Frost station metadata response must be a JSON object.')
    data = payload.get('data', [])
    if not isinstance(data, list):
        raise ValueError('Frost station metadata response must contain a data list.')

    rows: list[dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        station_id = _normalize_station_id(item.get('id'))
        if not station_id:
            continue
        geometry = item.get('geometry')
        longitude = None
        latitude = None
        if isinstance(geometry, dict):
            coordinates = geometry.get('coordinates')
            if (
                isinstance(coordinates, list)
                and len(coordinates) >= 2
                and _is_number(coordinates[0])
                and _is_number(coordinates[1])
            ):
                longitude = float(coordinates[0])
                latitude = float(coordinates[1])
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': _normalize_date_text(item.get('validFrom')),
                'end_date': _normalize_date_text(item.get('validTo')),
                'full_name': _clean_string(item.get('name')) or pd.NA,
                'longitude': longitude,
                'latitude': latitude,
                'elevation_m': _parse_optional_float(item.get('masl')),
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.drop_duplicates(subset=['station_id']).sort_values('station_id', kind='stable').reset_index(drop=True)


def normalize_frost_observation_metadata(stations: pd.DataFrame, spec: object) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in getattr(spec, 'supported_elements', ()):
            parameter_metadata = NO_FROST_DAILY_PARAMETER_METADATA[raw_code]
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': 'P1D Frost daily observations',
                    'name': parameter_metadata['name'],
                    'description': parameter_metadata['description'],
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_frost_observations_json(json_text: str) -> pd.DataFrame:
    payload = parse_frost_payload_json(json_text)
    if not isinstance(payload, dict):
        raise ValueError('Frost observations response must be a JSON object.')
    data = payload.get('data', [])
    if not isinstance(data, list):
        raise ValueError('Frost observations response must contain a data list.')

    rows: list[dict[str, object]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        station_id = _normalize_station_id(item.get('sourceId'))
        if not station_id:
            continue
        observation_date = _parse_reference_date(item.get('referenceTime'))
        observations = item.get('observations', [])
        if observation_date is None or not isinstance(observations, list):
            continue
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            element_id = _clean_string(observation.get('elementId'))
            if not element_id:
                continue
            value = _parse_optional_float(observation.get('value'))
            quality_code = _parse_optional_int(observation.get('qualityCode'))
            rows.append(
                {
                    'station_id': station_id,
                    'observation_date': observation_date,
                    'element_raw': element_id,
                    'value_raw': value,
                    'unit': _clean_string(observation.get('unit')),
                    'time_offset': _clean_string(observation.get('timeOffset')),
                    'time_resolution': _clean_string(observation.get('timeResolution')),
                    'quality_code': quality_code,
                    'level': observation.get('level'),
                    'performance_category': _clean_string(observation.get('performanceCategory')),
                    'exposure_category': _clean_string(observation.get('exposureCategory')),
                    'source_id': _clean_string(item.get('sourceId')),
                }
            )

    return pd.DataFrame.from_records(
        rows,
        columns=[
            'station_id',
            'observation_date',
            'element_raw',
            'value_raw',
            'unit',
            'time_offset',
            'time_resolution',
            'quality_code',
            'level',
            'performance_category',
            'exposure_category',
            'source_id',
        ],
    )


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _normalize_station_id(value: object) -> str:
    cleaned = _clean_string(value).upper()
    if not cleaned:
        return ''
    if ':' in cleaned:
        return cleaned.split(':', 1)[0]
    return cleaned


def _normalize_date_text(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    parsed = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(parsed):
        return ''
    return parsed.date().isoformat()


def _parse_reference_date(value: object):
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def _parse_optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _parse_optional_int(value: object):
    if value is None or pd.isna(value):
        return pd.Series([pd.NA], dtype='Int64').iloc[0]
    parsed = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return pd.Series([pd.NA], dtype='Int64').iloc[0]
    return pd.Series([int(parsed)], dtype='Int64').iloc[0]


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _is_number(value: object) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True
