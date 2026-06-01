from __future__ import annotations

import json

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_frost, resolve_frost_client_id
from .parser import (
    FROST_DAILY_SPECIAL_VALUE_DESCRIPTIONS,
    NO_FROST_NORMALIZED_DAILY_COLUMNS,
    parse_frost_observations_json,
)
from .registry import get_dataset_spec


def download_daily_observations_frost(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'frost' or query.resolution != 'daily':
        raise UnsupportedQueryError("MET Norway Frost downloader supports only provider='frost' and resolution='daily'.")
    if query.all_history:
        raise UnsupportedQueryError("MET Norway Frost daily downloader does not support all_history yet.")
    if not query.elements:
        raise UnsupportedQueryError('MET Norway Frost daily downloader requires at least one element.')
    if query.start_date is None or query.end_date is None:
        raise UnsupportedQueryError('MET Norway Frost daily downloader requires explicit start_date and end_date values.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_frost(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No MET Norway Frost station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No MET Norway Frost station metadata found for station_id: {', '.join(missing_station_ids)}")

    source = metadata_table.attrs.get('observations_source_url')
    if isinstance(source, str) and source:
        payload_text = _read_observations_text_from_source(source, timeout=timeout)
    else:
        spec = get_dataset_spec('frost', 'daily')
        response = requests.get(
            spec.data_url,
            params={
                'sources': ','.join(query.station_ids),
                'referencetime': f'{query.start_date.isoformat()}/{query.end_date.isoformat()}',
                'elements': ','.join(query.elements or []),
                'timeresolutions': 'P1D',
                'levels': 'default',
                'timeoffsets': 'default',
            },
            timeout=timeout,
            auth=(resolve_frost_client_id(), ''),
        )
        response.raise_for_status()
        response.encoding = 'utf-8'
        payload_text = response.text

    parsed = parse_frost_observations_json(payload_text)
    normalized = normalize_daily_observations_frost(parsed, query=query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, NO_FROST_NORMALIZED_DAILY_COLUMNS]


def normalize_daily_observations_frost(
    payload: pd.DataFrame,
    *,
    query: ObservationQuery,
) -> pd.DataFrame:
    if payload.empty:
        return pd.DataFrame(columns=NO_FROST_NORMALIZED_DAILY_COLUMNS)

    selected = payload.copy()
    selected['station_id'] = selected['station_id'].astype(str).str.strip().str.upper()
    selected = selected[selected['station_id'].isin(query.station_ids)]
    selected = selected[selected['observation_date'].between(query.start_date, query.end_date)]
    selected = selected[selected['element_raw'].isin(query.elements or [])]
    if selected.empty:
        return pd.DataFrame(columns=NO_FROST_NORMALIZED_DAILY_COLUMNS)

    element_frame = canonicalize_element_series(selected['element_raw'], query)
    selected['element'] = element_frame['element']
    selected['element_raw'] = element_frame['element_raw']
    selected['value'] = [
        _convert_frost_daily_value(row.element_raw, row.value_raw, row.unit)
        for row in selected.itertuples(index=False)
    ]
    selected['flag'] = [
        _build_flag_payload(row.element_raw, row.value_raw, row.unit, row.time_offset, row.time_resolution, row.level, row.performance_category, row.exposure_category, row.source_id)
        for row in selected.itertuples(index=False)
    ]
    selected['quality'] = pd.Series(selected['quality_code'], dtype='Int64')
    selected['gh_id'] = pd.NA
    selected['time_function'] = pd.NA
    selected['provider'] = query.provider
    selected['resolution'] = query.resolution

    normalized = selected.loc[:, NO_FROST_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)
    normalized['quality'] = pd.Series(normalized['quality'], dtype='Int64')
    return normalized


def _read_observations_text_from_source(source: str, *, timeout: int) -> str:
    return parse_text_source(source, timeout=timeout)


def parse_text_source(source: str, *, timeout: int) -> str:
    from .parser import read_text_from_source

    return read_text_from_source(source, timeout, requests)


def _convert_frost_daily_value(element_raw: str, value_raw: object, unit: object) -> object:
    if value_raw is None or pd.isna(value_raw):
        return pd.NA
    unit_text = str(unit or '').strip()
    value = float(value_raw)

    if element_raw in {'mean(air_temperature P1D)', 'max(air_temperature P1D)', 'min(air_temperature P1D)'}:
        if unit_text != 'degC':
            raise ValueError(f'Unexpected Frost unit for {element_raw!r}: {unit_text!r}')
        return value
    if element_raw == 'sum(precipitation_amount P1D)':
        if unit_text != 'mm':
            raise ValueError(f'Unexpected Frost unit for {element_raw!r}: {unit_text!r}')
        if value == -1.0:
            return 0.0
        return value
    if element_raw == 'mean(wind_speed P1D)':
        if unit_text != 'm/s':
            raise ValueError(f'Unexpected Frost unit for {element_raw!r}: {unit_text!r}')
        return value
    if element_raw == 'surface_snow_thickness':
        if (element_raw, value) in FROST_DAILY_SPECIAL_VALUE_DESCRIPTIONS:
            return pd.NA
        if unit_text == 'cm':
            return value * 10.0
        if unit_text == 'mm':
            return value
        raise ValueError(f'Unexpected Frost unit for {element_raw!r}: {unit_text!r}')
    raise ValueError(f'Unsupported Frost daily element: {element_raw!r}')


def _build_flag_payload(
    element_raw: str,
    value_raw: object,
    unit: object,
    time_offset: object,
    time_resolution: object,
    level: object,
    performance_category: object,
    exposure_category: object,
    source_id: object,
) -> object:
    payload: dict[str, object] = {}
    if source_id:
        payload['source_id'] = str(source_id)
    if unit:
        payload['unit'] = str(unit)
    if time_offset:
        payload['time_offset'] = str(time_offset)
    if time_resolution:
        payload['time_resolution'] = str(time_resolution)
    if level and not pd.isna(level):
        payload['level'] = level
    if performance_category:
        payload['performance_category'] = str(performance_category)
    if exposure_category:
        payload['exposure_category'] = str(exposure_category)
    if value_raw is not None and not pd.isna(value_raw):
        description = FROST_DAILY_SPECIAL_VALUE_DESCRIPTIONS.get((element_raw, float(value_raw)))
        if description is not None:
            payload['coded_value'] = float(value_raw)
            payload['coded_value_description'] = description
    if not payload:
        return pd.NA
    return json.dumps(payload, ensure_ascii=True, sort_keys=True)
