from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import requests

from .metadata import read_station_metadata_lu
from .parser import (
    LU_NORMALIZED_DAILY_COLUMNS,
    normalize_asta_daily_feature_rows,
    normalize_lu_daily_feature_rows,
    normalize_lu_daily_csv_rows,
    normalize_lu_station_id,
    parse_lu_daily_csv_text,
    parse_lu_feature_collection_json,
)
from .registry import LU_ASTA_DAILY_PARAMETER_METADATA, LU_DAILY_PARAMETER_METADATA, LU_METEOLUX_DAILY_CSV_URL, LU_METEOLUX_WFS_URL
from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery


def download_daily_observations_lu(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider not in {'meteolux', 'asta'} or query.resolution != 'daily':
        raise UnsupportedQueryError('The Luxembourg daily downloader supports only meteolux/daily and asta/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The MeteoLux Luxembourg daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_lu(timeout=timeout)
    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No MeteoLux station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query, metadata_table)
    frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        normalized_station_id = normalize_lu_station_id(station_id)
        for raw_code in query.elements:
            normalized = _download_and_normalize_daily_element(
                query=query,
                station_id=normalized_station_id,
                raw_code=raw_code,
                request_start=request_start,
                request_end=request_end,
                timeout=timeout,
                station_metadata=metadata_table,
            )
            if not normalized.empty:
                frames.append(normalized)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['station_id', 'element', 'observation_date']).reset_index(drop=True)
    return combined.loc[:, LU_NORMALIZED_DAILY_COLUMNS]


def normalize_lu_daily_observations(
    payload: dict[str, object],
    *,
    query: ObservationQuery,
    raw_code: str,
    station_id: str,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frame = normalize_lu_daily_feature_rows(
        payload,
        raw_code=raw_code,
        provider=query.provider,
        resolution=query.resolution,
    )
    if frame.empty:
        return frame
    frame = frame[frame['station_id'].astype(str) == station_id].copy()
    if frame.empty:
        return pd.DataFrame(columns=LU_NORMALIZED_DAILY_COLUMNS)
    frame = frame[
        (frame['observation_date'] >= query.start_date) & (frame['observation_date'] <= query.end_date)
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=LU_NORMALIZED_DAILY_COLUMNS)

    element_columns = canonicalize_element_series(pd.Series(frame['element_raw']), query)
    frame['element'] = element_columns['element']
    frame['element_raw'] = element_columns['element_raw']

    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
        frame = frame.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')

    return frame.loc[:, LU_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)


def normalize_asta_daily_observations(
    payload: dict[str, object],
    *,
    query: ObservationQuery,
    raw_code: str,
    station_id: str,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frame = normalize_asta_daily_feature_rows(
        payload,
        raw_code=raw_code,
        provider=query.provider,
        resolution=query.resolution,
    )
    if frame.empty:
        return frame
    frame = frame[frame['station_id'].astype(str) == station_id].copy()
    if frame.empty:
        return pd.DataFrame(columns=LU_NORMALIZED_DAILY_COLUMNS)
    frame = frame[
        (frame['observation_date'] >= query.start_date) & (frame['observation_date'] <= query.end_date)
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=LU_NORMALIZED_DAILY_COLUMNS)

    element_columns = canonicalize_element_series(pd.Series(frame['element_raw']), query)
    frame['element'] = element_columns['element']
    frame['element_raw'] = element_columns['element_raw']

    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
        frame = frame.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')

    return frame.loc[:, LU_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)


def normalize_lu_daily_csv_observations(
    csv_text: str,
    *,
    query: ObservationQuery,
    raw_code: str,
    station_id: str,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    frame = normalize_lu_daily_csv_rows(
        parse_lu_daily_csv_text(csv_text),
        raw_code=raw_code,
        provider=query.provider,
        resolution=query.resolution,
    )
    if frame.empty:
        return frame
    frame = frame[frame['station_id'].astype(str) == station_id].copy()
    if frame.empty:
        return pd.DataFrame(columns=LU_NORMALIZED_DAILY_COLUMNS)
    frame = frame[
        (frame['observation_date'] >= query.start_date) & (frame['observation_date'] <= query.end_date)
    ].copy()
    if frame.empty:
        return pd.DataFrame(columns=LU_NORMALIZED_DAILY_COLUMNS)

    element_columns = canonicalize_element_series(pd.Series(frame['element_raw']), query)
    frame['element'] = element_columns['element']
    frame['element_raw'] = element_columns['element_raw']

    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
        frame = frame.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')

    return frame.loc[:, LU_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)


def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[date, date]:
    if not query.all_history:
        return query.start_date, query.end_date
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    latest = pd.Timestamp.utcnow().tz_localize('UTC') if pd.Timestamp.utcnow().tzinfo is None else pd.Timestamp.utcnow().tz_convert('UTC')
    if pd.isna(begin):
        raise UnsupportedQueryError('MeteoLux all_history mode requires station coverage metadata.')
    return begin.date(), latest.date()


def _download_daily_payload(
    *,
    station_id: str,
    provider_name: str,
    raw_code: str,
    request_start: date,
    request_end: date,
    timeout: int,
    station_metadata: pd.DataFrame | None = None,
) -> dict[str, object]:
    parameter_metadata = _parameter_metadata_for_provider(provider_name)
    layer_name = parameter_metadata[raw_code]['layer_name']
    params = {
        'SERVICE': 'WFS',
        'VERSION': '2.0.0',
        'REQUEST': 'GetFeature',
        'TYPENAMES': layer_name,
        'OUTPUTFORMAT': 'application/json',
        'SRSNAME': 'EPSG:4326',
        'CQL_FILTER': _build_cql_filter(
            station_id=station_id,
            provider_name=provider_name,
            request_start=request_start,
            request_end=request_end,
            station_metadata=station_metadata,
        ),
    }
    response = requests.get(LU_METEOLUX_WFS_URL, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return parse_lu_feature_collection_json(response.text)


def _download_and_normalize_daily_element(
    *,
    query: ObservationQuery,
    station_id: str,
    raw_code: str,
    request_start: date,
    request_end: date,
    timeout: int,
    station_metadata: pd.DataFrame | None,
) -> pd.DataFrame:
    parameter_metadata = _parameter_metadata_for_provider(query.provider)
    source_kind = parameter_metadata[raw_code]['source_kind']
    if source_kind == 'csv':
        response = requests.get(LU_METEOLUX_DAILY_CSV_URL, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        return normalize_lu_daily_csv_observations(
            response.text,
            query=query,
            raw_code=raw_code,
            station_id=station_id,
            station_metadata=station_metadata,
        )

    payload = _download_daily_payload(
        station_id=station_id,
        provider_name=query.provider,
        raw_code=raw_code,
        request_start=request_start,
        request_end=request_end,
        timeout=timeout,
        station_metadata=station_metadata,
    )
    if query.provider == 'asta':
        return normalize_asta_daily_observations(
            payload,
            query=query,
            raw_code=raw_code,
            station_id=station_id,
            station_metadata=station_metadata,
        )
    return normalize_lu_daily_observations(
        payload,
        query=query,
        raw_code=raw_code,
        station_id=station_id,
        station_metadata=station_metadata,
    )


def _build_cql_filter(
    *,
    station_id: str,
    provider_name: str,
    request_start: date,
    request_end: date,
    station_metadata: pd.DataFrame | None = None,
) -> str:
    start_iso = f'{request_start.isoformat()}T00:00:00Z'
    end_iso = f'{(request_end + timedelta(days=1)).isoformat()}T00:00:00Z'
    station_filter = "name_descr = 'Findel Airport'"
    if provider_name == 'asta':
        station_name = _lookup_station_name(station_id, station_metadata)
        station_filter = f"name_descr = '{station_name}'"
    return (
        f"{station_filter} "
        f"AND datetime >= '{start_iso}' "
        f"AND datetime < '{end_iso}'"
    )


def _parameter_metadata_for_provider(provider_name: str) -> dict[str, dict[str, object]]:
    if provider_name == 'asta':
        return LU_ASTA_DAILY_PARAMETER_METADATA
    return LU_DAILY_PARAMETER_METADATA


def _lookup_station_name(station_id: str, station_metadata: pd.DataFrame | None) -> str:
    if station_metadata is None or station_metadata.empty:
        raise UnsupportedQueryError('ASTA daily downloads require station metadata so station ids can be mapped to names.')
    matches = station_metadata[station_metadata['station_id'].astype(str) == station_id]
    if matches.empty:
        raise StationNotFoundError(f'No ASTA station metadata found for station_id: {station_id}')
    station_name = matches.iloc[0]['full_name']
    if pd.isna(station_name) or not str(station_name).strip():
        raise UnsupportedQueryError(f'ASTA station metadata are missing full_name for station_id: {station_id}')
    return str(station_name)
