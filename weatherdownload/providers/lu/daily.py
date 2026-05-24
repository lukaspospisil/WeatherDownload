from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import requests

from .metadata import read_station_metadata_lu
from .parser import LU_NORMALIZED_DAILY_COLUMNS, normalize_lu_daily_feature_rows, normalize_lu_station_id, parse_lu_feature_collection_json
from .registry import LU_DAILY_PARAMETER_METADATA, LU_METEOLUX_WFS_URL
from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery


def download_daily_observations_lu(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'meteolux' or query.resolution != 'daily':
        raise UnsupportedQueryError('The MeteoLux Luxembourg downloader only supports meteolux/daily.')
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
            payload = _download_daily_payload(
                station_id=normalized_station_id,
                raw_code=raw_code,
                request_start=request_start,
                request_end=request_end,
                timeout=timeout,
            )
            normalized = normalize_lu_daily_observations(
                payload,
                query=query,
                raw_code=raw_code,
                station_id=normalized_station_id,
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
    raw_code: str,
    request_start: date,
    request_end: date,
    timeout: int,
) -> dict[str, object]:
    del station_id
    layer_name = LU_DAILY_PARAMETER_METADATA[raw_code]['layer_name']
    params = {
        'SERVICE': 'WFS',
        'VERSION': '2.0.0',
        'REQUEST': 'GetFeature',
        'TYPENAMES': layer_name,
        'OUTPUTFORMAT': 'application/json',
        'SRSNAME': 'EPSG:4326',
        'CQL_FILTER': _build_cql_filter(request_start=request_start, request_end=request_end),
    }
    response = requests.get(LU_METEOLUX_WFS_URL, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return parse_lu_feature_collection_json(response.text)


def _build_cql_filter(*, request_start: date, request_end: date) -> str:
    start_iso = f'{request_start.isoformat()}T00:00:00Z'
    end_iso = f'{(request_end + timedelta(days=1)).isoformat()}T00:00:00Z'
    return (
        "name_descr = 'Findel Airport' "
        f"AND datetime >= '{start_iso}' "
        f"AND datetime < '{end_iso}'"
    )
