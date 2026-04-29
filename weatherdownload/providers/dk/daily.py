from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import requests

from .metadata import read_station_metadata_dk
from .parser import (
    DK_NORMALIZED_DAILY_COLUMNS,
    build_dk_flag,
    normalize_dk_station_id,
    observation_date_from_interval_start,
    parse_dk_feature_collection_json,
)
from .registry import DMI_CLIMATE_STATION_VALUE_URL
from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery


MAX_FEATURES = 300000


def download_daily_observations_dk(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'historical' or query.resolution != 'daily':
        raise UnsupportedQueryError('The DMI Denmark daily downloader only supports historical/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The DMI Denmark daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_dk(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No DMI Denmark station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No DMI Denmark station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query, metadata_table)
    normalized_frames: list[pd.DataFrame] = []

    for station_id in query.station_ids:
        for parameter_id in query.elements or []:
            payload = _download_daily_payload(
                station_id=station_id,
                parameter_id=parameter_id,
                request_start=request_start,
                request_end=request_end,
                timeout=timeout,
            )
            normalized = normalize_daily_observations_dk(payload, query, station_metadata=metadata_table)
            if not normalized.empty:
                normalized_frames.append(normalized)

    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(normalized_frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'observation_date', 'element']).reset_index(drop=True)
    return combined.loc[:, DK_NORMALIZED_DAILY_COLUMNS]



def normalize_daily_observations_dk(
    payload: dict[str, object],
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    features = payload.get('features')
    if not isinstance(features, list) or not features:
        return pd.DataFrame(columns=DK_NORMALIZED_DAILY_COLUMNS)

    metadata_lookup = None
    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])

    rows: list[dict[str, object]] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get('properties')
        if not isinstance(properties, dict):
            continue
        station_id = normalize_dk_station_id(properties.get('stationId'))
        if station_id not in query.station_ids:
            continue
        raw_code = str(properties.get('parameterId', '')).strip()
        if raw_code not in (query.elements or []):
            continue
        observation_date = observation_date_from_interval_start(properties.get('from'))
        if observation_date is None:
            continue
        if query.start_date is not None and observation_date < query.start_date:
            continue
        if query.end_date is not None and observation_date > query.end_date:
            continue
        element_columns = canonicalize_element_series(pd.Series([raw_code]), query)
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': element_columns.iloc[0]['element'],
                'element_raw': element_columns.iloc[0]['element_raw'],
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': pd.to_numeric(pd.Series([properties.get('value')]), errors='coerce').iloc[0],
                'flag': build_dk_flag(properties),
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )

    if not rows:
        return pd.DataFrame(columns=DK_NORMALIZED_DAILY_COLUMNS)

    combined = pd.DataFrame.from_records(rows)
    combined['quality'] = pd.Series(combined['quality'], dtype='Int64')
    if metadata_lookup is not None:
        combined = combined.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
    return combined.loc[:, DK_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)



def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[date, date]:
    if not query.all_history:
        return query.start_date, query.end_date
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    end = pd.to_datetime(selected['end_date'], utc=True, errors='coerce')
    now_utc = pd.Timestamp.now(tz='UTC')
    end = end.fillna(now_utc)
    latest = end.max()
    if pd.isna(begin) or pd.isna(latest):
        raise UnsupportedQueryError('DMI Denmark all_history mode requires station coverage metadata.')
    return begin.date(), latest.date()



def _download_daily_payload(*, station_id: str, parameter_id: str, request_start: date, request_end: date, timeout: int) -> dict[str, object]:
    params = {
        'stationId': station_id,
        'parameterId': parameter_id,
        'timeResolution': 'day',
        'datetime': _build_datetime_filter(request_start=request_start, request_end=request_end),
        'limit': str(MAX_FEATURES),
    }
    response = requests.get(DMI_CLIMATE_STATION_VALUE_URL, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return parse_dk_feature_collection_json(response.text)



def _build_datetime_filter(*, request_start: date, request_end: date) -> str:
    # Denmark daily climate values may start on the previous UTC date because DMI defines the day in local time.
    start_boundary = request_start - timedelta(days=1)
    end_boundary = request_end + timedelta(days=1)
    return f'{start_boundary.isoformat()}T00:00:00Z/{end_boundary.isoformat()}T23:59:59Z'

