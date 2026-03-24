from __future__ import annotations

from datetime import date

import pandas as pd
import requests

from .be_metadata import read_station_metadata_be
from .be_parser import BE_NORMALIZED_DAILY_COLUMNS, parse_be_feature_collection_json, normalize_be_station_id
from .be_registry import RMI_AWS_DAILY_LAYER, RMI_AWS_WFS_URL, get_dataset_spec
from .elements import canonicalize_element_series
from .errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .queries import ObservationQuery


def download_daily_observations_be(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != 'daily':
        raise UnsupportedQueryError('The RMI/KMI Belgium daily downloader only supports historical/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The RMI/KMI Belgium daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_be(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No RMI/KMI Belgium station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No RMI/KMI Belgium station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query, metadata_table)
    payloads = []
    for station_id in query.station_ids:
        payloads.append(_download_daily_payload(station_id=station_id, request_start=request_start, request_end=request_end, timeout=timeout))

    normalized_frames = [normalize_daily_observations_be(payload, query, station_metadata=metadata_table) for payload in payloads]
    normalized_frames = [frame for frame in normalized_frames if not frame.empty]
    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(normalized_frames, ignore_index=True)
    return combined.loc[:, BE_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)



def normalize_daily_observations_be(
    payload: dict[str, object],
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    features = payload.get('features')
    if not isinstance(features, list) or not features:
        return pd.DataFrame(columns=BE_NORMALIZED_DAILY_COLUMNS)

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
        station_id = normalize_be_station_id(properties.get('code'))
        if station_id not in query.station_ids:
            continue
        timestamp = pd.to_datetime(properties.get('timestamp'), utc=True, errors='coerce')
        if pd.isna(timestamp):
            continue
        observation_date = timestamp.date()
        if query.start_date is not None and observation_date < query.start_date:
            continue
        if query.end_date is not None and observation_date > query.end_date:
            continue
        for raw_code in query.elements or []:
            if raw_code not in properties:
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
                    'value': pd.to_numeric(pd.Series([properties.get(raw_code)]), errors='coerce').iloc[0],
                    'flag': properties.get('qc_flags') if properties.get('qc_flags') not in (None, '') else pd.NA,
                    'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                    'dataset_scope': query.dataset_scope,
                    'resolution': query.resolution,
                }
            )

    if not rows:
        return pd.DataFrame(columns=BE_NORMALIZED_DAILY_COLUMNS)

    combined = pd.DataFrame.from_records(rows)
    if metadata_lookup is not None:
        combined = combined.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
    return combined.loc[:, BE_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)



def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[date, date]:
    if not query.all_history:
        return query.start_date, query.end_date
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    end = pd.to_datetime(selected['end_date'], utc=True, errors='coerce')
    end = end.fillna(pd.Timestamp.utcnow().tz_localize('UTC') if pd.Timestamp.utcnow().tzinfo is None else pd.Timestamp.utcnow().tz_convert('UTC'))
    latest = end.max()
    if pd.isna(begin) or pd.isna(latest):
        raise UnsupportedQueryError('RMI/KMI Belgium all_history mode requires station coverage metadata.')
    return begin.date(), latest.date()



def _download_daily_payload(*, station_id: str, request_start: date, request_end: date, timeout: int) -> dict[str, object]:
    params = {
        'service': 'WFS',
        'version': '1.0.0',
        'request': 'GetFeature',
        'typeName': RMI_AWS_DAILY_LAYER,
        'outputFormat': 'application/json',
        'srsName': 'EPSG:4326',
        'sortBy': 'timestamp A',
        'maxFeatures': '50000',
        'cql_filter': _build_cql_filter(station_id=station_id, request_start=request_start, request_end=request_end),
    }
    response = requests.get(RMI_AWS_WFS_URL, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return parse_be_feature_collection_json(response.text)



def _build_cql_filter(*, station_id: str, request_start: date, request_end: date) -> str:
    code = int(station_id)
    start_iso = f'{request_start.isoformat()}T00:00:00Z'
    end_iso = f'{request_end.isoformat()}T00:00:00Z'
    return f"code = {code} AND timestamp >= '{start_iso}' AND timestamp <= '{end_iso}'"
