from __future__ import annotations

import pandas as pd
import requests

from .dk_metadata import read_station_metadata_dk
from .dk_parser import (
    DK_NORMALIZED_SUBDAILY_COLUMNS,
    normalize_dk_station_id,
    observation_timestamp_from_observed,
    parse_dk_feature_collection_json,
)
from .dk_registry import DMI_METOBS_OBSERVATION_URL
from .elements import canonicalize_element_series
from .errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .queries import ObservationQuery


MAX_FEATURES = 300000
NULLABLE_INT_DTYPE = pd.Int64Dtype()


def download_tenmin_observations_dk(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != '10min':
        raise UnsupportedQueryError('The DMI Denmark 10min downloader only supports historical/10min.')
    if not query.elements:
        raise UnsupportedQueryError('The DMI Denmark 10min downloader requires at least one element.')

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
            payload = _download_tenmin_payload(
                station_id=station_id,
                parameter_id=parameter_id,
                request_start=request_start,
                request_end=request_end,
                timeout=timeout,
            )
            normalized = normalize_tenmin_observations_dk(payload, query, station_metadata=metadata_table)
            if not normalized.empty:
                normalized_frames.append(normalized)

    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(normalized_frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'timestamp', 'element']).reset_index(drop=True)
    return combined.loc[:, DK_NORMALIZED_SUBDAILY_COLUMNS]



def normalize_tenmin_observations_dk(
    payload: dict[str, object],
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    features = payload.get('features')
    if not isinstance(features, list) or not features:
        return pd.DataFrame(columns=DK_NORMALIZED_SUBDAILY_COLUMNS)

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
        timestamp = observation_timestamp_from_observed(properties.get('observed'))
        if timestamp is None:
            continue
        if query.start is not None and timestamp < pd.Timestamp(query.start):
            continue
        if query.end is not None and timestamp > pd.Timestamp(query.end):
            continue
        element_columns = canonicalize_element_series(pd.Series([raw_code]), query)
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': element_columns.iloc[0]['element'],
                'element_raw': element_columns.iloc[0]['element_raw'],
                'timestamp': timestamp,
                'value': pd.to_numeric(pd.Series([properties.get('value')]), errors='coerce').iloc[0],
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype=NULLABLE_INT_DTYPE).iloc[0],
                'dataset_scope': query.dataset_scope,
                'resolution': query.resolution,
            }
        )

    if not rows:
        return pd.DataFrame(columns=DK_NORMALIZED_SUBDAILY_COLUMNS)

    combined = pd.DataFrame.from_records(rows)
    combined['quality'] = pd.Series(combined['quality'], dtype=NULLABLE_INT_DTYPE)
    if metadata_lookup is not None:
        combined = combined.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
    return combined.loc[:, DK_NORMALIZED_SUBDAILY_COLUMNS].reset_index(drop=True)



def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    if not query.all_history:
        return pd.Timestamp(query.start), pd.Timestamp(query.end)
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    end = pd.to_datetime(selected['end_date'], utc=True, errors='coerce')
    now_utc = pd.Timestamp.now(tz='UTC')
    end = end.fillna(now_utc)
    latest = end.max()
    if pd.isna(begin) or pd.isna(latest):
        raise UnsupportedQueryError('DMI Denmark all_history mode requires station coverage metadata.')
    return begin, latest



def _download_tenmin_payload(*, station_id: str, parameter_id: str, request_start: pd.Timestamp, request_end: pd.Timestamp, timeout: int) -> dict[str, object]:
    params = {
        'stationId': station_id,
        'parameterId': parameter_id,
        'datetime': f'{_format_timestamp(request_start)}/{_format_timestamp(request_end)}',
        'limit': str(MAX_FEATURES),
    }
    response = requests.get(DMI_METOBS_OBSERVATION_URL, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return parse_dk_feature_collection_json(response.text)



def _format_timestamp(timestamp: pd.Timestamp) -> str:
    utc_timestamp = pd.Timestamp(timestamp)
    if utc_timestamp.tzinfo is None:
        utc_timestamp = utc_timestamp.tz_localize('UTC')
    else:
        utc_timestamp = utc_timestamp.tz_convert('UTC')
    return utc_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
