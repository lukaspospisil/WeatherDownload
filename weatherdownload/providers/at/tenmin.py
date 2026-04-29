from __future__ import annotations

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .metadata import read_station_metadata_geosphere
from .parser import (
    GEOSPHERE_NORMALIZED_SUBDAILY_COLUMNS,
    build_geosphere_flag,
    parse_geosphere_tenmin_csv,
)
from ...queries import ObservationQuery

GEOSPHERE_TENMIN_API_URL = 'https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-10min'


def download_tenmin_observations_geosphere(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'historical' or query.resolution != '10min':
        raise UnsupportedQueryError('The GeoSphere Austria 10min downloader only supports historical/10min.')
    if not query.elements:
        raise UnsupportedQueryError('The GeoSphere Austria 10min downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_geosphere(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No GeoSphere Austria station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No GeoSphere Austria station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query, metadata_table)
    csv_text = _download_tenmin_csv(query, request_start=request_start, request_end=request_end, timeout=timeout)
    parsed = parse_geosphere_tenmin_csv(csv_text)
    normalized = normalize_tenmin_observations_geosphere(parsed, query, station_metadata=metadata_table)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, GEOSPHERE_NORMALIZED_SUBDAILY_COLUMNS]


def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[str, str]:
    if not query.all_history:
        return pd.Timestamp(query.start).isoformat(), pd.Timestamp(query.end).isoformat()
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    end = pd.to_datetime(selected['end_date'], utc=True, errors='coerce').max()
    if pd.isna(begin) or pd.isna(end):
        raise UnsupportedQueryError('GeoSphere Austria all_history mode requires station coverage metadata.')
    return begin.isoformat(), end.isoformat()


def _download_tenmin_csv(query: ObservationQuery, *, request_start: str, request_end: str, timeout: int) -> str:
    params: list[tuple[str, str]] = []
    for station_id in query.station_ids:
        params.append(('station_ids', station_id))
    for raw_code in query.elements or []:
        params.append(('parameters', raw_code))
        params.append(('parameters', f'{raw_code}_flag'))
    params.extend([
        ('start', request_start),
        ('end', request_end),
        ('output_format', 'csv'),
    ])
    response = requests.get(GEOSPHERE_TENMIN_API_URL, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def normalize_tenmin_observations_geosphere(
    table: pd.DataFrame,
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    metadata_lookup = None
    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])

    rows: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        if raw_code not in table.columns:
            continue
        flag_column = f'{raw_code}_flag' if f'{raw_code}_flag' in table.columns else None
        element_columns = canonicalize_element_series(pd.Series([raw_code] * len(table.index), index=table.index), query)
        normalized = pd.DataFrame(
            {
                'station_id': table['station'].astype(str).str.strip(),
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'timestamp': pd.to_datetime(table['time'], utc=True, errors='coerce'),
                'value': pd.to_numeric(table[raw_code], errors='coerce'),
                'flag': table[flag_column].map(build_geosphere_flag) if flag_column else pd.Series(pd.NA, index=table.index, dtype='object'),
                'quality': pd.Series(pd.NA, index=table.index, dtype='Int64'),
                'provider': query.provider,
                'resolution': query.resolution,
                'gh_id': pd.NA,
            }
        )
        if metadata_lookup is not None:
            normalized = normalized.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=GEOSPHERE_NORMALIZED_SUBDAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    if query.start is not None:
        start = pd.Timestamp(query.start)
        if start.tzinfo is None:
            start = start.tz_localize('UTC')
        else:
            start = start.tz_convert('UTC')
        combined = combined[combined['timestamp'] >= start]
    if query.end is not None:
        end = pd.Timestamp(query.end)
        if end.tzinfo is None:
            end = end.tz_localize('UTC')
        else:
            end = end.tz_convert('UTC')
        combined = combined[combined['timestamp'] <= end]
    return combined.loc[:, GEOSPHERE_NORMALIZED_SUBDAILY_COLUMNS].reset_index(drop=True)

