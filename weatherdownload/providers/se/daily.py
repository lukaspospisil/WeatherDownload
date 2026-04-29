from __future__ import annotations

from datetime import date

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_se
from .parser import SE_NORMALIZED_DAILY_COLUMNS, parse_se_daily_csv
from .registry import SMHI_METOBS_API_BASE


def download_daily_observations_se(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'historical' or query.resolution != 'daily':
        raise UnsupportedQueryError('The SMHI Sweden daily downloader only supports historical/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The SMHI Sweden daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_se(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No SMHI Sweden station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No SMHI Sweden station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query, metadata_table)
    normalized_frames: list[pd.DataFrame] = []

    for station_id in query.station_ids:
        for parameter_id in query.elements or []:
            csv_text = _download_daily_csv(station_id=station_id, parameter_id=parameter_id, timeout=timeout)
            normalized = normalize_daily_observations_se(
                parse_se_daily_csv(csv_text),
                query,
                parameter_id=parameter_id,
                request_start=request_start,
                request_end=request_end,
                station_metadata=metadata_table,
            )
            if not normalized.empty:
                normalized_frames.append(normalized)

    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(normalized_frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'observation_date', 'element']).reset_index(drop=True)
    return combined.loc[:, SE_NORMALIZED_DAILY_COLUMNS]


def normalize_daily_observations_se(
    parsed_payload: dict[str, object],
    query: ObservationQuery,
    *,
    parameter_id: str,
    request_start: date,
    request_end: date,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    station_id = str(parsed_payload.get('station_id', '')).strip()
    if station_id not in query.station_ids or parameter_id not in (query.elements or []):
        return pd.DataFrame(columns=SE_NORMALIZED_DAILY_COLUMNS)

    records = parsed_payload.get('records')
    if not isinstance(records, pd.DataFrame) or records.empty:
        return pd.DataFrame(columns=SE_NORMALIZED_DAILY_COLUMNS)

    metadata_lookup = None
    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])

    element_columns = canonicalize_element_series(pd.Series([parameter_id]), query).iloc[0]
    rows: list[dict[str, object]] = []
    for record in records.itertuples(index=False):
        if record.observation_date < request_start or record.observation_date > request_end:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'observation_date': record.observation_date,
                'time_function': pd.NA,
                'value': record.value,
                'flag': record.flag,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )

    if not rows:
        return pd.DataFrame(columns=SE_NORMALIZED_DAILY_COLUMNS)

    combined = pd.DataFrame.from_records(rows)
    combined['quality'] = pd.Series(combined['quality'], dtype='Int64')
    if metadata_lookup is not None:
        combined = combined.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
    return combined.loc[:, SE_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)


def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[date, date]:
    if not query.all_history:
        return query.start_date, query.end_date
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    end = pd.to_datetime(selected['end_date'], utc=True, errors='coerce').max()
    if pd.isna(begin) or pd.isna(end):
        raise UnsupportedQueryError('SMHI Sweden all_history mode requires station coverage metadata.')
    return begin.date(), end.date()


def _download_daily_csv(*, station_id: str, parameter_id: str, timeout: int) -> str:
    url = f'{SMHI_METOBS_API_BASE}/parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv'
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text

