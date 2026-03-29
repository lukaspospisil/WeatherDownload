from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .parser import (
    normalize_recent_daily_long_table,
    parse_recent_daily_month_directories,
    parse_recent_daily_month_files,
    parse_recent_daily_payload_json,
)
from .registry import ShmuDatasetSpec, get_dataset_spec

NORMALIZED_SHMU_DAILY_COLUMNS = [
    'station_id',
    'gh_id',
    'element',
    'element_raw',
    'observation_date',
    'time_function',
    'value',
    'flag',
    'quality',
    'dataset_scope',
    'resolution',
]


def download_daily_observations_shmu(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'recent' or query.resolution != 'daily':
        raise UnsupportedQueryError('Experimental SHMU provider currently supports only recent/daily observations.')
    if query.all_history:
        raise UnsupportedQueryError('Experimental SHMU recent/daily downloader does not support all_history yet.')
    if not query.elements:
        raise UnsupportedQueryError('The experimental SHMU recent/daily downloader requires at least one element.')

    spec = get_dataset_spec('recent', 'daily')
    month_urls = build_recent_daily_month_urls(query, spec=spec, timeout=timeout)
    tables: list[pd.DataFrame] = []
    for month_url in month_urls:
        payload_text = _read_text(month_url, timeout=timeout)
        _, raw_table = parse_recent_daily_payload_json(payload_text)
        tables.append(raw_table)
    if not tables:
        raise EmptyResultError('No SHMU observations found for the given query.')

    merged = pd.concat(tables, ignore_index=True)
    normalized = normalize_daily_observations_shmu(merged, query=query, station_metadata=station_metadata)
    if normalized.empty:
        raise EmptyResultError('No SHMU observations found for the given query.')
    return normalized.loc[:, NORMALIZED_SHMU_DAILY_COLUMNS]


def resolve_latest_recent_daily_data_url(timeout: int = 60) -> str:
    spec = get_dataset_spec('recent', 'daily')
    month_directories = parse_recent_daily_month_directories(_read_text(spec.data_index_url, timeout=timeout))
    if not month_directories:
        raise EmptyResultError('No SHMU recent daily month directories were found.')
    latest_month = month_directories[-1]
    month_dir_url = f'{spec.data_index_url}{latest_month}/'
    month_files = parse_recent_daily_month_files(_read_text(month_dir_url, timeout=timeout))
    if not month_files:
        raise EmptyResultError(f'No SHMU recent daily files were found in {month_dir_url}')
    return f'{month_dir_url}{month_files[-1]}'


def build_recent_daily_month_urls(query: ObservationQuery, *, spec: ShmuDatasetSpec, timeout: int) -> list[str]:
    if query.start_date is None or query.end_date is None:
        raise UnsupportedQueryError('Experimental SHMU recent/daily downloader requires start_date and end_date.')
    requested_months = sorted({
        f'{timestamp.year:04d}-{timestamp.month:02d}'
        for timestamp in pd.date_range(query.start_date, query.end_date, freq='D')
    })
    month_directory_lookup = {
        month: f'{spec.data_index_url}{month}/'
        for month in parse_recent_daily_month_directories(_read_text(spec.data_index_url, timeout=timeout))
    }
    urls: list[str] = []
    missing_months: list[str] = []
    for month in requested_months:
        month_dir_url = month_directory_lookup.get(month)
        if month_dir_url is None:
            missing_months.append(month)
            continue
        month_files = parse_recent_daily_month_files(_read_text(month_dir_url, timeout=timeout))
        expected_file = f'kli-inter - {month}.json'
        if expected_file not in month_files:
            missing_months.append(month)
            continue
        urls.append(f'{month_dir_url}{expected_file}')
    if missing_months:
        raise StationNotFoundError(f'SHMU recent/daily files are not available for month(s): {", ".join(missing_months)}')
    return urls


def normalize_daily_observations_shmu(
    raw_table: pd.DataFrame,
    *,
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    normalized_long = normalize_recent_daily_long_table(raw_table, query.elements or [])
    if normalized_long.empty:
        return pd.DataFrame(columns=NORMALIZED_SHMU_DAILY_COLUMNS)

    normalized = normalized_long[
        normalized_long['station_id'].isin(query.station_ids)
        & normalized_long['observation_date'].between(query.start_date, query.end_date)
    ].copy()
    if normalized.empty:
        return pd.DataFrame(columns=NORMALIZED_SHMU_DAILY_COLUMNS)

    element_frame = canonicalize_element_series(normalized['element_raw'], query)
    normalized['element'] = element_frame['element']
    normalized['element_raw'] = element_frame['element_raw']
    normalized['gh_id'] = pd.NA
    normalized['time_function'] = pd.NA
    normalized['flag'] = pd.NA
    normalized['quality'] = pd.NA
    normalized['dataset_scope'] = query.dataset_scope
    normalized['resolution'] = query.resolution

    if station_metadata is not None and not station_metadata.empty:
        gh_lookup = station_metadata.set_index('station_id')['gh_id']
        normalized['gh_id'] = normalized['station_id'].map(gh_lookup).astype('object')

    return normalized.loc[:, NORMALIZED_SHMU_DAILY_COLUMNS].reset_index(drop=True)


def _read_text(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text

