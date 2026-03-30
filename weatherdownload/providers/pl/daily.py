from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_pl
from .parser import (
    PL_NORMALIZED_DAILY_COLUMNS,
    decode_pl_bytes,
    flag_with_missing,
    normalize_pl_gh_id,
    normalize_pl_observation_date,
    parse_pl_daily_klimat_csv,
    parse_pl_daily_synop_csv,
    station_lookup_by_gh_id,
    to_numeric_with_missing,
)
from .registry import PL_DAILY_KLIMAT_BASE_URL, PL_DAILY_SYNOP_BASE_URL, get_dataset_spec


@dataclass(frozen=True)
class PlDailyDownloadTarget:
    station_id: str
    archive_url: str
    source_kind: str
    dataset_scope: str


_SUPPORTED_PL_DAILY_SCOPES = {'historical', 'historical_klimat'}


def download_daily_observations_pl(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope not in _SUPPORTED_PL_DAILY_SCOPES or query.resolution != 'daily':
        raise UnsupportedQueryError('The IMGW Poland daily downloader supports only historical/daily and historical_klimat/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The IMGW Poland daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_pl(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No IMGW Poland station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No IMGW Poland station metadata found for station_id: {', '.join(missing_station_ids)}")

    targets = build_pl_daily_download_targets(query)
    parsed_tables: list[pd.DataFrame] = []
    for target in targets:
        try:
            table = _download_daily_archive(target, timeout=timeout)
        except FileNotFoundError:
            continue
        if table.empty:
            continue
        table['_source_kind'] = target.source_kind
        table['_target_station_id'] = target.station_id
        parsed_tables.append(table)

    if not parsed_tables:
        raise StationNotFoundError(f"No IMGW Poland daily data found for station_id: {', '.join(sorted(query.station_ids))}")

    normalized = normalize_daily_observations_pl(
        pd.concat(parsed_tables, ignore_index=True),
        query,
        station_metadata=metadata_table,
    )
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, PL_NORMALIZED_DAILY_COLUMNS]


def build_pl_daily_download_targets(query: ObservationQuery) -> list[PlDailyDownloadTarget]:
    spec = get_dataset_spec(query.dataset_scope, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested IMGW Poland dataset path is not implemented.')

    request_start, request_end = _resolve_request_range(query)
    if query.dataset_scope == 'historical':
        return _build_pl_daily_synop_download_targets(query.station_ids, request_start, request_end)
    if query.dataset_scope == 'historical_klimat':
        return _build_pl_daily_klimat_download_targets(query.station_ids, request_start, request_end)
    raise UnsupportedQueryError('The requested IMGW Poland dataset path is not implemented.')


def normalize_daily_observations_pl(
    table: pd.DataFrame,
    query: ObservationQuery,
    station_metadata: pd.DataFrame,
) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=PL_NORMALIZED_DAILY_COLUMNS)

    gh_lookup = station_lookup_by_gh_id(station_metadata)
    resolved_station_ids = table['NSP'].map(lambda value: gh_lookup.get(normalize_pl_gh_id(value), ''))
    fallback_station_ids = table['_target_station_id'].astype('string').fillna('')
    table = table.copy()
    table['_resolved_station_id'] = resolved_station_ids.where(resolved_station_ids.ne(''), fallback_station_ids)

    rows: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        if raw_code not in table.columns:
            continue
        flag_column_name = _flag_column_for_element(raw_code)
        flag_series = table[flag_column_name] if flag_column_name in table.columns else pd.Series(pd.NA, index=table.index, dtype='string')
        zero_when_flag_nine = raw_code in {'SMDB', 'USL'}
        element_columns = canonicalize_element_series(pd.Series([raw_code] * len(table.index), index=table.index), query)
        normalized = pd.DataFrame(
            {
                'station_id': table['_resolved_station_id'].astype('string').str.strip(),
                'gh_id': table['NSP'].map(normalize_pl_gh_id).replace('', pd.NA),
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'observation_date': table.apply(normalize_pl_observation_date, axis=1),
                'time_function': pd.NA,
                'value': to_numeric_with_missing(table[raw_code], flag_series=flag_series, zero_when_flag_nine=zero_when_flag_nine),
                'flag': flag_with_missing(flag_series),
                'quality': pd.Series(pd.NA, index=table.index, dtype='Int64'),
                'dataset_scope': query.dataset_scope,
                'resolution': query.resolution,
            }
        )
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=PL_NORMALIZED_DAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[combined['observation_date'].notna()]
    if query.start_date is not None:
        combined = combined[combined['observation_date'] >= query.start_date]
    if query.end_date is not None:
        combined = combined[combined['observation_date'] <= query.end_date]
    combined = combined.sort_values(['station_id', 'observation_date', 'element']).reset_index(drop=True)
    return combined.loc[:, PL_NORMALIZED_DAILY_COLUMNS]


def _resolve_request_range(query: ObservationQuery) -> tuple[date, date]:
    if query.all_history:
        return date(1900, 1, 1), pd.Timestamp.today().date()
    return query.start_date, query.end_date


def _build_pl_daily_synop_download_targets(station_ids: list[str], request_start: date, request_end: date) -> list[PlDailyDownloadTarget]:
    targets: list[PlDailyDownloadTarget] = []
    seen: set[tuple[str, str]] = set()
    current_year = pd.Timestamp.today().year

    for station_id in station_ids:
        for year in range(request_start.year, request_end.year + 1):
            if year < 2001:
                bucket_start = year - ((year - 1996) % 5)
                bucket_end = bucket_start + 4
                archive_name = f'{bucket_start}_{bucket_end}_{int(station_id)}_s.zip'
                archive_url = f'{PL_DAILY_SYNOP_BASE_URL}/{bucket_start}_{bucket_end}/{archive_name}'
                key = (station_id, archive_url)
                if key not in seen:
                    seen.add(key)
                    targets.append(PlDailyDownloadTarget(station_id=station_id, archive_url=archive_url, source_kind='station_archive', dataset_scope='historical'))
                continue

            if year < current_year:
                archive_name = f'{year}_{int(station_id)}_s.zip'
                archive_url = f'{PL_DAILY_SYNOP_BASE_URL}/{year}/{archive_name}'
                key = (station_id, archive_url)
                if key not in seen:
                    seen.add(key)
                    targets.append(PlDailyDownloadTarget(station_id=station_id, archive_url=archive_url, source_kind='station_archive', dataset_scope='historical'))
                continue

            month_start = 1 if year != request_start.year else request_start.month
            month_end = 12 if year != request_end.year else request_end.month
            for month in range(month_start, month_end + 1):
                archive_name = f'{year}_{month:02d}_s.zip'
                archive_url = f'{PL_DAILY_SYNOP_BASE_URL}/{year}/{archive_name}'
                key = (station_id, archive_url)
                if key in seen:
                    continue
                seen.add(key)
                targets.append(PlDailyDownloadTarget(station_id=station_id, archive_url=archive_url, source_kind='monthly_all_stations', dataset_scope='historical'))
    return targets


def _build_pl_daily_klimat_download_targets(station_ids: list[str], request_start: date, request_end: date) -> list[PlDailyDownloadTarget]:
    targets: list[PlDailyDownloadTarget] = []
    seen: set[tuple[str, str]] = set()

    for station_id in station_ids:
        for year in range(request_start.year, request_end.year + 1):
            if year < 2001:
                bucket_start = year - ((year - 1996) % 5)
                bucket_end = bucket_start + 4
                archive_name = f'{year}_k.zip'
                archive_url = f'{PL_DAILY_KLIMAT_BASE_URL}/{bucket_start}_{bucket_end}/{archive_name}'
                key = (station_id, archive_url)
                if key in seen:
                    continue
                seen.add(key)
                targets.append(PlDailyDownloadTarget(station_id=station_id, archive_url=archive_url, source_kind='yearly_all_stations', dataset_scope='historical_klimat'))
                continue

            month_start = 1 if year != request_start.year else request_start.month
            month_end = 12 if year != request_end.year else request_end.month
            for month in range(month_start, month_end + 1):
                archive_name = f'{year}_{month:02d}_k.zip'
                archive_url = f'{PL_DAILY_KLIMAT_BASE_URL}/{year}/{archive_name}'
                key = (station_id, archive_url)
                if key in seen:
                    continue
                seen.add(key)
                targets.append(PlDailyDownloadTarget(station_id=station_id, archive_url=archive_url, source_kind='monthly_all_stations', dataset_scope='historical_klimat'))
    return targets


def _download_daily_archive(target: PlDailyDownloadTarget, timeout: int) -> pd.DataFrame:
    response = requests.get(target.archive_url, timeout=timeout)
    if response.status_code == 404:
        raise FileNotFoundError(target.archive_url)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        product_names = [name for name in archive.namelist() if name.lower().endswith('.csv')]
        if not product_names:
            raise ValueError(f'No IMGW Poland daily CSV found in archive for station_id {target.station_id}.')
        csv_text = decode_pl_bytes(archive.read(product_names[0]))
    if target.dataset_scope == 'historical_klimat':
        return parse_pl_daily_klimat_csv(csv_text)
    return parse_pl_daily_synop_csv(csv_text)


def _flag_column_for_element(raw_code: str) -> str:
    return {
        'TMAX': 'WTMAX',
        'TMIN': 'WTMIN',
        'STD': 'WSTD',
        'SMDB': 'WSMDB',
        'USL': 'WUSL',
    }.get(raw_code, '')
