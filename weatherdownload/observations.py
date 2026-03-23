from __future__ import annotations

import pandas as pd

from .chmi_daily import (
    NORMALIZED_DAILY_COLUMNS,
    build_daily_download_targets,
    download_daily_csv,
    normalize_daily_observations,
    parse_daily_csv,
)
from .chmi_hourly import (
    NORMALIZED_HOURLY_COLUMNS,
    build_hourly_download_targets,
    download_hourly_csv,
    normalize_hourly_observations,
    parse_hourly_csv,
)
from .chmi_tenmin import (
    NORMALIZED_TENMIN_COLUMNS,
    build_tenmin_download_targets,
    download_tenmin_csv,
    normalize_tenmin_observations,
    parse_tenmin_csv,
)
from .dwd_daily import download_daily_observations_dwd
from .dwd_subdaily import NORMALIZED_DWD_SUBDAILY_COLUMNS, download_subdaily_observations_dwd
from .chmi_registry import get_dataset_spec as get_chmi_dataset_spec
from .errors import DatasetNotImplementedError, DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .queries import ObservationQuery
from .shmu_observations import NORMALIZED_SHMU_DAILY_COLUMNS, download_daily_observations_shmu


def download_observations(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
    country: str | None = None,
) -> pd.DataFrame:
    from .providers import get_provider, normalize_country_code

    resolved_country = normalize_country_code(country or query.country)
    provider = get_provider(resolved_country)
    return provider.download_observations(query, timeout, station_metadata)


def _download_observations_chmi(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    from .metadata import read_station_metadata

    dataset_spec = get_chmi_dataset_spec(query.dataset_scope, query.resolution)
    if not dataset_spec.implemented:
        raise DatasetNotImplementedError(
            f"Dataset path '{query.dataset_scope}/{query.resolution}' is valid in CHMI, but is not implemented by this library yet."
        )
    metadata_table = station_metadata if station_metadata is not None else read_station_metadata(country='CZ', timeout=timeout)

    if query.dataset_scope == 'historical_csv' and query.resolution == '10min':
        return _download_tenmin_observations(query, timeout=timeout, station_metadata=metadata_table)
    if query.dataset_scope == 'historical_csv' and query.resolution == 'daily':
        return _download_daily_observations(query, timeout=timeout, station_metadata=metadata_table)
    if query.dataset_scope == 'historical_csv' and query.resolution == '1hour':
        return _download_hourly_observations(query, timeout=timeout, station_metadata=metadata_table)

    raise UnsupportedQueryError(
        f'Unsupported query combination for the current downloader implementation: {query.dataset_scope}/{query.resolution}'
    )


def _download_observations_dwd(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope == 'historical' and query.resolution == 'daily':
        return download_daily_observations_dwd(query, timeout=timeout, station_metadata=station_metadata)
    if query.dataset_scope == 'historical' and query.resolution in {'1hour', '10min'}:
        return download_subdaily_observations_dwd(query, timeout=timeout, station_metadata=station_metadata).loc[:, NORMALIZED_DWD_SUBDAILY_COLUMNS]
    raise NotImplementedError('Only the first DWD historical downloader paths for daily, 1hour, and 10min are implemented so far.')


def _download_observations_shmu(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope == 'recent' and query.resolution == 'daily':
        return download_daily_observations_shmu(query, timeout=timeout, station_metadata=station_metadata).loc[:, NORMALIZED_SHMU_DAILY_COLUMNS]
    raise NotImplementedError('Experimental SHMU support currently implements only recent/daily station observations.')


def _download_tenmin_observations(query: ObservationQuery, timeout: int, station_metadata: pd.DataFrame | None) -> pd.DataFrame:
    if not query.elements:
        raise UnsupportedQueryError('The 10min historical_csv downloader requires at least one element.')
    targets = build_tenmin_download_targets(query, timeout=timeout)
    parsed_tables: list[pd.DataFrame] = []
    missing_station_ids: set[str] = set()
    any_downloaded = False
    for target in targets:
        try:
            csv_text = download_tenmin_csv(target, timeout=timeout)
        except FileNotFoundError:
            missing_station_ids.add(target.station_id)
            continue
        except DownloadError:
            raise
        any_downloaded = True
        parsed_tables.append(parse_tenmin_csv(csv_text))
    if not any_downloaded:
        if missing_station_ids:
            station_list = ', '.join(sorted(missing_station_ids))
            raise StationNotFoundError(f'No 10min historical_csv data found for station_id: {station_list}')
        raise EmptyResultError('No observations found for the given query.')
    merged = pd.concat(parsed_tables, ignore_index=True)
    normalized = normalize_tenmin_observations(merged, query, station_metadata=station_metadata)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, NORMALIZED_TENMIN_COLUMNS]


def _download_daily_observations(query: ObservationQuery, timeout: int, station_metadata: pd.DataFrame | None) -> pd.DataFrame:
    if not query.elements:
        raise UnsupportedQueryError('The daily historical_csv downloader requires at least one element.')
    targets = build_daily_download_targets(query)
    parsed_tables: list[pd.DataFrame] = []
    missing_station_ids: set[str] = set()
    any_downloaded = False
    for target in targets:
        try:
            csv_text = download_daily_csv(target, timeout=timeout)
        except FileNotFoundError:
            missing_station_ids.add(target.station_id)
            continue
        except DownloadError:
            raise
        any_downloaded = True
        parsed_tables.append(parse_daily_csv(csv_text))
    if not any_downloaded:
        if missing_station_ids:
            station_list = ', '.join(sorted(missing_station_ids))
            raise StationNotFoundError(f'No daily historical_csv data found for station_id: {station_list}')
        raise EmptyResultError('No observations found for the given query.')
    merged = pd.concat(parsed_tables, ignore_index=True)
    normalized = normalize_daily_observations(merged, query, station_metadata=station_metadata)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, NORMALIZED_DAILY_COLUMNS]


def _download_hourly_observations(query: ObservationQuery, timeout: int, station_metadata: pd.DataFrame | None) -> pd.DataFrame:
    if not query.elements:
        raise UnsupportedQueryError('The hourly historical_csv downloader requires at least one element.')
    targets = build_hourly_download_targets(query, timeout=timeout)
    parsed_tables: list[pd.DataFrame] = []
    missing_station_ids: set[str] = set()
    any_downloaded = False
    for target in targets:
        try:
            csv_text = download_hourly_csv(target, timeout=timeout)
        except FileNotFoundError:
            missing_station_ids.add(target.station_id)
            continue
        except DownloadError:
            raise
        any_downloaded = True
        parsed_tables.append(parse_hourly_csv(csv_text))
    if not any_downloaded:
        if missing_station_ids:
            station_list = ', '.join(sorted(missing_station_ids))
            raise StationNotFoundError(f'No hourly historical_csv data found for station_id: {station_list}')
        raise EmptyResultError('No observations found for the given query.')
    merged = pd.concat(parsed_tables, ignore_index=True)
    normalized = normalize_hourly_observations(merged, query, station_metadata=station_metadata)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, NORMALIZED_HOURLY_COLUMNS]
