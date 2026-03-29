from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import requests

from .metadata import read_station_metadata_ch
from .parser import (
    CH_NORMALIZED_SUBDAILY_COLUMNS,
    current_year_utc,
    empty_flag_series,
    historical_asset_year_range,
    normalize_ch_observation_timestamp,
    normalize_ch_query_timestamp,
    parse_ch_observation_csv,
    parse_ch_station_item_json,
    read_text_from_source,
    to_numeric_with_missing,
)
from .registry import get_dataset_spec
from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery


@dataclass(frozen=True)
class ChTenminDownloadTarget:
    station_id: str
    asset_url: str
    source_kind: str



def download_tenmin_observations_ch(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != '10min':
        raise UnsupportedQueryError('The MeteoSwiss Switzerland 10-minute downloader only supports historical/10min.')
    if not query.elements:
        raise UnsupportedQueryError('The MeteoSwiss Switzerland 10-minute downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_ch(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No MeteoSwiss Switzerland station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No MeteoSwiss Switzerland station metadata found for station_id: {', '.join(missing_station_ids)}")

    targets = build_ch_tenmin_download_targets(query, timeout=timeout)
    parsed_tables: list[pd.DataFrame] = []
    for target in targets:
        table = _download_tenmin_asset(target.asset_url, timeout=timeout)
        if table.empty:
            continue
        table['_source_kind'] = target.source_kind
        parsed_tables.append(table)

    if not parsed_tables:
        raise StationNotFoundError(f"No MeteoSwiss Switzerland 10-minute data found for station_id: {', '.join(sorted(query.station_ids))}")

    normalized = normalize_tenmin_observations_ch(pd.concat(parsed_tables, ignore_index=True), query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, CH_NORMALIZED_SUBDAILY_COLUMNS]



def build_ch_tenmin_download_targets(query: ObservationQuery, timeout: int = 60) -> list[ChTenminDownloadTarget]:
    spec = get_dataset_spec(query.dataset_scope, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested MeteoSwiss Switzerland dataset path is not implemented.')

    request_start = normalize_ch_query_timestamp(query.start)
    request_end = normalize_ch_query_timestamp(query.end)
    current_year = current_year_utc()
    targets: list[ChTenminDownloadTarget] = []
    for station_id in query.station_ids:
        assets = _fetch_station_assets(spec, station_id, timeout=timeout)
        for asset_name, asset_url in assets.items():
            if not asset_name.startswith(f'ogd-smn_{station_id.lower()}_t_historical_'):
                continue
            year_range = historical_asset_year_range(asset_name)
            if year_range is None:
                continue
            start_year, end_year = year_range
            asset_start = pd.Timestamp(year=start_year, month=1, day=1, tz='UTC')
            asset_end = pd.Timestamp(year=end_year, month=12, day=31, hour=23, minute=50, tz='UTC')
            if asset_end < request_start or asset_start > request_end:
                continue
            targets.append(ChTenminDownloadTarget(station_id=station_id, asset_url=asset_url, source_kind='historical'))
        recent_url = assets.get(f'ogd-smn_{station_id.lower()}_t_recent.csv')
        if recent_url is not None and (query.all_history or request_end.year >= current_year):
            targets.append(ChTenminDownloadTarget(station_id=station_id, asset_url=recent_url, source_kind='recent'))
    return targets



def normalize_tenmin_observations_ch(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=CH_NORMALIZED_SUBDAILY_COLUMNS)

    request_start = normalize_ch_query_timestamp(query.start)
    request_end = normalize_ch_query_timestamp(query.end)
    rows: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        if raw_code not in table.columns:
            continue
        element_columns = canonicalize_element_series(pd.Series([raw_code] * len(table.index), index=table.index), query)
        normalized = pd.DataFrame(
            {
                'station_id': table['station_abbr'].astype('string').str.upper(),
                'gh_id': pd.NA,
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'timestamp': table['reference_timestamp'].map(normalize_ch_observation_timestamp),
                'value': to_numeric_with_missing(table[raw_code]),
                'flag': empty_flag_series(table.index),
                'quality': pd.Series(pd.NA, index=table.index, dtype='Int64'),
                'dataset_scope': query.dataset_scope,
                'resolution': query.resolution,
                '_source_kind': table['_source_kind'].astype('string'),
            }
        )
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=CH_NORMALIZED_SUBDAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[combined['timestamp'].notna()]
    combined = combined[(combined['timestamp'] >= request_start) & (combined['timestamp'] <= request_end)]
    current_year = current_year_utc()
    combined['_year'] = combined['timestamp'].dt.year
    combined = combined[
        ((combined['_source_kind'] == 'historical') & (combined['_year'] < current_year))
        | ((combined['_source_kind'] == 'recent') & (combined['_year'] >= current_year))
    ]
    combined['_priority'] = combined['_source_kind'].map({'historical': 0, 'recent': 1}).fillna(0)
    combined = combined.sort_values(['station_id', 'timestamp', 'element', '_priority'])
    combined = combined.drop_duplicates(subset=['station_id', 'timestamp', 'element'], keep='last')
    combined = combined.drop(columns=['_source_kind', '_year', '_priority']).reset_index(drop=True)
    return combined.loc[:, CH_NORMALIZED_SUBDAILY_COLUMNS]



def _fetch_station_assets(spec, station_id: str, timeout: int) -> dict[str, str]:
    item_url = spec.item_url_template.format(station_id=station_id.lower())
    item_text = read_text_from_source(item_url, timeout, requests)
    return parse_ch_station_item_json(item_text)



def _download_tenmin_asset(asset_url: str, timeout: int) -> pd.DataFrame:
    csv_text = read_text_from_source(asset_url, timeout, requests)
    return parse_ch_observation_csv(csv_text)

