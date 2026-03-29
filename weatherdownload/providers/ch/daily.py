from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import requests

from .metadata import read_station_metadata_ch
from .parser import (
    CH_NORMALIZED_DAILY_COLUMNS,
    current_year_utc,
    empty_flag_series,
    normalize_ch_observation_date,
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
class ChDailyDownloadTarget:
    station_id: str
    asset_url: str
    source_kind: str



def download_daily_observations_ch(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != 'daily':
        raise UnsupportedQueryError('The MeteoSwiss Switzerland daily downloader only supports historical/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The MeteoSwiss Switzerland daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_ch(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No MeteoSwiss Switzerland station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No MeteoSwiss Switzerland station metadata found for station_id: {', '.join(missing_station_ids)}")

    targets = build_ch_daily_download_targets(query, timeout=timeout)
    parsed_tables: list[pd.DataFrame] = []
    for target in targets:
        table = _download_daily_asset(target.asset_url, timeout=timeout)
        if table.empty:
            continue
        table['_source_kind'] = target.source_kind
        parsed_tables.append(table)

    if not parsed_tables:
        raise StationNotFoundError(f"No MeteoSwiss Switzerland daily data found for station_id: {', '.join(sorted(query.station_ids))}")

    normalized = normalize_daily_observations_ch(pd.concat(parsed_tables, ignore_index=True), query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, CH_NORMALIZED_DAILY_COLUMNS]



def build_ch_daily_download_targets(query: ObservationQuery, timeout: int = 60) -> list[ChDailyDownloadTarget]:
    spec = get_dataset_spec(query.dataset_scope, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested MeteoSwiss Switzerland dataset path is not implemented.')

    current_year = current_year_utc()
    request_start = pd.Timestamp(query.start_date)
    request_end = pd.Timestamp(query.end_date)
    targets: list[ChDailyDownloadTarget] = []
    for station_id in query.station_ids:
        assets = _fetch_station_assets(spec, station_id, timeout=timeout)
        historical_url = assets.get(f'ogd-smn_{station_id.lower()}_d_historical.csv')
        recent_url = assets.get(f'ogd-smn_{station_id.lower()}_d_recent.csv')
        if historical_url is not None and (query.all_history or request_start.year < current_year):
            targets.append(ChDailyDownloadTarget(station_id=station_id, asset_url=historical_url, source_kind='historical'))
        if recent_url is not None and (query.all_history or request_end.year >= current_year):
            targets.append(ChDailyDownloadTarget(station_id=station_id, asset_url=recent_url, source_kind='recent'))
    return targets



def normalize_daily_observations_ch(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=CH_NORMALIZED_DAILY_COLUMNS)

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
                'observation_date': table['reference_timestamp'].map(normalize_ch_observation_date),
                'time_function': pd.NA,
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
        return pd.DataFrame(columns=CH_NORMALIZED_DAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[combined['observation_date'].notna()]
    combined = combined[(combined['observation_date'] >= query.start_date) & (combined['observation_date'] <= query.end_date)]
    current_year = current_year_utc()
    combined['_year'] = combined['observation_date'].map(lambda value: value.year)
    combined = combined[
        ((combined['_source_kind'] == 'historical') & (combined['_year'] < current_year))
        | ((combined['_source_kind'] == 'recent') & (combined['_year'] >= current_year))
    ]
    combined['_priority'] = combined['_source_kind'].map({'historical': 0, 'recent': 1}).fillna(0)
    combined = combined.sort_values(['station_id', 'observation_date', 'element', '_priority'])
    combined = combined.drop_duplicates(subset=['station_id', 'observation_date', 'element'], keep='last')
    combined = combined.drop(columns=['_source_kind', '_year', '_priority']).reset_index(drop=True)
    return combined.loc[:, CH_NORMALIZED_DAILY_COLUMNS]



def _fetch_station_assets(spec, station_id: str, timeout: int) -> dict[str, str]:
    item_url = spec.item_url_template.format(station_id=station_id.lower())
    item_text = read_text_from_source(item_url, timeout, requests)
    return parse_ch_station_item_json(item_text)



def _download_daily_asset(asset_url: str, timeout: int) -> pd.DataFrame:
    csv_text = read_text_from_source(asset_url, timeout, requests)
    return parse_ch_observation_csv(csv_text)

