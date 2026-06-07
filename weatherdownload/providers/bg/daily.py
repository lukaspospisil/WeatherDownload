from __future__ import annotations

from datetime import date

import pandas as pd

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_nimh
from .parser import (
    BG_NIMH_NORMALIZED_DAILY_COLUMNS,
    build_source_url,
    normalize_bg_precipitation_month,
    normalize_bg_snow_month,
    parse_bg_month_links,
    parse_bg_precipitation_month,
    parse_bg_snow_month,
    read_text_from_source,
)
from .registry import get_dataset_spec


def download_daily_observations_nimh(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'nimh' or query.resolution != 'daily':
        raise UnsupportedQueryError('The NIMH Bulgaria daily downloader only supports nimh/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The NIMH Bulgaria daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_nimh(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No NIMH Bulgaria station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No NIMH Bulgaria station metadata found for station_id: {', '.join(missing_station_ids)}")

    spec = get_dataset_spec('nimh', 'daily')
    frames: list[pd.DataFrame] = []
    raw_elements = set(query.elements or [])

    if 'precipitation' in raw_elements:
        frames.extend(_download_month_series(query, spec.rain_page_url, slug='prec', timeout=timeout))
    if 'snow_cover_depth' in raw_elements:
        frames.extend(_download_month_series(query, spec.snow_page_url, slug='snow', timeout=timeout))

    if not frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[(combined['observation_date'] >= query.start_date) & (combined['observation_date'] <= query.end_date)]
    if combined.empty:
        raise EmptyResultError('No observations found for the given query.')

    mapping = metadata_table.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
    combined = combined.drop(columns=['gh_id']).merge(mapping, on='station_id', how='left')
    combined = combined.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)
    element_columns = canonicalize_element_series(combined['element_raw'], query)
    combined['element'] = element_columns['element']
    combined['element_raw'] = element_columns['element_raw']
    return combined.loc[:, BG_NIMH_NORMALIZED_DAILY_COLUMNS]


def _download_month_series(
    query: ObservationQuery,
    page_url: str,
    *,
    slug: str,
    timeout: int,
) -> list[pd.DataFrame]:
    html = read_text_from_source(page_url, timeout)
    links = parse_bg_month_links(html, slug=slug)
    if not links:
        return []
    frames: list[pd.DataFrame] = []
    for year, month in _iter_months(query.start_date, query.end_date):
        href = links.get((year, month))
        if href is None:
            continue
        csv_text = read_text_from_source(build_source_url(page_url, href), timeout)
        if slug == 'prec':
            table, _ = parse_bg_precipitation_month(csv_text)
            frames.append(normalize_bg_precipitation_month(table, year=year, month=month, provider=query.provider, resolution=query.resolution))
        else:
            table, _ = parse_bg_snow_month(csv_text)
            frames.append(normalize_bg_snow_month(table, year=year, month=month, provider=query.provider, resolution=query.resolution))
    return frames


def _iter_months(start_date: date, end_date: date) -> list[tuple[int, int]]:
    cursor_year = start_date.year
    cursor_month = start_date.month
    months: list[tuple[int, int]] = []
    while (cursor_year, cursor_month) <= (end_date.year, end_date.month):
        months.append((cursor_year, cursor_month))
        if cursor_month == 12:
            cursor_year += 1
            cursor_month = 1
        else:
            cursor_month += 1
    return months
