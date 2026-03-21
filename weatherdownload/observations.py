from __future__ import annotations

import pandas as pd

from .chmi_daily import (
    NORMALIZED_DAILY_COLUMNS,
    build_daily_download_targets,
    download_daily_csv,
    normalize_daily_observations,
    parse_daily_csv,
)
from .errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .metadata import read_station_metadata
from .queries import ObservationQuery


def download_observations(query: ObservationQuery, timeout: int = 60, station_metadata: pd.DataFrame | None = None) -> pd.DataFrame:
    if query.dataset_scope != 'historical_csv' or query.resolution != 'daily':
        raise UnsupportedQueryError(
            'Unsupported query combination: only dataset_scope="historical_csv" and resolution="daily" are currently implemented.'
        )
    if not query.elements:
        raise UnsupportedQueryError('The daily historical_csv downloader requires at least one element.')
    metadata_table = station_metadata if station_metadata is not None else read_station_metadata(timeout=timeout)
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
            raise StationNotFoundError(f"No daily historical_csv data found for station_id: {station_list}")
        raise EmptyResultError('No observations found for the given query.')
    merged = pd.concat(parsed_tables, ignore_index=True)
    normalized = normalize_daily_observations(merged, query, station_metadata=metadata_table)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, NORMALIZED_DAILY_COLUMNS]
