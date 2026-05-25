from __future__ import annotations

import pandas as pd
import requests

from .metadata import read_station_metadata_ie
from .parser import IE_NORMALIZED_DAILY_COLUMNS, normalize_ie_daily_rows, parse_ie_daily_csv_text
from .registry import IE_METEIREANN_DAILY_CSV_URL_TEMPLATE
from ...elements import canonicalize_element_series
from ...errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery


def download_daily_observations_ie(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'meteireann' or query.resolution != 'daily':
        raise UnsupportedQueryError('The Ireland daily downloader supports only meteireann/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The Met Eireann Ireland daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_ie(timeout=timeout)
    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No Met Eireann station metadata found for station_id: {', '.join(missing_station_ids)}")

    frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        station_rows = metadata_table[metadata_table['station_id'].astype(str) == station_id]
        station_name = str(station_rows.iloc[0]['full_name']) if not station_rows.empty else None
        parsed = _download_station_csv(station_id=station_id, timeout=timeout)
        for raw_code in query.elements:
            normalized = normalize_ie_daily_rows(
                parsed,
                raw_code=raw_code,
                provider=query.provider,
                resolution=query.resolution,
                station_id=station_id,
                expected_station_name=station_name,
            )
            if normalized.empty:
                continue
            if not query.all_history:
                normalized = normalized[
                    (normalized['observation_date'] >= query.start_date)
                    & (normalized['observation_date'] <= query.end_date)
                ].copy()
            if normalized.empty:
                continue
            element_columns = canonicalize_element_series(pd.Series(normalized['element_raw']), query)
            normalized['element'] = element_columns['element']
            normalized['element_raw'] = element_columns['element_raw']
            frames.append(normalized.loc[:, IE_NORMALIZED_DAILY_COLUMNS])

    if not frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['station_id', 'element', 'observation_date']).reset_index(drop=True)
    return combined.loc[:, IE_NORMALIZED_DAILY_COLUMNS]


def _download_station_csv(*, station_id: str, timeout: int):
    url = IE_METEIREANN_DAILY_CSV_URL_TEMPLATE.format(station_id=station_id)
    response = requests.get(url, timeout=timeout)
    if response.status_code == 404:
        raise StationNotFoundError(f'No Met Eireann daily CSV data found for station_id: {station_id}')
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise DownloadError(f'Failed to download Met Eireann daily CSV for station_id {station_id}: {exc}') from exc
    response.encoding = 'utf-8'
    try:
        return parse_ie_daily_csv_text(response.text)
    except ValueError as exc:
        raise EmptyResultError(f'Met Eireann daily CSV for station_id {station_id} is empty or invalid: {exc}') from exc
