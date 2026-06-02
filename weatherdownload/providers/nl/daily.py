from __future__ import annotations

from datetime import date

import pandas as pd

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import download_knmi_daily_text, read_station_metadata_knmi
from .parser import KNMI_NORMALIZED_DAILY_COLUMNS, normalize_knmi_daily_rows, parse_knmi_daily_text


def download_daily_observations_knmi(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'knmi' or query.resolution != 'daily':
        raise UnsupportedQueryError('The KNMI daily downloader only supports knmi/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The KNMI daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_knmi(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No KNMI station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No KNMI station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query)
    response_text = download_knmi_daily_text(
        station_ids=query.station_ids,
        raw_elements=list(query.elements or []),
        start_date=request_start.isoformat(),
        end_date=request_end.isoformat(),
        timeout=timeout,
    )
    parsed = parse_knmi_daily_text(response_text)

    normalized_frames: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        normalized = normalize_knmi_daily_rows(
            parsed,
            raw_code=raw_code,
            provider=query.provider,
            resolution=query.resolution,
            station_ids=set(query.station_ids),
            start_date=query.start_date,
            end_date=query.end_date,
        )
        if normalized.empty:
            continue
        element_columns = canonicalize_element_series(pd.Series(normalized['element_raw']), query)
        normalized['element'] = element_columns['element']
        normalized['element_raw'] = element_columns['element_raw']
        normalized_frames.append(normalized.loc[:, KNMI_NORMALIZED_DAILY_COLUMNS])

    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(normalized_frames, ignore_index=True)
    metadata_lookup = metadata_table.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
    combined = combined.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
    return combined.loc[:, KNMI_NORMALIZED_DAILY_COLUMNS].sort_values(
        ['station_id', 'observation_date', 'element']
    ).reset_index(drop=True)


def _resolve_request_range(query: ObservationQuery) -> tuple[date, date]:
    if not query.all_history:
        return query.start_date, query.end_date
    return date(1901, 1, 1), (pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=1)).date()
