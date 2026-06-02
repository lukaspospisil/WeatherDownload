from __future__ import annotations

from datetime import date

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_ilmateenistus
from .parser import (
    EE_ILMATEENISTUS_NORMALIZED_DAILY_COLUMNS,
    normalize_ee_daily_rows,
    parse_ee_payload_json,
)
from .registry import get_dataset_spec


def download_daily_observations_ilmateenistus(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'ilmateenistus' or query.resolution != 'daily':
        raise UnsupportedQueryError('The Estonia daily downloader supports only ilmateenistus/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The Estonia daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_ilmateenistus(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No Estonian station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(
            f"No Estonian station metadata found for station_id: {', '.join(missing_station_ids)}"
        )

    request_start, request_end = _resolve_request_range(query, metadata_table)
    frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        for year in range(request_start.year, request_end.year + 1):
            records = _download_station_year_records(
                station_id=station_id,
                year=year,
                raw_elements=list(query.elements),
                timeout=timeout,
            )
            if not records:
                continue
            for raw_code in query.elements:
                normalized = normalize_ee_daily_rows(
                    records,
                    raw_code=raw_code,
                    provider=query.provider,
                    resolution=query.resolution,
                    station_id=station_id,
                    start_date=query.start_date,
                    end_date=query.end_date,
                )
                if normalized.empty:
                    continue
                element_columns = canonicalize_element_series(pd.Series(normalized['element_raw']), query)
                normalized['element'] = element_columns['element']
                normalized['element_raw'] = element_columns['element_raw']
                frames.append(normalized.loc[:, EE_ILMATEENISTUS_NORMALIZED_DAILY_COLUMNS])

    if not frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(frames, ignore_index=True)
    metadata_lookup = metadata_table.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
    combined = combined.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
    combined = combined.drop_duplicates(
        subset=['station_id', 'element', 'observation_date'],
        keep='last',
    ).reset_index(drop=True)
    return combined.loc[:, EE_ILMATEENISTUS_NORMALIZED_DAILY_COLUMNS].sort_values(
        ['station_id', 'observation_date', 'element'],
        kind='stable',
    ).reset_index(drop=True)


def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[date, date]:
    if not query.all_history:
        return query.start_date, query.end_date
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    if pd.isna(begin):
        raise UnsupportedQueryError('Estonia all_history mode requires station coverage metadata.')
    return begin.date(), pd.Timestamp.utcnow().normalize().date()


def _download_station_year_records(
    *,
    station_id: str,
    year: int,
    raw_elements: list[str],
    timeout: int,
) -> list[dict[str, object]]:
    spec = get_dataset_spec('ilmateenistus', 'daily')
    params = {
        'jaam_kood': f'eq.{station_id}',
        'aasta': f'eq.{year}',
        'element_kood': f"in.({','.join(raw_elements)})",
        'order': 'kuu.asc,paev.asc,element_kood.asc',
        'limit': 5000,
    }
    response = requests.get(spec.observation_base_url, params=params, timeout=timeout)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise DownloadError(
            f'Failed to download Estonian daily observations for station_id {station_id} and year {year}: {exc}'
        ) from exc
    response.encoding = 'utf-8'
    return parse_ee_payload_json(response.text)
