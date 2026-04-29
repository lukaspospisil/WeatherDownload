from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .parser import FR_NORMALIZED_DAILY_COLUMNS, decode_fr_daily_payload, normalize_fr_daily_observations, parse_fr_daily_csv
from .registry import get_dataset_spec


@dataclass(frozen=True)
class FrDailyDownloadTarget:
    department: str
    period_key: str
    url: str


def download_daily_observations_fr(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'meteo_france' or query.resolution != 'daily':
        raise UnsupportedQueryError("The Meteo-France daily downloader supports only provider='meteo_france' and resolution='daily'.")
    if not query.elements:
        raise UnsupportedQueryError('The Meteo-France daily downloader requires at least one element.')

    metadata_table = station_metadata
    if metadata_table is not None and not metadata_table.empty:
        available_station_ids = set(metadata_table['station_id'].astype(str))
        missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
        if missing_station_ids:
            raise StationNotFoundError(f"No Meteo-France station metadata found for station_id: {', '.join(missing_station_ids)}")

    targets = build_fr_daily_download_targets(query)
    parsed_tables: list[pd.DataFrame] = []
    for target in targets:
        try:
            table = _download_target_table(target, timeout=timeout)
        except FileNotFoundError:
            continue
        if table.empty:
            continue
        parsed_tables.append(table)

    if not parsed_tables:
        raise StationNotFoundError(f"No Meteo-France daily RR-T-Vent data found for station_id: {', '.join(sorted(query.station_ids))}")

    normalized = normalize_fr_daily_observations(pd.concat(parsed_tables, ignore_index=True), query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, FR_NORMALIZED_DAILY_COLUMNS]


def build_fr_daily_download_targets(query: ObservationQuery) -> list[FrDailyDownloadTarget]:
    spec = get_dataset_spec(query.provider, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested Meteo-France dataset path is not implemented.')

    request_start, request_end = _resolve_request_range(query)
    bucket_keys = _period_keys_for_range(request_start, request_end)
    departments = sorted({station_id[:2] for station_id in query.station_ids})
    targets: list[FrDailyDownloadTarget] = []
    for department in departments:
        for period_key in bucket_keys:
            targets.append(
                FrDailyDownloadTarget(
                    department=department,
                    period_key=period_key,
                    url=f'{spec.data_base_url}/Q_{department}_{period_key}_RR-T-Vent.csv.gz',
                )
            )
    return targets


def _download_target_table(target: FrDailyDownloadTarget, timeout: int) -> pd.DataFrame:
    response = requests.get(target.url, timeout=timeout)
    if response.status_code == 404:
        raise FileNotFoundError(target.url)
    response.raise_for_status()
    return parse_fr_daily_csv(decode_fr_daily_payload(response.content))


def _resolve_request_range(query: ObservationQuery) -> tuple[date, date]:
    if query.all_history:
        return date(1850, 1, 1), pd.Timestamp.today().date()
    return query.start_date, query.end_date


def _period_keys_for_range(request_start: date, request_end: date) -> list[str]:
    current_year = pd.Timestamp.today().year
    previous_end_year = current_year - 2
    keys: list[str] = []
    if request_start.year <= 1949:
        keys.append('avant-1949')
    if request_end.year >= 1950 and request_start.year <= previous_end_year:
        keys.append(f'previous-1950-{previous_end_year}')
    if request_end.year >= current_year - 1:
        keys.append(f'latest-{current_year - 1}-{current_year}')
    return keys
