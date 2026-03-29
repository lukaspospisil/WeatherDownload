from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from .elements import canonicalize_element_series
from .errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .hu_metadata import read_station_metadata_hu
from .hu_parser import (
    HU_NORMALIZED_DAILY_COLUMNS,
    flag_with_missing,
    normalize_hu_observation_date,
    parse_hu_daily_csv,
    parse_hu_directory_listing,
    quality_column_for_element,
    to_numeric_with_missing,
)
from .hu_registry import HU_DAILY_HISTORICAL_URL, HU_DAILY_RECENT_URL, get_dataset_spec
from .queries import ObservationQuery

_HU_HISTORICAL_ARCHIVE_PATTERN = re.compile(
    r'HABP_1D_(?P<station_id>\d+)_(?P<start>\d{8})_(?P<end>\d{8})_hist\.zip'
)
_HU_DAILY_PRODUCT_PATTERN = re.compile(
    r'HABP_1D_\d{8}_\d{8}_(?P<station_id>\d+)\.csv'
)


@dataclass(frozen=True)
class HuDailyDownloadTarget:
    station_id: str
    archive_url: str


def download_daily_observations_hu(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != 'daily':
        raise UnsupportedQueryError('The HungaroMet Hungary daily downloader only supports historical/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The HungaroMet Hungary daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_hu(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No HungaroMet Hungary station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No HungaroMet Hungary station metadata found for station_id: {', '.join(missing_station_ids)}")

    targets = build_hu_daily_download_targets(query, timeout=timeout)
    parsed_tables: list[pd.DataFrame] = []
    found_station_ids: set[str] = set()

    for target in targets:
        try:
            archive_bytes = _download_archive_bytes(target.archive_url, timeout=timeout)
        except FileNotFoundError:
            continue
        parsed_tables.append(_parse_daily_archive(archive_bytes, target.station_id))
        found_station_ids.add(target.station_id)

    if not parsed_tables:
        raise StationNotFoundError(f"No HungaroMet Hungary daily data found for station_id: {', '.join(sorted(query.station_ids))}")

    normalized = normalize_daily_observations_hu(pd.concat(parsed_tables, ignore_index=True), query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, HU_NORMALIZED_DAILY_COLUMNS]


def build_hu_daily_download_targets(query: ObservationQuery, timeout: int = 60) -> list[HuDailyDownloadTarget]:
    spec = get_dataset_spec(query.dataset_scope, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested HungaroMet Hungary dataset path is not implemented.')

    request_start, request_end = _resolve_request_range(query)
    listing_html = _fetch_historical_directory_listing(timeout=timeout)
    historical_entries = parse_hu_directory_listing(listing_html, _HU_HISTORICAL_ARCHIVE_PATTERN)

    targets: list[HuDailyDownloadTarget] = []
    for station_id in query.station_ids:
        for entry in historical_entries:
            if entry['station_id'] != station_id:
                continue
            archive_start = pd.to_datetime(entry['start'], format='%Y%m%d', errors='coerce')
            archive_end = pd.to_datetime(entry['end'], format='%Y%m%d', errors='coerce')
            if pd.isna(archive_start) or pd.isna(archive_end):
                continue
            if archive_end.date() < request_start or archive_start.date() > request_end:
                continue
            archive_name = f"HABP_1D_{station_id}_{entry['start']}_{entry['end']}_hist.zip"
            targets.append(HuDailyDownloadTarget(station_id=station_id, archive_url=HU_DAILY_HISTORICAL_URL + archive_name))
        if request_end >= date(pd.Timestamp.today().year, 1, 1):
            targets.append(
                HuDailyDownloadTarget(
                    station_id=station_id,
                    archive_url=f'{HU_DAILY_RECENT_URL}HABP_1D_{station_id}_akt.zip',
                )
            )
    return targets


def normalize_daily_observations_hu(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=HU_NORMALIZED_DAILY_COLUMNS)

    rows: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        if raw_code not in table.columns:
            continue
        quality_column = quality_column_for_element(raw_code)
        element_columns = canonicalize_element_series(pd.Series([raw_code] * len(table.index), index=table.index), query)
        normalized = pd.DataFrame(
            {
                'station_id': table['StationNumber'].astype('string').str.strip(),
                'gh_id': pd.NA,
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'observation_date': table['Time'].map(normalize_hu_observation_date),
                'time_function': pd.NA,
                'value': to_numeric_with_missing(table[raw_code]),
                'flag': flag_with_missing(table[quality_column]) if quality_column in table.columns else pd.Series(pd.NA, index=table.index, dtype='string'),
                'quality': pd.Series(pd.NA, index=table.index, dtype='Int64'),
                'dataset_scope': query.dataset_scope,
                'resolution': query.resolution,
            }
        )
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=HU_NORMALIZED_DAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[combined['observation_date'].notna()]
    if query.start_date is not None:
        combined = combined[combined['observation_date'] >= query.start_date]
    if query.end_date is not None:
        combined = combined[combined['observation_date'] <= query.end_date]
    combined = combined.sort_values(['station_id', 'observation_date', 'element']).reset_index(drop=True)
    return combined.loc[:, HU_NORMALIZED_DAILY_COLUMNS]


def _resolve_request_range(query: ObservationQuery) -> tuple[date, date]:
    if query.all_history:
        return date(1900, 1, 1), date(pd.Timestamp.today().year, 12, 31)
    return query.start_date, query.end_date


def _fetch_historical_directory_listing(timeout: int) -> str:
    response = requests.get(HU_DAILY_HISTORICAL_URL, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _download_archive_bytes(archive_url: str, timeout: int) -> bytes:
    response = requests.get(archive_url, timeout=timeout)
    if response.status_code == 404:
        raise FileNotFoundError(archive_url)
    response.raise_for_status()
    return response.content


def _parse_daily_archive(archive_bytes: bytes, station_id: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        product_names = [
            name for name in archive.namelist()
            if _HU_DAILY_PRODUCT_PATTERN.fullmatch(name.split('/')[-1])
        ]
        if not product_names:
            raise ValueError(f'No HungaroMet daily product file found in archive for station_id {station_id}.')
        csv_text = archive.read(product_names[0]).decode('utf-8')
    return parse_hu_daily_csv(csv_text)
