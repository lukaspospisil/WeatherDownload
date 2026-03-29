from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass

import pandas as pd
import requests

from .elements import canonicalize_element_series
from .errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .hu_metadata import read_station_metadata_hu
from .hu_parser import (
    HU_NORMALIZED_SUBDAILY_COLUMNS,
    flag_with_missing,
    normalize_hu_observation_timestamp,
    normalize_hu_query_timestamp,
    parse_hu_directory_listing,
    parse_hu_subdaily_csv,
    quality_column_for_element,
    to_numeric_with_missing,
)
from .hu_registry import HU_HOURLY_HISTORICAL_URL, HU_HOURLY_RECENT_URL, get_dataset_spec
from .queries import ObservationQuery

_HU_HOURLY_ARCHIVE_PATTERN = re.compile(
    r'HABP_1H_(?P<station_id>\d+)_(?P<start>\d{8})_(?P<end>\d{8})_hist\.zip'
)
_HU_HOURLY_PRODUCT_PATTERN = re.compile(
    r'HABP_1H_\d{8}_\d{8}_(?P<station_id>\d+)\.csv'
)


@dataclass(frozen=True)
class HuHourlyDownloadTarget:
    station_id: str
    archive_url: str


def download_hourly_observations_hu(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != '1hour':
        raise UnsupportedQueryError('The HungaroMet Hungary hourly downloader only supports historical/1hour.')
    if not query.elements:
        raise UnsupportedQueryError('The HungaroMet Hungary hourly downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_hu(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No HungaroMet Hungary station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No HungaroMet Hungary station metadata found for station_id: {', '.join(missing_station_ids)}")

    targets = build_hu_hourly_download_targets(query, timeout=timeout)
    parsed_tables: list[pd.DataFrame] = []

    for target in targets:
        try:
            archive_bytes = _download_archive_bytes(target.archive_url, timeout=timeout)
        except FileNotFoundError:
            continue
        parsed_tables.append(_parse_hourly_archive(archive_bytes, target.station_id))

    if not parsed_tables:
        raise StationNotFoundError(f"No HungaroMet Hungary hourly data found for station_id: {', '.join(sorted(query.station_ids))}")

    normalized = normalize_hourly_observations_hu(pd.concat(parsed_tables, ignore_index=True), query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, HU_NORMALIZED_SUBDAILY_COLUMNS]


def build_hu_hourly_download_targets(query: ObservationQuery, timeout: int = 60) -> list[HuHourlyDownloadTarget]:
    spec = get_dataset_spec(query.dataset_scope, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested HungaroMet Hungary dataset path is not implemented.')

    request_start, request_end = _resolve_request_range(query)
    listing_html = _fetch_historical_directory_listing(timeout=timeout)
    historical_entries = parse_hu_directory_listing(listing_html, _HU_HOURLY_ARCHIVE_PATTERN)

    targets: list[HuHourlyDownloadTarget] = []
    for station_id in query.station_ids:
        for entry in historical_entries:
            if entry['station_id'] != station_id:
                continue
            archive_start = pd.to_datetime(entry['start'], format='%Y%m%d', errors='coerce', utc=True)
            archive_end = pd.to_datetime(entry['end'], format='%Y%m%d', errors='coerce', utc=True)
            if pd.isna(archive_start) or pd.isna(archive_end):
                continue
            archive_end = archive_end + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
            if archive_end < request_start or archive_start > request_end:
                continue
            archive_name = f"HABP_1H_{station_id}_{entry['start']}_{entry['end']}_hist.zip"
            targets.append(HuHourlyDownloadTarget(station_id=station_id, archive_url=HU_HOURLY_HISTORICAL_URL + archive_name))
        if request_end >= pd.Timestamp(year=pd.Timestamp.today(tz='UTC').year, month=1, day=1, tz='UTC'):
            targets.append(
                HuHourlyDownloadTarget(
                    station_id=station_id,
                    archive_url=f'{HU_HOURLY_RECENT_URL}HABP_1H_{station_id}_akt.zip',
                )
            )
    return targets


def normalize_hourly_observations_hu(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    return _normalize_hu_subdaily_observations(table, query)


def _normalize_hu_subdaily_observations(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=HU_NORMALIZED_SUBDAILY_COLUMNS)

    request_start = normalize_hu_query_timestamp(query.start)
    request_end = normalize_hu_query_timestamp(query.end)
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
                'timestamp': table['Time'].map(normalize_hu_observation_timestamp),
                'value': to_numeric_with_missing(table[raw_code]),
                'flag': flag_with_missing(table[quality_column]) if quality_column in table.columns else pd.Series(pd.NA, index=table.index, dtype='string'),
                'quality': pd.Series(pd.NA, index=table.index, dtype='Int64'),
                'dataset_scope': query.dataset_scope,
                'resolution': query.resolution,
            }
        )
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=HU_NORMALIZED_SUBDAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[combined['timestamp'].notna()]
    combined = combined[(combined['timestamp'] >= request_start) & (combined['timestamp'] <= request_end)]
    combined = combined.sort_values(['station_id', 'timestamp', 'element']).reset_index(drop=True)
    return combined.loc[:, HU_NORMALIZED_SUBDAILY_COLUMNS]


def _resolve_request_range(query: ObservationQuery) -> tuple[pd.Timestamp, pd.Timestamp]:
    if query.all_history:
        return pd.Timestamp('1900-01-01T00:00:00Z'), pd.Timestamp(year=pd.Timestamp.today(tz='UTC').year, month=12, day=31, hour=23, minute=59, tz='UTC')
    return normalize_hu_query_timestamp(query.start), normalize_hu_query_timestamp(query.end)


def _fetch_historical_directory_listing(timeout: int) -> str:
    response = requests.get(HU_HOURLY_HISTORICAL_URL, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _download_archive_bytes(archive_url: str, timeout: int) -> bytes:
    response = requests.get(archive_url, timeout=timeout)
    if response.status_code == 404:
        raise FileNotFoundError(archive_url)
    response.raise_for_status()
    return response.content


def _parse_hourly_archive(archive_bytes: bytes, station_id: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        product_names = [
            name for name in archive.namelist()
            if _HU_HOURLY_PRODUCT_PATTERN.fullmatch(name.split('/')[-1])
        ]
        if not product_names:
            raise ValueError(f'No HungaroMet hourly product file found in archive for station_id {station_id}.')
        csv_text = archive.read(product_names[0]).decode('utf-8')
    return parse_hu_subdaily_csv(csv_text)
