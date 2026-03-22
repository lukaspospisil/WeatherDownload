from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass

import pandas as pd
import requests

from .dwd_registry import get_dataset_spec
from .errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .queries import ObservationQuery

NORMALIZED_DWD_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
DWD_DAILY_DIRECTORY_URL = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/'
_DWD_DAILY_ARCHIVE_PATTERN = re.compile(r'tageswerte_KL_(?P<station_id>\d{5})_\d{8}_\d{8}_hist\.zip')
_DWD_DAILY_PRODUCT_PATTERN = re.compile(r'produkt_klima_tag_\d{8}_\d{8}_(?P<station_id>\d{5})\.txt')
_DWD_DAILY_MISSING_SENTINELS = {'-999', '-999.0', '-9999', '-9999.0'}
_DWD_DAILY_QN3_ELEMENTS = {'FX', 'FM'}


@dataclass(frozen=True)
class DwdDailyDownloadTarget:
    station_id: str
    archive_url: str


def download_daily_observations_dwd(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != 'daily':
        raise UnsupportedQueryError('The DWD daily downloader only supports historical/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The DWD daily downloader requires at least one element.')

    archive_urls = _fetch_daily_archive_urls(timeout=timeout)
    targets = _build_daily_download_targets(query, archive_urls)
    if not targets:
        station_list = ', '.join(sorted(query.station_ids))
        raise StationNotFoundError(f'No DWD daily historical data found for station_id: {station_list}')

    parsed_tables: list[pd.DataFrame] = []
    found_station_ids: set[str] = set()
    for target in targets:
        archive_bytes = _download_archive_bytes(target.archive_url, timeout=timeout)
        parsed_tables.append(_parse_daily_archive(archive_bytes, target.station_id))
        found_station_ids.add(target.station_id)

    missing_station_ids = sorted(set(query.station_ids) - found_station_ids)
    if not parsed_tables:
        if missing_station_ids:
            raise StationNotFoundError(f"No DWD daily historical data found for station_id: {', '.join(missing_station_ids)}")
        raise EmptyResultError('No observations found for the given query.')

    merged = pd.concat(parsed_tables, ignore_index=True)
    normalized = normalize_daily_observations_dwd(merged, query, station_metadata=station_metadata)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, NORMALIZED_DWD_DAILY_COLUMNS]


def _build_daily_download_targets(query: ObservationQuery, archive_urls: dict[str, str]) -> list[DwdDailyDownloadTarget]:
    spec = get_dataset_spec(query.dataset_scope, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested DWD dataset path is not implemented.')
    targets: list[DwdDailyDownloadTarget] = []
    for station_id in query.station_ids:
        archive_url = archive_urls.get(station_id)
        if archive_url is None:
            continue
        targets.append(DwdDailyDownloadTarget(station_id=station_id, archive_url=archive_url))
    return targets


def _fetch_daily_archive_urls(timeout: int) -> dict[str, str]:
    response = requests.get(DWD_DAILY_DIRECTORY_URL, timeout=timeout)
    response.raise_for_status()
    html = response.text
    archive_urls: dict[str, str] = {}
    for match in _DWD_DAILY_ARCHIVE_PATTERN.finditer(html):
        archive_name = match.group(0)
        archive_urls[match.group('station_id')] = DWD_DAILY_DIRECTORY_URL + archive_name
    return archive_urls


def _download_archive_bytes(archive_url: str, timeout: int) -> bytes:
    response = requests.get(archive_url, timeout=timeout)
    response.raise_for_status()
    return response.content


def _parse_daily_archive(archive_bytes: bytes, station_id: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        product_names = [
            name for name in archive.namelist()
            if _DWD_DAILY_PRODUCT_PATTERN.fullmatch(name.split('/')[-1])
        ]
        if not product_names:
            raise ValueError(f'No DWD daily product file found in archive for station_id {station_id}.')
        product_name = product_names[0]
        csv_text = archive.read(product_name).decode('latin-1')
    table = pd.read_csv(io.StringIO(csv_text), sep=';', dtype=str)
    table.columns = [column.strip() for column in table.columns]
    return table


def normalize_daily_observations_dwd(
    table: pd.DataFrame,
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    metadata_lookup = _build_gh_id_lookup(station_metadata)
    for element in query.elements or []:
        if element not in table.columns:
            continue
        quality_column = 'QN_3' if element in _DWD_DAILY_QN3_ELEMENTS else 'QN_4'
        normalized = pd.DataFrame(
            {
                'station_id': table['STATIONS_ID'].astype(str).str.strip().str.zfill(5),
                'element': element,
                'observation_date': pd.to_datetime(table['MESS_DATUM'].astype(str).str.strip(), format='%Y%m%d').dt.date,
                'time_function': pd.NA,
                'value': _to_numeric_with_missing(table[element]),
                'flag': pd.NA,
                'quality': _to_quality_with_missing(table[quality_column]) if quality_column in table.columns else pd.Series(pd.NA, index=table.index),
                'dataset_scope': query.dataset_scope,
                'resolution': query.resolution,
            }
        )
        if metadata_lookup is not None:
            normalized = normalized.merge(metadata_lookup, on='station_id', how='left')
        else:
            normalized['gh_id'] = pd.NA
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=NORMALIZED_DWD_DAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    if query.start_date is not None:
        combined = combined[combined['observation_date'] >= query.start_date]
    if query.end_date is not None:
        combined = combined[combined['observation_date'] <= query.end_date]
    return combined.loc[:, NORMALIZED_DWD_DAILY_COLUMNS].reset_index(drop=True)


def _build_gh_id_lookup(station_metadata: pd.DataFrame | None) -> pd.DataFrame | None:
    if station_metadata is None or station_metadata.empty or 'gh_id' not in station_metadata.columns:
        return None
    return station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])


def _to_numeric_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.where(~cleaned.isin(_DWD_DAILY_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned, errors='coerce')


def _to_quality_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.where(~cleaned.isin(_DWD_DAILY_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned, errors='coerce').astype('Int64')
