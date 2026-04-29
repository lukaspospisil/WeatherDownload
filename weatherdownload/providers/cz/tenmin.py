from __future__ import annotations

import io
import re
from dataclasses import dataclass

import pandas as pd
import requests

from .registry import get_dataset_spec
from ...elements import canonicalize_element_series
from ...errors import DownloadError, UnsupportedQueryError
from ...queries import ObservationQuery

RAW_TENMIN_COLUMNS = ['STATION', 'ELEMENT', 'DT', 'VALUE', 'FLAG', 'QUALITY']
NORMALIZED_TENMIN_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'provider', 'resolution'
]
_YEAR_LINK_PATTERN = re.compile(r'href=["\'](?P<year>\d{4})/["\']')


@dataclass(slots=True)
class TenMinDownloadTarget:
    station_id: str
    element: str
    group: str
    year: str
    year_month: str
    url: str


def build_tenmin_download_targets(query: ObservationQuery, timeout: int = 60) -> list[TenMinDownloadTarget]:
    spec = get_dataset_spec(query.provider, query.resolution)
    if spec.time_semantics != 'datetime':
        raise UnsupportedQueryError(
            f"Unsupported time semantics for 10min downloader: {spec.time_semantics}"
        )
    if spec.station_identifier_type != 'wsi':
        raise UnsupportedQueryError(
            f"Unsupported station identifier type for 10min downloader: {spec.station_identifier_type}"
        )
    if spec.element_groups is None or spec.endpoint_pattern is None:
        raise UnsupportedQueryError('The registered 10min dataset spec is missing endpoint metadata.')

    if query.all_history:
        return _build_all_history_tenmin_download_targets(query, spec, timeout=timeout)
    if query.start is None or query.end is None:
        raise UnsupportedQueryError('The 10min historical_csv downloader requires start and end unless all_history=True is set.')

    start = pd.Timestamp(query.start)
    end = pd.Timestamp(query.end)
    month_starts = pd.period_range(start=start, end=end, freq='M')

    targets: list[TenMinDownloadTarget] = []
    for station_id in query.station_ids:
        for element in query.elements or []:
            group = spec.element_groups.get(element)
            if group is None:
                raise UnsupportedQueryError(f"Unsupported 10min historical_csv element: {element}")
            for month in month_starts:
                year = f'{month.year:04d}'
                year_month = f'{month.year:04d}{month.month:02d}'
                url = spec.endpoint_pattern.format(group=group, year=year, station_id=station_id, element=element, year_month=year_month)
                targets.append(TenMinDownloadTarget(station_id=station_id, element=element, group=group, year=year, year_month=year_month, url=url))
    return targets


def download_tenmin_csv(target: TenMinDownloadTarget, timeout: int = 60) -> str:
    try:
        response = requests.get(target.url, timeout=timeout)
    except requests.RequestException as exc:
        raise DownloadError(f"Failed to download {target.url}") from exc
    if response.status_code == 404:
        raise FileNotFoundError(target.url)
    try:
        response.raise_for_status()
    except Exception as exc:
        raise DownloadError(f"Failed to download {target.url}") from exc
    response.encoding = 'utf-8'
    return response.text


def parse_tenmin_csv(csv_text: str) -> pd.DataFrame:
    table = pd.read_csv(io.StringIO(csv_text))
    missing_columns = [column for column in RAW_TENMIN_COLUMNS if column not in table.columns]
    if missing_columns:
        raise ValueError(f"Downloaded file is missing expected columns: {missing_columns}")
    return table.loc[:, RAW_TENMIN_COLUMNS]


def normalize_tenmin_observations(table: pd.DataFrame, query: ObservationQuery, station_metadata: pd.DataFrame | None = None) -> pd.DataFrame:
    normalized = table.rename(columns={
        'STATION': 'station_id',
        'DT': 'timestamp',
        'VALUE': 'value',
        'FLAG': 'flag',
        'QUALITY': 'quality',
    }).copy()
    element_columns = canonicalize_element_series(table['ELEMENT'], query)
    normalized['element'] = element_columns['element']
    normalized['element_raw'] = element_columns['element_raw']
    normalized['timestamp'] = pd.to_datetime(normalized['timestamp'], utc=True)
    normalized['value'] = pd.to_numeric(normalized['value'], errors='coerce')
    normalized['quality'] = pd.to_numeric(normalized['quality'], errors='coerce').astype('Int64')
    normalized['flag'] = normalized['flag'].astype('string').replace({'': pd.NA})
    normalized['provider'] = query.provider
    normalized['resolution'] = query.resolution
    if station_metadata is not None and not station_metadata.empty:
        mapping = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
        normalized = normalized.merge(mapping, on='station_id', how='left')
    else:
        normalized['gh_id'] = pd.NA
    if query.start is not None:
        normalized = normalized[normalized['timestamp'] >= pd.Timestamp(query.start)]
    if query.end is not None:
        normalized = normalized[normalized['timestamp'] <= pd.Timestamp(query.end)]
    return normalized.loc[:, NORMALIZED_TENMIN_COLUMNS].reset_index(drop=True)


def _build_all_history_tenmin_download_targets(query: ObservationQuery, spec, timeout: int) -> list[TenMinDownloadTarget]:
    targets: list[TenMinDownloadTarget] = []
    year_cache: dict[str, list[str]] = {}
    file_cache: dict[tuple[str, str], list[str]] = {}
    for station_id in query.station_ids:
        for element in query.elements or []:
            group = spec.element_groups.get(element)
            if group is None:
                raise UnsupportedQueryError(f"Unsupported 10min historical_csv element: {element}")
            if group not in year_cache:
                year_cache[group] = _fetch_available_years(_group_directory_url(spec.endpoint_pattern, group), timeout=timeout)
            for year in year_cache[group]:
                cache_key = (group, year)
                if cache_key not in file_cache:
                    file_cache[cache_key] = _fetch_directory_filenames(_year_directory_url(spec.endpoint_pattern, group, year), timeout=timeout)
                pattern = re.compile(rf'10m-{re.escape(station_id)}-{re.escape(element)}-(?P<year_month>\d{{6}})\.csv')
                for filename in file_cache[cache_key]:
                    match = pattern.fullmatch(filename)
                    if match is None:
                        continue
                    year_month = match.group('year_month')
                    url = _year_directory_url(spec.endpoint_pattern, group, year) + filename
                    targets.append(TenMinDownloadTarget(station_id=station_id, element=element, group=group, year=year, year_month=year_month, url=url))
    return targets


def _group_directory_url(endpoint_pattern: str, group: str) -> str:
    return endpoint_pattern.split('{year}/', maxsplit=1)[0].format(group=group)


def _year_directory_url(endpoint_pattern: str, group: str, year: str) -> str:
    return _group_directory_url(endpoint_pattern, group) + f'{year}/'


def _fetch_available_years(directory_url: str, timeout: int) -> list[str]:
    try:
        response = requests.get(directory_url, timeout=timeout)
    except requests.RequestException as exc:
        raise DownloadError(f'Failed to list {directory_url}') from exc
    try:
        response.raise_for_status()
    except Exception as exc:
        raise DownloadError(f'Failed to list {directory_url}') from exc
    years = sorted({match.group('year') for match in _YEAR_LINK_PATTERN.finditer(response.text)})
    return years


def _fetch_directory_filenames(directory_url: str, timeout: int) -> list[str]:
    try:
        response = requests.get(directory_url, timeout=timeout)
    except requests.RequestException as exc:
        raise DownloadError(f'Failed to list {directory_url}') from exc
    try:
        response.raise_for_status()
    except Exception as exc:
        raise DownloadError(f'Failed to list {directory_url}') from exc
    return sorted(set(re.findall(r'href=["\']([^"\']+\.csv)["\']', response.text)))

