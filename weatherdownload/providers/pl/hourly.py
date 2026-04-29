from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import date

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_pl
from .parser import (
    PL_NORMALIZED_SUBDAILY_COLUMNS,
    decode_pl_bytes,
    flag_with_missing,
    normalize_pl_gh_id,
    normalize_pl_observation_timestamp,
    normalize_pl_query_timestamp,
    parse_pl_hourly_synop_csv,
    station_lookup_by_gh_id,
    to_numeric_with_missing,
)
from .registry import PL_HOURLY_SYNOP_BASE_URL, get_dataset_spec


@dataclass(frozen=True)
class PlHourlyDownloadTarget:
    station_id: str
    archive_url: str



def download_hourly_observations_pl(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'historical' or query.resolution != '1hour':
        raise UnsupportedQueryError('The IMGW Poland hourly downloader supports only historical/1hour.')
    if not query.elements:
        raise UnsupportedQueryError('The IMGW Poland hourly downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_pl(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No IMGW Poland station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No IMGW Poland station metadata found for station_id: {', '.join(missing_station_ids)}")

    targets = build_pl_hourly_download_targets(query)
    parsed_tables: list[pd.DataFrame] = []
    for target in targets:
        try:
            table = _download_hourly_archive(target, timeout=timeout)
        except FileNotFoundError:
            continue
        if table.empty:
            continue
        table['_target_station_id'] = target.station_id
        parsed_tables.append(table)

    if not parsed_tables:
        raise StationNotFoundError(f"No IMGW Poland hourly data found for station_id: {', '.join(sorted(query.station_ids))}")

    normalized = normalize_hourly_observations_pl(
        pd.concat(parsed_tables, ignore_index=True),
        query,
        station_metadata=metadata_table,
    )
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, PL_NORMALIZED_SUBDAILY_COLUMNS]



def build_pl_hourly_download_targets(query: ObservationQuery) -> list[PlHourlyDownloadTarget]:
    spec = get_dataset_spec(query.provider, query.resolution)
    if not spec.implemented:
        raise UnsupportedQueryError('The requested IMGW Poland dataset path is not implemented.')

    request_start, request_end = _resolve_request_range(query)
    targets: list[PlHourlyDownloadTarget] = []
    seen: set[tuple[str, str]] = set()

    for station_id in query.station_ids:
        for year in range(request_start.year, request_end.year + 1):
            if year < 2001:
                bucket_start = year - ((year - 1996) % 5)
                bucket_end = bucket_start + 4
                archive_name = f'{bucket_start}_{bucket_end}_{int(station_id)}_s.zip'
                archive_url = f'{PL_HOURLY_SYNOP_BASE_URL}/{bucket_start}_{bucket_end}/{archive_name}'
            else:
                archive_name = f'{year}_{int(station_id)}_s.zip'
                archive_url = f'{PL_HOURLY_SYNOP_BASE_URL}/{year}/{archive_name}'
            key = (station_id, archive_url)
            if key in seen:
                continue
            seen.add(key)
            targets.append(PlHourlyDownloadTarget(station_id=station_id, archive_url=archive_url))
    return targets



def normalize_hourly_observations_pl(
    table: pd.DataFrame,
    query: ObservationQuery,
    station_metadata: pd.DataFrame,
) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=PL_NORMALIZED_SUBDAILY_COLUMNS)

    gh_lookup = station_lookup_by_gh_id(station_metadata)
    resolved_station_ids = table['NSP'].map(lambda value: gh_lookup.get(normalize_pl_gh_id(value), ''))
    fallback_station_ids = table['_target_station_id'].astype('string').fillna('')
    table = table.copy()
    table['_resolved_station_id'] = resolved_station_ids.where(resolved_station_ids.ne(''), fallback_station_ids)

    request_start = normalize_pl_query_timestamp(query.start)
    request_end = normalize_pl_query_timestamp(query.end)
    rows: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        if raw_code not in table.columns:
            continue
        flag_column_name = _flag_column_for_element(raw_code)
        flag_series = table[flag_column_name] if flag_column_name in table.columns else pd.Series(pd.NA, index=table.index, dtype='string')
        element_columns = canonicalize_element_series(pd.Series([raw_code] * len(table.index), index=table.index), query)
        normalized = pd.DataFrame(
            {
                'station_id': table['_resolved_station_id'].astype('string').str.strip(),
                'gh_id': table['NSP'].map(normalize_pl_gh_id).replace('', pd.NA),
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'timestamp': table.apply(normalize_pl_observation_timestamp, axis=1),
                'value': to_numeric_with_missing(table[raw_code]),
                'flag': flag_with_missing(flag_series),
                'quality': pd.Series(pd.NA, index=table.index, dtype='Int64'),
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=PL_NORMALIZED_SUBDAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[combined['timestamp'].notna()]
    combined = combined[(combined['timestamp'] >= request_start) & (combined['timestamp'] <= request_end)]
    combined = combined.sort_values(['station_id', 'timestamp', 'element']).reset_index(drop=True)
    return combined.loc[:, PL_NORMALIZED_SUBDAILY_COLUMNS]



def _resolve_request_range(query: ObservationQuery) -> tuple[pd.Timestamp, pd.Timestamp]:
    if query.all_history:
        return pd.Timestamp('1900-01-01T00:00:00Z'), pd.Timestamp.today(tz='UTC')
    return normalize_pl_query_timestamp(query.start), normalize_pl_query_timestamp(query.end)



def _download_hourly_archive(target: PlHourlyDownloadTarget, timeout: int) -> pd.DataFrame:
    response = requests.get(target.archive_url, timeout=timeout)
    if response.status_code == 404:
        raise FileNotFoundError(target.archive_url)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        product_names = [name for name in archive.namelist() if name.lower().endswith('.csv')]
        if not product_names:
            raise ValueError(f'No IMGW Poland hourly CSV found in archive for station_id {target.station_id}.')
        csv_text = decode_pl_bytes(archive.read(product_names[0]))
    return parse_pl_hourly_synop_csv(csv_text)



def _flag_column_for_element(raw_code: str) -> str:
    return {
        'TEMP': 'WTEMP',
        'FWR': 'WFWR',
        'PORW': 'WPORW',
        'WLGW': 'WWLGW',
        'CPW': 'WCPW',
        'PPPS': 'WPPPS',
    }.get(raw_code, '')
