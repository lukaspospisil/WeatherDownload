from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import pandas as pd
import requests

from ...errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .eccc_parser import (
    CA_ECCC_NORMALIZED_HOURLY_COLUMNS,
    normalize_ca_eccc_hourly_observations,
    parse_ca_eccc_hourly_feature_collection,
    read_text_from_source,
)
from .registry import get_dataset_spec

_ECCC_HOURLY_PROPERTIES = 'CLIMATE_IDENTIFIER,UTC_DATE,LOCAL_DATE,TEMP,TEMP_FLAG,RELATIVE_HUMIDITY,RELATIVE_HUMIDITY_FLAG'
_ECCC_PAGE_LIMIT = 5000
_ECCC_CHUNK_DAYS = 7
_ECCC_LOCAL_WINDOW_HOURS = 14


def download_hourly_observations_eccc(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'eccc' or query.resolution != '1hour':
        raise UnsupportedQueryError("The ECCC hourly downloader supports only provider='eccc' and resolution='1hour'.")
    if not query.elements:
        raise UnsupportedQueryError('The ECCC hourly downloader requires at least one element.')

    metadata_table = station_metadata
    if metadata_table is not None and not metadata_table.empty:
        available_station_ids = set(metadata_table['station_id'].astype(str))
        missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
        if missing_station_ids:
            raise StationNotFoundError(f"No ECCC station metadata found for station_id: {', '.join(missing_station_ids)}")

    source = _resolve_local_fixture_source(metadata_table)
    if source is not None:
        payload_text = read_text_from_source(source, timeout, requests)
        parsed = parse_ca_eccc_hourly_feature_collection(payload_text)
        normalized = normalize_ca_eccc_hourly_observations(
            parsed,
            station_ids=query.station_ids,
            raw_elements=query.elements,
            start=query.start,
            end=query.end,
            provider=query.provider,
            resolution=query.resolution,
        )
        if normalized.empty:
            raise EmptyResultError('No observations found for the given query.')
        return normalized.loc[:, CA_ECCC_NORMALIZED_HOURLY_COLUMNS]

    if query.start is None or query.end is None:
        raise UnsupportedQueryError('The ECCC hourly downloader requires start and end timestamps.')

    request_start = _normalize_query_timestamp(query.start)
    request_end = _normalize_query_timestamp(query.end)
    spec = get_dataset_spec(query.provider, query.resolution)
    base_url = _strip_query_params(spec.daily_data_url)

    normalized_frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        for chunk_start, chunk_end in _iter_day_chunks(request_start, request_end, days=_ECCC_CHUNK_DAYS):
            params = _build_eccc_hourly_params(
                station_id=station_id,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                limit=_ECCC_PAGE_LIMIT,
            )
            for page_text in _download_eccc_pages_stop_on_empty(url=base_url, params=params, timeout=timeout):
                parsed = parse_ca_eccc_hourly_feature_collection(page_text)
                normalized = normalize_ca_eccc_hourly_observations(
                    parsed,
                    station_ids=[station_id],
                    raw_elements=query.elements,
                    start=query.start,
                    end=query.end,
                    provider=query.provider,
                    resolution=query.resolution,
                )
                if not normalized.empty:
                    normalized_frames.append(normalized)

    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(normalized_frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'timestamp', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, CA_ECCC_NORMALIZED_HOURLY_COLUMNS]


def _resolve_local_fixture_source(station_metadata: pd.DataFrame | None) -> str | None:
    if station_metadata is None:
        return None
    source = station_metadata.attrs.get('source_url')
    if not isinstance(source, str):
        return None
    return source if Path(source).exists() else None


def _normalize_query_timestamp(value: str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return ts


def _iter_day_chunks(start: pd.Timestamp, end: pd.Timestamp, *, days: int) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    chunks: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    current = start
    step = timedelta(days=days)
    while current <= end:
        chunk_end = min(end, current + step - timedelta(seconds=1))
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(seconds=1)
    return chunks


def _strip_query_params(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, '', ''))


def _build_eccc_hourly_params(
    *,
    station_id: str,
    chunk_start: pd.Timestamp,
    chunk_end: pd.Timestamp,
    limit: int,
) -> dict[str, str]:
    # GeoMet climate-hourly appears to apply the OGC API `datetime` filter to LOCAL_DATE semantics,
    # while we normalize timestamps using UTC_DATE. To reliably retrieve UTC_DATE rows for a UTC query
    # without station timezone metadata, request a conservative local-time window and filter by UTC_DATE
    # client-side in normalization.
    request_start = chunk_start - pd.Timedelta(hours=_ECCC_LOCAL_WINDOW_HOURS)
    request_end = chunk_end + pd.Timedelta(hours=_ECCC_LOCAL_WINDOW_HOURS)
    start_text = request_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_text = request_end.strftime('%Y-%m-%dT%H:%M:%SZ')
    return {
        'f': 'json',
        'CLIMATE_IDENTIFIER': str(station_id).strip(),
        'datetime': f'{start_text}/{end_text}',
        'properties': _ECCC_HOURLY_PROPERTIES,
        'limit': str(limit),
    }


def _extract_next_href(payload: dict[str, object], request_url: str) -> str | None:
    links = payload.get('links')
    if not isinstance(links, list):
        return None
    for link in links:
        if not isinstance(link, dict):
            continue
        if link.get('rel') != 'next':
            continue
        href = link.get('href')
        if isinstance(href, str) and href.strip():
            return urljoin(request_url, href.strip())
    return None


def _download_eccc_pages_stop_on_empty(*, url: str, params: dict[str, str], timeout: int) -> list[str]:
    pages: list[str] = []
    next_url: str | None = None
    seen_next: set[str] = set()

    for _ in range(1000):
        if next_url is None:
            response = requests.get(url, params=params, timeout=timeout)
        else:
            response = requests.get(next_url, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'

        text = response.text
        pages.append(text)
        try:
            payload = json.loads(text.lstrip('\ufeff'))
        except json.JSONDecodeError as exc:
            raise DownloadError('ECCC GeoMet response is not valid JSON.') from exc
        if not isinstance(payload, dict):
            break
        features = payload.get('features')
        if isinstance(features, list) and len(features) == 0:
            break

        next_candidate = _extract_next_href(payload, next_url or url)
        if not next_candidate or next_candidate in seen_next:
            break
        seen_next.add(next_candidate)
        next_url = next_candidate

    return pages
