from __future__ import annotations

import calendar
import json
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import pandas as pd
import requests

from ...errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .eccc_parser import (
    CA_ECCC_NORMALIZED_DAILY_COLUMNS,
    normalize_ca_eccc_daily_observations,
    parse_ca_eccc_daily_feature_collection,
    read_text_from_source,
)
from ..ghcnd.wrappers import build_daily_observation_downloader, build_station_dly_url_builder
from .registry import get_dataset_spec

download_daily_observations_ghcnd = build_daily_observation_downloader(get_dataset_spec=get_dataset_spec)
build_station_dly_url = build_station_dly_url_builder(get_dataset_spec=get_dataset_spec)

_ECCC_DAILY_PROPERTIES = (
    'CLIMATE_IDENTIFIER,LOCAL_DATE,MEAN_TEMPERATURE,MAX_TEMPERATURE,MIN_TEMPERATURE,TOTAL_PRECIPITATION'
)
_ECCC_PAGE_LIMIT = 5000


def download_daily_observations_eccc(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'eccc' or query.resolution != 'daily':
        raise UnsupportedQueryError("The ECCC daily downloader supports only provider='eccc' and resolution='daily'.")
    if not query.elements:
        raise UnsupportedQueryError('The ECCC daily downloader requires at least one element.')

    metadata_table = station_metadata
    if metadata_table is not None and not metadata_table.empty:
        available_station_ids = set(metadata_table['station_id'].astype(str))
        missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
        if missing_station_ids:
            raise StationNotFoundError(f"No ECCC station metadata found for station_id: {', '.join(missing_station_ids)}")

    source = _resolve_local_fixture_source(metadata_table)
    if source is None:
        request_start, request_end = _resolve_request_range(query, metadata_table)
        spec = get_dataset_spec(query.provider, query.resolution)
        base_url = _strip_query_params(spec.daily_data_url)

        normalized_frames: list[pd.DataFrame] = []
        for station_id in query.station_ids:
            for chunk_start, chunk_end in _iter_month_chunks(request_start, request_end):
                params = _build_eccc_daily_params(
                    station_id=station_id,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    limit=_ECCC_PAGE_LIMIT,
                )
                for page_text in _download_eccc_pages(url=base_url, params=params, timeout=timeout):
                    parsed = parse_ca_eccc_daily_feature_collection(page_text)
                    normalized = normalize_ca_eccc_daily_observations(
                        parsed,
                        station_ids=[station_id],
                        raw_elements=query.elements,
                        start_date=request_start,
                        end_date=request_end,
                        provider=query.provider,
                        resolution=query.resolution,
                    )
                    if not normalized.empty:
                        normalized_frames.append(normalized)

        if not normalized_frames:
            raise EmptyResultError('No observations found for the given query.')
        combined = pd.concat(normalized_frames, ignore_index=True)
        combined = combined.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)
        return combined.loc[:, CA_ECCC_NORMALIZED_DAILY_COLUMNS]

    payload_text = read_text_from_source(source, timeout, requests)
    parsed = parse_ca_eccc_daily_feature_collection(payload_text)
    normalized = normalize_ca_eccc_daily_observations(
        parsed,
        station_ids=query.station_ids,
        raw_elements=query.elements,
        start_date=query.start_date,
        end_date=query.end_date,
        provider=query.provider,
        resolution=query.resolution,
    )
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, CA_ECCC_NORMALIZED_DAILY_COLUMNS]


def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame | None) -> tuple[date, date]:
    if not query.all_history:
        if query.start_date is None or query.end_date is None:
            raise UnsupportedQueryError('The ECCC daily downloader requires start_date and end_date unless all_history=True is set.')
        return query.start_date, query.end_date

    if station_metadata is None or station_metadata.empty:
        raise UnsupportedQueryError('ECCC all_history mode requires station coverage metadata.')

    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected.get('begin_date'), utc=True, errors='coerce').min()
    end = pd.to_datetime(selected.get('end_date'), utc=True, errors='coerce').max()
    if pd.isna(begin) or pd.isna(end):
        raise UnsupportedQueryError('ECCC all_history mode requires station coverage metadata.')
    return begin.date(), end.date()


def _iter_month_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        month_last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, month_last_day)
        chunk_end = min(end, month_end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _strip_query_params(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, '', ''))


def _build_eccc_daily_params(*, station_id: str, chunk_start: date, chunk_end: date, limit: int) -> dict[str, str]:
    return {
        'f': 'json',
        'CLIMATE_IDENTIFIER': str(station_id).strip(),
        'datetime': f'{chunk_start.isoformat()}/{chunk_end.isoformat()}',
        'properties': _ECCC_DAILY_PROPERTIES,
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


def _download_eccc_pages(*, url: str, params: dict[str, str], timeout: int) -> list[str]:
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
        pages.append(response.text)

        try:
            payload = json.loads(response.text.lstrip('\ufeff'))
        except json.JSONDecodeError as exc:
            raise DownloadError('ECCC GeoMet response is not valid JSON.') from exc
        if not isinstance(payload, dict):
            break

        next_candidate = _extract_next_href(payload, next_url or url)
        if not next_candidate or next_candidate in seen_next:
            break
        seen_next.add(next_candidate)
        next_url = next_candidate

    return pages


def _resolve_local_fixture_source(station_metadata: pd.DataFrame | None) -> str | None:
    if station_metadata is None:
        return None
    source = station_metadata.attrs.get('source_url')
    if not isinstance(source, str):
        return None
    return source if Path(source).exists() else None
