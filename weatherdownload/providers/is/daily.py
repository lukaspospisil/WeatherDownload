from __future__ import annotations

from datetime import date

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from ..ghcnd.wrappers import build_daily_observation_downloader
from .metadata import read_station_metadata_vedur
from .parser import normalize_vedur_daily_rows, parse_vedur_payload_json
from .registry import get_dataset_spec

download_daily_observations_ghcnd = build_daily_observation_downloader(get_dataset_spec=get_dataset_spec)


def download_daily_observations_vedur(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'vedur' or query.resolution != 'daily':
        raise UnsupportedQueryError('The Iceland daily downloader supports only vedur/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The Vedur Iceland daily downloader requires at least one element.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_vedur(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No Vedur station metadata are available.')

    station_id_lookup = {
        str(station_id).strip(): str(station_id).strip()
        for station_id in metadata_table['station_id'].astype(str)
    }
    missing_station_ids = sorted(station_id for station_id in query.station_ids if station_id.strip() not in station_id_lookup)
    if missing_station_ids:
        raise StationNotFoundError(f"No Vedur station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query, metadata_table)
    spec = get_dataset_spec('vedur', 'daily')
    station_supported_raw = metadata_table.attrs.get('station_provider_raw_elements_by_path', {}).get((spec.provider, spec.resolution), {})

    frames: list[pd.DataFrame] = []
    for requested_station_id in query.station_ids:
        station_id = station_id_lookup[requested_station_id.strip()]
        station_raw_elements = station_supported_raw.get(station_id, [])
        resolved_raw_elements = _resolve_station_raw_elements(list(query.elements), station_raw_elements, spec)
        unsupported_requested = [
            requested_raw
            for requested_raw in query.elements
            if not _resolve_station_raw_elements([requested_raw], station_raw_elements, spec)
        ]
        if unsupported_requested:
            raise UnsupportedQueryError(
                f"Vedur daily station_id {station_id} does not support requested raw elements: {', '.join(unsupported_requested)}"
            )
        source_url = spec.aws_observation_url if _station_source_kind(station_raw_elements) == 'aws' else spec.synop_observation_url
        records = _download_station_records(
            source_url=source_url,
            station_id=station_id,
            start_date=request_start,
            end_date=request_end,
            timeout=timeout,
        )
        normalized = normalize_vedur_daily_rows(
            records,
            station_id=station_id,
            raw_elements=resolved_raw_elements,
            provider=query.provider,
            resolution=query.resolution,
        )
        if not normalized.empty:
            normalized = normalized[
                (normalized['observation_date'] >= request_start)
                & (normalized['observation_date'] <= request_end)
            ].copy()
        if normalized.empty:
            continue
        element_columns = canonicalize_element_series(pd.Series(normalized['element_raw']), query)
        normalized['element'] = element_columns['element']
        normalized['element_raw'] = element_columns['element_raw']
        frames.append(normalized)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['station_id', 'element', 'observation_date'], keep='last').reset_index(drop=True)
    return combined.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)


def _download_station_records(
    *,
    source_url: str,
    station_id: str,
    start_date: date,
    end_date: date,
    timeout: int,
) -> list[dict[str, object]]:
    response = requests.get(
        source_url,
        params={
            'station_id': station_id,
            'day_from': start_date.isoformat(),
            'day_to': end_date.isoformat(),
        },
        timeout=timeout,
    )
    if response.status_code == 404:
        return []
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise DownloadError(
            f'Failed to download Vedur daily observations for station_id {station_id}: {exc}'
        ) from exc
    response.encoding = 'utf-8'
    payload = parse_vedur_payload_json(response.text)
    if not isinstance(payload, list):
        raise DownloadError(f'Vedur daily observations for station_id {station_id} returned an unexpected payload shape.')
    return [record for record in payload if isinstance(record, dict)]


def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[date, date]:
    if not query.all_history:
        if query.start_date is None or query.end_date is None:
            raise UnsupportedQueryError('The Vedur Iceland daily downloader requires start_date and end_date.')
        return query.start_date, query.end_date
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    if pd.isna(begin):
        raise UnsupportedQueryError('Vedur all_history mode requires station coverage metadata.')
    return begin.date(), pd.Timestamp.utcnow().normalize().date()


def _resolve_station_raw_elements(query_raw_elements: list[str], station_raw_elements: list[str], spec: object) -> list[str]:
    station_supported = set(station_raw_elements)
    raw_to_canonical: dict[str, str] = {}
    for canonical_name, raw_codes in (getattr(spec, 'canonical_elements', None) or {}).items():
        for raw_code in raw_codes:
            raw_to_canonical[raw_code] = canonical_name

    resolved: list[str] = []
    for requested_raw in query_raw_elements:
        if requested_raw in station_supported:
            resolved.append(requested_raw)
            continue
        canonical_name = raw_to_canonical.get(requested_raw)
        alternatives = list((getattr(spec, 'canonical_elements', None) or {}).get(canonical_name, ()))
        replacement = next((raw_code for raw_code in alternatives if raw_code in station_supported), None)
        if replacement is not None:
            resolved.append(replacement)
    return list(dict.fromkeys(resolved))


def _station_source_kind(station_raw_elements: list[str]) -> str:
    if 'tx' in station_raw_elements or 'tn' in station_raw_elements or 'rsun' in station_raw_elements:
        return 'aws'
    return 'synop'
