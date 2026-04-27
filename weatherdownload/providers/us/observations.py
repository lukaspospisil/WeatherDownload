from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .parser import GHCND_NORMALIZED_DAILY_COLUMNS, normalize_daily_observations_ghcnd, parse_ghcnd_dly_text
from .registry import GhcndDatasetSpec, get_dataset_spec


def download_daily_observations_ghcnd(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'ghcnd' or query.resolution != 'daily':
        raise UnsupportedQueryError('NOAA GHCN-Daily support currently implements only ghcnd/daily observations.')
    if not query.elements:
        raise UnsupportedQueryError('NOAA GHCN-Daily daily downloader requires at least one element.')

    spec = get_dataset_spec('ghcnd', 'daily')
    tables: list[pd.DataFrame] = []
    missing_station_ids: list[str] = []
    for station_id in query.station_ids:
        source = build_station_dly_url(station_id, spec=spec)
        try:
            text = _read_text(source, timeout=timeout)
        except FileNotFoundError:
            missing_station_ids.append(station_id)
            continue
        raw_table = parse_ghcnd_dly_text(text, supported_elements=tuple(query.elements))
        if raw_table.empty:
            continue
        tables.append(raw_table)
    if not tables:
        if missing_station_ids:
            raise StationNotFoundError(f'No GHCN-Daily .dly data found for station_id: {", ".join(sorted(missing_station_ids))}')
        raise EmptyResultError('No GHCN-Daily observations found for the given query.')

    merged = pd.concat(tables, ignore_index=True)
    normalized = normalize_daily_observations_ghcnd(merged, query=query, station_metadata=station_metadata)
    if normalized.empty:
        raise EmptyResultError('No GHCN-Daily observations found for the given query.')
    return normalized.loc[:, GHCND_NORMALIZED_DAILY_COLUMNS]


def build_station_dly_url(station_id: str, *, spec: GhcndDatasetSpec | None = None) -> str:
    effective_spec = spec or get_dataset_spec('ghcnd', 'daily')
    return f'{effective_spec.data_base_url}/{station_id}.dly'


def _read_text(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    if response.status_code == 404:
        raise FileNotFoundError(source)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text
