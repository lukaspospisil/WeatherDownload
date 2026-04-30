from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
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
        raise UnsupportedQueryError(
            'Live ECCC daily download is not implemented yet. Use fixture-backed station metadata for the current parser-level national path.'
        )

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


def _resolve_local_fixture_source(station_metadata: pd.DataFrame | None) -> str | None:
    if station_metadata is None:
        return None
    source = station_metadata.attrs.get('source_url')
    if not isinstance(source, str):
        return None
    return source if Path(source).exists() else None
