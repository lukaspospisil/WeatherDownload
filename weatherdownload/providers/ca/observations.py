from __future__ import annotations

import pandas as pd

from ...queries import ObservationQuery
from ..ghcnd.observations import (
    build_station_dly_url as build_station_dly_url_shared,
    download_daily_observations_ghcnd as download_daily_observations_ghcnd_shared,
)
from ..ghcnd.registry import GhcndDatasetSpec
from .registry import get_dataset_spec


def download_daily_observations_ghcnd(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    spec = get_dataset_spec('ghcnd', 'daily')
    return download_daily_observations_ghcnd_shared(query, timeout, station_metadata, spec=spec)


def build_station_dly_url(station_id: str, *, spec: GhcndDatasetSpec | None = None) -> str:
    effective_spec = spec or get_dataset_spec('ghcnd', 'daily')
    return build_station_dly_url_shared(station_id, spec=effective_spec)
