from __future__ import annotations

import pandas as pd

from ..ghcnd.metadata import (
    read_station_metadata_ghcnd as read_station_metadata_ghcnd_shared,
    read_station_observation_metadata_ghcnd as read_station_observation_metadata_ghcnd_shared,
)
from .registry import get_dataset_spec


def read_station_metadata_ghcnd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('ghcnd', 'daily')
    return read_station_metadata_ghcnd_shared(source_url, timeout, spec=spec, country_prefix='MX')


def read_station_observation_metadata_ghcnd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('ghcnd', 'daily')
    return read_station_observation_metadata_ghcnd_shared(source_url, timeout, spec=spec, country_prefix='MX')
