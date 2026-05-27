from __future__ import annotations

import pandas as pd

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import normalize_si_arso_observation_metadata, read_si_arso_station_metadata
from .registry import SI_ARSO_PARAMETER_METADATA, get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='SI', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='SI',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_arso(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('arso', 'daily').station_metadata_url
    return read_si_arso_station_metadata(source, timeout=timeout)


def read_station_observation_metadata_arso(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_arso(source_url=source_url, timeout=timeout)
    return normalize_si_arso_observation_metadata(stations, get_dataset_spec('arso', 'daily'), SI_ARSO_PARAMETER_METADATA)
