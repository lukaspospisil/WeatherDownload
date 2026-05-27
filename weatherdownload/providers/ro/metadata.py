from __future__ import annotations

import pandas as pd

from .parser import normalize_ro_anm_observation_metadata, read_ro_anm_station_metadata
from .registry import RO_ANM_PARAMETER_METADATA, get_dataset_spec
from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='RO', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='RO',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_anm(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('anm', 'daily').station_metadata_url
    return read_ro_anm_station_metadata(source, timeout=timeout)


def read_station_observation_metadata_anm(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_anm(source_url=source_url, timeout=timeout)
    return normalize_ro_anm_observation_metadata(stations, get_dataset_spec('anm', 'daily'), RO_ANM_PARAMETER_METADATA)
