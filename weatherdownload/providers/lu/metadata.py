from __future__ import annotations

import pandas as pd

from .parser import normalize_lu_observation_metadata, normalize_lu_station_metadata
from .registry import get_dataset_spec, list_implemented_dataset_specs


def read_station_metadata_lu(source_url: str | None = None, timeout: int = 60):
    del source_url, timeout
    return normalize_lu_station_metadata()


def read_station_observation_metadata_lu(source_url: str | None = None, timeout: int = 60):
    del source_url, timeout
    stations = normalize_lu_station_metadata()
    frames = [normalize_lu_observation_metadata(stations, spec) for spec in list_implemented_dataset_specs()]
    if not frames:
        return normalize_lu_observation_metadata(stations, get_dataset_spec('meteolux', 'daily'))
    return frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)
