from __future__ import annotations

from .parser import normalize_ie_observation_metadata, normalize_ie_station_metadata
from .registry import get_dataset_spec


def read_station_metadata_ie(source_url: str | None = None, timeout: int = 60):
    del source_url, timeout
    return normalize_ie_station_metadata()


def read_station_observation_metadata_ie(source_url: str | None = None, timeout: int = 60):
    del source_url, timeout
    stations = normalize_ie_station_metadata()
    return normalize_ie_observation_metadata(stations, get_dataset_spec('meteireann', 'daily'))
