from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from .parser import (
    normalize_ghcnd_observation_metadata,
    normalize_ghcnd_station_metadata,
    parse_ghcnd_inventory_text,
    parse_ghcnd_stations_text,
)
from .registry import GHCND_INVENTORY_URL, GHCND_STATIONS_URL, get_dataset_spec


def read_station_metadata_ghcnd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('ghcnd', 'daily')
    stations_source = source_url or GHCND_STATIONS_URL
    inventory_source = _related_source(stations_source, 'stations', 'inventory') or GHCND_INVENTORY_URL
    stations_table = parse_ghcnd_stations_text(_read_text(stations_source, timeout=timeout))
    inventory_table = parse_ghcnd_inventory_text(_read_text(inventory_source, timeout=timeout))
    return normalize_ghcnd_station_metadata(
        stations_table,
        inventory_table,
        country='US',
        required_elements=spec.supported_elements,
    )


def read_station_observation_metadata_ghcnd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('ghcnd', 'daily')
    inventory_source = source_url or GHCND_INVENTORY_URL
    if source_url is not None and source_url.endswith('stations.txt'):
        inventory_source = _related_source(source_url, 'stations', 'inventory') or GHCND_INVENTORY_URL
    inventory_table = parse_ghcnd_inventory_text(_read_text(inventory_source, timeout=timeout))
    return normalize_ghcnd_observation_metadata(
        inventory_table,
        country='US',
        supported_elements=spec.supported_elements,
    )


def _read_text(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _related_source(source: str, old: str, new: str) -> str | None:
    local_path = Path(source)
    if local_path.exists():
        candidate = local_path.with_name(local_path.name.replace(old, new))
        if candidate.exists():
            return str(candidate)
        return None
    if old not in source:
        return None
    return source.replace(old, new)
