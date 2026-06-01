from __future__ import annotations

import os

import pandas as pd
import requests

from ...metadata import STATION_OBSERVATION_METADATA_COLUMNS
from ..ghcnd.mixed import (
    build_mixed_station_metadata_reader,
    build_mixed_station_observation_metadata_reader,
)
from .ghcnd import (
    read_station_metadata_ghcnd,
    read_station_observation_metadata_ghcnd,
)
from .parser import (
    normalize_frost_observation_metadata,
    parse_frost_station_metadata_json,
    read_text_from_source,
)
from .registry import get_dataset_spec, list_implemented_dataset_specs


def resolve_frost_client_id() -> str:
    for environment_name in ('WEATHERDOWNLOAD_FROST_CLIENT_ID', 'FROST_CLIENT_ID'):
        value = os.getenv(environment_name, '').strip()
        if value:
            return value
    raise ValueError(
        'MET Norway Frost client ID is required for NO frost live use. Set WEATHERDOWNLOAD_FROST_CLIENT_ID or FROST_CLIENT_ID before using country="NO", provider="frost".'
    )


def read_station_metadata_frost(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('frost', 'daily')
    if source_url is not None:
        stations = parse_frost_station_metadata_json(read_text_from_source(source_url, timeout, requests))
        stations.attrs['source_url'] = source_url
        return stations

    response = requests.get(
        spec.station_metadata_url,
        params={'types': 'SensorSystem', 'country': 'NO'},
        timeout=timeout,
        auth=(resolve_frost_client_id(), ''),
    )
    response.raise_for_status()
    response.encoding = 'utf-8'
    stations = parse_frost_station_metadata_json(response.text)
    stations.attrs['source_url'] = spec.station_metadata_url
    return stations


def read_station_observation_metadata_frost(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_frost(source_url=source_url, timeout=timeout)
    if stations.empty:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)
    return normalize_frost_observation_metadata(stations, get_dataset_spec('frost', 'daily'))


read_station_metadata = build_mixed_station_metadata_reader(
    read_national_station_metadata=read_station_metadata_frost,
    read_ghcnd_station_metadata=read_station_metadata_ghcnd,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
)
read_station_observation_metadata = build_mixed_station_observation_metadata_reader(
    read_national_station_observation_metadata=read_station_observation_metadata_frost,
    read_ghcnd_station_observation_metadata=read_station_observation_metadata_ghcnd,
)
