from __future__ import annotations

import pandas as pd

from ...metadata import STATION_OBSERVATION_METADATA_COLUMNS
from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .ipma_parser import (
    normalize_ipma_observation_metadata,
    normalize_ipma_station_metadata,
    read_text_from_source,
    sort_station_metadata,
)
from .registry import get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='PO', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='PO',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_ipma(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('ipma', '1hour')
    metadata_text = read_text_from_source(source_url or spec.station_metadata_url or '', timeout=timeout)
    stations = normalize_ipma_station_metadata(metadata_text)
    stations.attrs['source_url'] = source_url or spec.station_metadata_url
    return sort_station_metadata(stations)


def read_station_observation_metadata_ipma(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_ipma(source_url=source_url, timeout=timeout)
    if stations.empty:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)
    spec = get_dataset_spec('ipma', '1hour')
    return normalize_ipma_observation_metadata(stations, spec)
