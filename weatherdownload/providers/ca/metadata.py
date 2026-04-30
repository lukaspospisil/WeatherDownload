from __future__ import annotations

import pandas as pd

import requests

from .eccc_parser import (
    normalize_ca_eccc_observation_metadata,
    parse_ca_eccc_station_metadata_feature_collection,
    read_text_from_source,
)
from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .registry import get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='CA', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='CA',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_eccc(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('eccc', 'daily').station_metadata_url
    metadata_text = read_text_from_source(source, timeout, requests)
    stations = parse_ca_eccc_station_metadata_feature_collection(metadata_text)
    stations.attrs['source_url'] = source
    return stations


def read_station_observation_metadata_eccc(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_eccc(source_url=source_url, timeout=timeout)
    return normalize_ca_eccc_observation_metadata(stations)
