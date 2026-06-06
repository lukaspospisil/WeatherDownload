from __future__ import annotations

import pandas as pd
import requests

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import normalize_be_observation_metadata, normalize_be_station_metadata, parse_be_feature_collection_json, read_text_from_source
from .registry import (
    BE_DAILY_PARAMETER_METADATA,
    BE_HOURLY_PARAMETER_METADATA,
    BE_TENMIN_PARAMETER_METADATA,
    get_dataset_spec,
    list_implemented_dataset_specs,
)

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='BE', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='BE',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_rmi(source_url: str | None = None, timeout: int = 60):
    spec = get_dataset_spec('rmi', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    return normalize_be_station_metadata(parse_be_feature_collection_json(metadata_text))


def read_station_observation_metadata_rmi(source_url: str | None = None, timeout: int = 60):
    stations = read_station_metadata_rmi(source_url=source_url, timeout=timeout)
    frames = []
    for spec in list_implemented_dataset_specs():
        if spec.provider == 'ghcnd':
            continue
        parameter_metadata = _parameter_metadata_for_spec(spec)
        frames.append(normalize_be_observation_metadata(stations, spec, parameter_metadata))
    if not frames:
        return normalize_be_observation_metadata(stations, get_dataset_spec('rmi', 'daily'), BE_DAILY_PARAMETER_METADATA)
    return pd.concat(frames, ignore_index=True)


read_station_metadata_be = read_station_metadata_rmi
read_station_observation_metadata_be = read_station_observation_metadata_rmi


def _parameter_metadata_for_spec(spec):
    if spec.resolution == '1hour':
        return BE_HOURLY_PARAMETER_METADATA
    if spec.resolution == '10min':
        return BE_TENMIN_PARAMETER_METADATA
    return BE_DAILY_PARAMETER_METADATA
