from __future__ import annotations

import requests
import pandas as pd

from .parser import normalize_be_observation_metadata, normalize_be_station_metadata, parse_be_feature_collection_json, read_text_from_source
from .registry import (
    BE_DAILY_PARAMETER_METADATA,
    BE_HOURLY_PARAMETER_METADATA,
    BE_TENMIN_PARAMETER_METADATA,
    get_dataset_spec,
    list_implemented_dataset_specs,
)


def read_station_metadata_be(source_url: str | None = None, timeout: int = 60):
    spec = get_dataset_spec('historical', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    return normalize_be_station_metadata(parse_be_feature_collection_json(metadata_text))



def read_station_observation_metadata_be(source_url: str | None = None, timeout: int = 60):
    stations = read_station_metadata_be(source_url=source_url, timeout=timeout)
    frames = []
    for spec in list_implemented_dataset_specs():
        parameter_metadata = _parameter_metadata_for_spec(spec)
        frames.append(normalize_be_observation_metadata(stations, spec, parameter_metadata))
    if not frames:
        return normalize_be_observation_metadata(stations, get_dataset_spec('historical', 'daily'), BE_DAILY_PARAMETER_METADATA)
    return pd.concat(frames, ignore_index=True)



def _parameter_metadata_for_spec(spec):
    if spec.resolution == '1hour':
        return BE_HOURLY_PARAMETER_METADATA
    if spec.resolution == '10min':
        return BE_TENMIN_PARAMETER_METADATA
    return BE_DAILY_PARAMETER_METADATA

