from __future__ import annotations

import requests

from .be_parser import normalize_be_observation_metadata, normalize_be_station_metadata, parse_be_feature_collection_json, read_text_from_source
from .be_registry import BE_PARAMETER_METADATA, get_dataset_spec


def read_station_metadata_be(source_url: str | None = None, timeout: int = 60):
    spec = get_dataset_spec('historical', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    return normalize_be_station_metadata(parse_be_feature_collection_json(metadata_text))



def read_station_observation_metadata_be(source_url: str | None = None, timeout: int = 60):
    stations = read_station_metadata_be(source_url=source_url, timeout=timeout)
    spec = get_dataset_spec('historical', 'daily')
    return normalize_be_observation_metadata(stations, spec, BE_PARAMETER_METADATA)
