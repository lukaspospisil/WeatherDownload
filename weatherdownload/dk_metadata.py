from __future__ import annotations

import requests
import pandas as pd

from .dk_parser import normalize_dk_observation_metadata, normalize_dk_station_metadata, parse_dk_feature_collection_json, read_text_from_source
from .dk_registry import DK_DAILY_PARAMETER_METADATA, get_dataset_spec


def read_station_metadata_dk(source_url: str | None = None, timeout: int = 60):
    spec = get_dataset_spec('historical', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    return normalize_dk_station_metadata(parse_dk_feature_collection_json(metadata_text))



def read_station_observation_metadata_dk(source_url: str | None = None, timeout: int = 60):
    spec = get_dataset_spec('historical', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    payload = parse_dk_feature_collection_json(metadata_text)
    return normalize_dk_observation_metadata(payload, spec, DK_DAILY_PARAMETER_METADATA)
