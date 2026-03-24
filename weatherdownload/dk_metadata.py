from __future__ import annotations

import pandas as pd
import requests

from .dk_parser import normalize_dk_observation_metadata, normalize_dk_station_metadata, parse_dk_feature_collection_json, read_text_from_source
from .dk_registry import DK_DAILY_PARAMETER_METADATA, DK_HOURLY_PARAMETER_METADATA, DK_TENMIN_PARAMETER_METADATA, get_dataset_spec, list_implemented_dataset_specs



def read_station_metadata_dk(source_url: str | None = None, timeout: int = 60):
    spec = get_dataset_spec('historical', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    return normalize_dk_station_metadata(parse_dk_feature_collection_json(metadata_text))



def read_station_observation_metadata_dk(source_url: str | None = None, timeout: int = 60):
    stations_text = read_text_from_source(source_url or get_dataset_spec('historical', 'daily').metadata_url, timeout, requests)
    payload = parse_dk_feature_collection_json(stations_text)
    frames = []
    for spec in list_implemented_dataset_specs():
        frames.append(normalize_dk_observation_metadata(payload, spec, _parameter_metadata_for_spec(spec)))
    if not frames:
        return normalize_dk_observation_metadata(payload, get_dataset_spec('historical', 'daily'), DK_DAILY_PARAMETER_METADATA)
    return pd.concat(frames, ignore_index=True)



def _parameter_metadata_for_spec(spec):
    if spec.resolution == '10min':
        return DK_TENMIN_PARAMETER_METADATA
    if spec.resolution == '1hour':
        return DK_HOURLY_PARAMETER_METADATA
    return DK_DAILY_PARAMETER_METADATA
