from __future__ import annotations

import requests

from .geosphere_parser import (
    normalize_geosphere_observation_metadata,
    normalize_geosphere_station_metadata,
    parse_geosphere_metadata_json,
    read_text_from_source,
)
from .geosphere_registry import get_dataset_spec


def read_station_metadata_geosphere(source_url: str | None = None, timeout: int = 60):
    metadata_text = read_text_from_source(source_url or get_dataset_spec('historical', 'daily').metadata_url, timeout, requests)
    return normalize_geosphere_station_metadata(parse_geosphere_metadata_json(metadata_text))


def read_station_observation_metadata_geosphere(source_url: str | None = None, timeout: int = 60):
    spec = get_dataset_spec('historical', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    payload = parse_geosphere_metadata_json(metadata_text)
    return normalize_geosphere_observation_metadata(payload, spec.supported_elements)
