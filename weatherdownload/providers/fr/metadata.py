from __future__ import annotations

import json

import pandas as pd
import requests

from .ghcnd import read_station_metadata_ghcnd, read_station_observation_metadata_ghcnd
from .parser import normalize_fr_observation_metadata, parse_fr_station_metadata_json, read_text_from_source
from .registry import FR_DAILY_PARAMETER_METADATA, get_dataset_spec


def read_station_metadata_fr(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('meteo_france', 'daily').station_metadata_url
    metadata_text = read_text_from_source(source, timeout, requests)
    return parse_fr_station_metadata_json(metadata_text)


def read_station_observation_metadata_fr(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('meteo_france', 'daily').station_metadata_url
    metadata_text = read_text_from_source(source, timeout, requests)
    payload = json.loads(metadata_text.lstrip('\ufeff'))
    if not isinstance(payload, list):
        raise ValueError('Meteo-France station metadata must be a top-level JSON list.')
    return normalize_fr_observation_metadata(payload, get_dataset_spec('meteo_france', 'daily'), FR_DAILY_PARAMETER_METADATA)
