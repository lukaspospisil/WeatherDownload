from __future__ import annotations

import pandas as pd
import requests

from .pl_parser import normalize_pl_observation_metadata, parse_pl_station_metadata_csv, read_text_from_source
from .pl_registry import PL_DAILY_PARAMETER_METADATA, get_dataset_spec


def read_station_metadata_pl(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('historical', 'daily').station_metadata_url
    metadata_text = read_text_from_source(source, timeout, requests)
    return parse_pl_station_metadata_csv(metadata_text)


def read_station_observation_metadata_pl(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations_source = source_url or get_dataset_spec('historical', 'daily').station_metadata_url
    stations = parse_pl_station_metadata_csv(read_text_from_source(stations_source, timeout, requests))
    specs_and_metadata = [
        (get_dataset_spec('historical', 'daily'), PL_DAILY_PARAMETER_METADATA),
    ]
    return normalize_pl_observation_metadata(stations, specs_and_metadata)
