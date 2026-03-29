from __future__ import annotations

import pandas as pd
import requests

from .hu_parser import normalize_hu_observation_metadata, parse_hu_station_metadata_csv, read_text_from_source
from .hu_registry import (
    HU_DAILY_PARAMETER_METADATA,
    HU_HOURLY_PARAMETER_METADATA,
    HU_TENMIN_PARAMETER_METADATA,
    get_dataset_spec,
)


def read_station_metadata_hu(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('historical', 'daily')
    metadata_text = read_text_from_source(source_url or spec.metadata_url, timeout, requests)
    return parse_hu_station_metadata_csv(metadata_text)


def read_station_observation_metadata_hu(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_hu(source_url=source_url, timeout=timeout)
    specs_and_metadata = [
        (get_dataset_spec('historical', 'daily'), HU_DAILY_PARAMETER_METADATA),
        (get_dataset_spec('historical', '1hour'), HU_HOURLY_PARAMETER_METADATA),
        (get_dataset_spec('historical', '10min'), HU_TENMIN_PARAMETER_METADATA),
    ]
    return normalize_hu_observation_metadata(stations, specs_and_metadata)
