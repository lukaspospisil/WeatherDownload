from __future__ import annotations

import pandas as pd
import requests

from .parser import normalize_hu_observation_metadata, parse_hu_station_metadata_csv, read_text_from_source
from .registry import (
    HU_DAILY_PARAMETER_METADATA,
    HU_HOURLY_PARAMETER_METADATA,
    HU_TENMIN_PARAMETER_METADATA,
    HU_TENMIN_WIND_METADATA_URL,
    HU_TENMIN_WIND_PARAMETER_METADATA,
    get_dataset_spec,
)


def read_station_metadata_hu(
    source_url: str | None = None,
    timeout: int = 60,
    wind_source_url: str | None = None,
) -> pd.DataFrame:
    generic_stations = _read_station_metadata_table(source_url or get_dataset_spec('historical', 'daily').metadata_url, timeout)
    if source_url is not None and wind_source_url is None:
        return generic_stations

    wind_stations = _read_station_metadata_table(wind_source_url or HU_TENMIN_WIND_METADATA_URL, timeout)
    if generic_stations.empty:
        return wind_stations
    if wind_stations.empty:
        return generic_stations

    combined = pd.concat([generic_stations, wind_stations], ignore_index=True)
    combined = combined.drop_duplicates().reset_index(drop=True)
    return combined


def read_station_observation_metadata_hu(
    source_url: str | None = None,
    timeout: int = 60,
    wind_source_url: str | None = None,
) -> pd.DataFrame:
    generic_stations = _read_station_metadata_table(source_url or get_dataset_spec('historical', 'daily').metadata_url, timeout)
    generic_specs_and_metadata = [
        (get_dataset_spec('historical', 'daily'), HU_DAILY_PARAMETER_METADATA),
        (get_dataset_spec('historical', '1hour'), HU_HOURLY_PARAMETER_METADATA),
        (get_dataset_spec('historical', '10min'), HU_TENMIN_PARAMETER_METADATA),
    ]
    generic_metadata = normalize_hu_observation_metadata(generic_stations, generic_specs_and_metadata)

    if source_url is not None and wind_source_url is None:
        return generic_metadata

    wind_stations = _read_station_metadata_table(wind_source_url or HU_TENMIN_WIND_METADATA_URL, timeout)
    wind_metadata = normalize_hu_observation_metadata(
        wind_stations,
        [(get_dataset_spec('historical_wind', '10min'), HU_TENMIN_WIND_PARAMETER_METADATA)],
    )
    if generic_metadata.empty:
        return wind_metadata
    if wind_metadata.empty:
        return generic_metadata
    return pd.concat([generic_metadata, wind_metadata], ignore_index=True)



def _read_station_metadata_table(source_url: str, timeout: int) -> pd.DataFrame:
    metadata_text = read_text_from_source(source_url, timeout, requests)
    return parse_hu_station_metadata_csv(metadata_text)

