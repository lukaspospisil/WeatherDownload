from __future__ import annotations

import pandas as pd
import requests

from .parser import (
    normalize_pl_observation_metadata,
    parse_pl_meteo_station_coordinates_json,
    parse_pl_station_metadata_csv,
    read_text_from_source,
)
from .registry import (
    PL_DAILY_KLIMAT_PARAMETER_METADATA,
    PL_DAILY_SYNOP_PARAMETER_METADATA,
    PL_HOURLY_SYNOP_PARAMETER_METADATA,
    get_dataset_spec,
)

PL_METEO_API_URL = 'https://danepubliczne.imgw.pl/api/data/meteo'


def read_station_metadata_pl(
    source_url: str | None = None,
    timeout: int = 60,
    coordinates_source_url: str | None = None,
) -> pd.DataFrame:
    source = source_url or get_dataset_spec('historical', 'daily').station_metadata_url
    metadata_text = read_text_from_source(source, timeout, requests)
    stations = parse_pl_station_metadata_csv(metadata_text)
    if stations.empty:
        return stations
    if source_url is not None and coordinates_source_url is None:
        return stations
    coordinates_source = coordinates_source_url or PL_METEO_API_URL
    return _enrich_pl_station_metadata_with_coordinates(stations, coordinates_source, timeout)


def read_station_observation_metadata_pl(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations_source = source_url or get_dataset_spec('historical', 'daily').station_metadata_url
    stations = parse_pl_station_metadata_csv(read_text_from_source(stations_source, timeout, requests))
    specs_and_metadata = [
        (get_dataset_spec('historical', 'daily'), PL_DAILY_SYNOP_PARAMETER_METADATA),
        (get_dataset_spec('historical', '1hour'), PL_HOURLY_SYNOP_PARAMETER_METADATA),
        (get_dataset_spec('historical_klimat', 'daily'), PL_DAILY_KLIMAT_PARAMETER_METADATA),
    ]
    return normalize_pl_observation_metadata(stations, specs_and_metadata)


def _enrich_pl_station_metadata_with_coordinates(
    stations: pd.DataFrame,
    coordinates_source_url: str,
    timeout: int,
) -> pd.DataFrame:
    try:
        coordinates_text = read_text_from_source(coordinates_source_url, timeout, requests)
        coordinates = parse_pl_meteo_station_coordinates_json(coordinates_text)
    except (requests.RequestException, OSError, ValueError):
        return stations

    if coordinates.empty:
        return stations

    enriched = stations.merge(coordinates, on='gh_id', how='left', suffixes=('', '_api'))
    for column in ('longitude', 'latitude'):
        enriched[column] = enriched[column].where(enriched[column].notna(), enriched[f'{column}_api'])
        enriched = enriched.drop(columns=[f'{column}_api'])
    return enriched


