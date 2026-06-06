from __future__ import annotations

import pandas as pd
import requests

from ..ch.metadata import _related_source
from ..ch.parser import (
    normalize_ch_observation_metadata,
    parse_ch_data_inventory_csv,
    parse_ch_parameter_metadata_csv,
    read_text_from_source,
)
from ..ch.metadata import read_station_metadata_ch
from .registry import (
    LI_DAILY_PARAMETER_METADATA,
    LI_METEOSWISS_STATION_IDS,
    get_dataset_spec,
)


def read_station_metadata_li(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_ch(source_url=source_url, timeout=timeout)
    return _filter_li_stations(stations)


def read_station_observation_metadata_li(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations_source = source_url or get_dataset_spec('meteoswiss', 'daily').station_metadata_url
    stations = read_station_metadata_li(source_url=stations_source, timeout=timeout)
    if stations.empty:
        return pd.DataFrame(columns=['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'])

    parameter_source = _related_source(stations_source, 'parameters') or get_dataset_spec('meteoswiss', 'daily').parameter_metadata_url
    inventory_source = _related_source(stations_source, 'datainventory') or get_dataset_spec('meteoswiss', 'daily').data_inventory_url

    try:
        parameter_table = parse_ch_parameter_metadata_csv(read_text_from_source(parameter_source, timeout, requests))
    except Exception:
        parameter_table = None
    try:
        inventory_table = parse_ch_data_inventory_csv(read_text_from_source(inventory_source, timeout, requests))
    except Exception:
        inventory_table = None

    metadata = normalize_ch_observation_metadata(
        stations,
        [(get_dataset_spec('meteoswiss', 'daily'), LI_DAILY_PARAMETER_METADATA)],
        parameter_table=parameter_table,
        inventory_table=inventory_table,
    )
    return metadata.reset_index(drop=True)


def _filter_li_stations(stations: pd.DataFrame) -> pd.DataFrame:
    if stations.empty:
        return stations.copy()
    filtered = stations[stations['station_id'].astype('string').str.upper().isin(LI_METEOSWISS_STATION_IDS)]
    return filtered.reset_index(drop=True)
