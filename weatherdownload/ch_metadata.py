from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from .ch_parser import (
    normalize_ch_observation_metadata,
    parse_ch_data_inventory_csv,
    parse_ch_parameter_metadata_csv,
    parse_ch_station_metadata_csv,
    read_text_from_source,
)
from .ch_registry import (
    CH_DAILY_PARAMETER_METADATA,
    CH_HOURLY_PARAMETER_METADATA,
    CH_TENMIN_PARAMETER_METADATA,
    get_dataset_spec,
)



def read_station_metadata_ch(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('historical', 'daily').station_metadata_url
    metadata_text = read_text_from_source(source, timeout, requests)
    return parse_ch_station_metadata_csv(metadata_text)



def read_station_observation_metadata_ch(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations_source = source_url or get_dataset_spec('historical', 'daily').station_metadata_url
    stations = parse_ch_station_metadata_csv(read_text_from_source(stations_source, timeout, requests))

    parameter_source = _related_source(stations_source, 'parameters') or get_dataset_spec('historical', 'daily').parameter_metadata_url
    inventory_source = _related_source(stations_source, 'datainventory') or get_dataset_spec('historical', 'daily').data_inventory_url

    parameter_table = _try_read_parameter_table(parameter_source, timeout)
    inventory_table = _try_read_inventory_table(inventory_source, timeout)

    specs_and_metadata = [
        (get_dataset_spec('historical', 'daily'), CH_DAILY_PARAMETER_METADATA),
        (get_dataset_spec('historical', '1hour'), CH_HOURLY_PARAMETER_METADATA),
        (get_dataset_spec('historical', '10min'), CH_TENMIN_PARAMETER_METADATA),
    ]
    return normalize_ch_observation_metadata(
        stations,
        specs_and_metadata,
        parameter_table=parameter_table,
        inventory_table=inventory_table,
    )



def _related_source(stations_source: str, kind: str) -> str | None:
    local_path = Path(stations_source)
    if not local_path.exists():
        return None
    filename = local_path.name
    replacements = {
        'parameters': ['meta_stations', 'meta_parameters'],
        'datainventory': ['meta_stations', 'meta_datainventory'],
    }
    source_token, target_token = replacements[kind]
    candidate_names = [filename.replace(source_token, target_token)]
    if 'stations' in filename:
        candidate_names.append(filename.replace('stations', target_token))
    for candidate_name in candidate_names:
        candidate = local_path.with_name(candidate_name)
        if candidate.exists():
            return str(candidate)
    return None



def _try_read_parameter_table(source: str, timeout: int) -> pd.DataFrame | None:
    try:
        return parse_ch_parameter_metadata_csv(read_text_from_source(source, timeout, requests))
    except Exception:
        return None



def _try_read_inventory_table(source: str, timeout: int) -> pd.DataFrame | None:
    try:
        return parse_ch_data_inventory_csv(read_text_from_source(source, timeout, requests))
    except Exception:
        return None
