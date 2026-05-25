from __future__ import annotations

import pandas as pd
import requests

from .parser import (
    normalize_asta_observation_metadata,
    normalize_asta_station_metadata,
    normalize_lu_observation_metadata,
    normalize_lu_station_metadata,
    parse_lu_feature_collection_json,
)
from .registry import LU_METEOLUX_WFS_URL, get_dataset_spec, list_implemented_dataset_specs


def read_station_metadata_lu(source_url: str | None = None, timeout: int = 60):
    del source_url
    frames = [normalize_lu_station_metadata()]
    asta_stations = _read_asta_station_metadata(timeout=timeout)
    if not asta_stations.empty:
        frames.append(asta_stations)
    return pd.concat(frames, ignore_index=True)


def read_station_observation_metadata_lu(source_url: str | None = None, timeout: int = 60):
    del source_url
    stations = read_station_metadata_lu(timeout=timeout)
    frames = []
    for spec in list_implemented_dataset_specs():
        if spec.provider == 'asta':
            asta_stations = stations[stations['station_id'].astype(str).str.startswith('AGM_')].reset_index(drop=True)
            frames.append(normalize_asta_observation_metadata(asta_stations, spec))
        else:
            meteolux_stations = stations[stations['station_id'] == '0-20000-0-06590'].reset_index(drop=True)
            frames.append(normalize_lu_observation_metadata(meteolux_stations, spec))
    if not frames:
        return normalize_lu_observation_metadata(stations, get_dataset_spec('meteolux', 'daily'))
    return frames[0] if len(frames) == 1 else pd.concat(frames, ignore_index=True)


def _read_asta_station_metadata(timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('asta', 'daily')
    params = {
        'SERVICE': 'WFS',
        'VERSION': '2.0.0',
        'REQUEST': 'GetFeature',
        'TYPENAMES': 'MF.SpatialSamplingFeature_ASTA',
        'OUTPUTFORMAT': 'application/json',
        'SRSNAME': 'EPSG:4326',
        'COUNT': '500',
    }
    response = requests.get(LU_METEOLUX_WFS_URL, params=params, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return normalize_asta_station_metadata(parse_lu_feature_collection_json(response.text))
