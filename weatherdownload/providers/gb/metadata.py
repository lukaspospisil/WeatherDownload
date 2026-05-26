from __future__ import annotations

import pandas as pd

from ...metadata import STATION_OBSERVATION_METADATA_COLUMNS
from ..ghcnd.mixed import (
    build_mixed_station_metadata_reader,
    build_mixed_station_observation_metadata_reader,
)
from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import (
    normalize_metoffice_datahub_observation_metadata,
    normalize_metoffice_datahub_station_metadata,
    read_text_from_source,
)
from .registry import get_dataset_spec, list_implemented_dataset_specs

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='GB', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='GB',
    get_dataset_spec=get_dataset_spec,
)


def resolve_metoffice_datahub_api_key() -> str:
    import os

    for environment_name in ('WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY', 'METOFFICE_DATAHUB_API_KEY'):
        value = os.getenv(environment_name, '').strip()
        if value:
            return value
    raise ValueError(
        'Met Office Weather DataHub API key is required for GB metoffice_datahub live use. '
        'Set WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY or METOFFICE_DATAHUB_API_KEY before using '
        'country="GB", provider="metoffice_datahub".'
    )


def read_station_metadata_metoffice_datahub(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('metoffice_datahub', '1hour')
    metadata_text = read_text_from_source(source_url or spec.station_metadata_url or '', timeout=timeout)
    stations = normalize_metoffice_datahub_station_metadata(metadata_text)
    stations.attrs['source_url'] = source_url or spec.station_metadata_url
    return stations


def read_station_observation_metadata_metoffice_datahub(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_metoffice_datahub(source_url=source_url, timeout=timeout)
    if stations.empty:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)
    spec = get_dataset_spec('metoffice_datahub', '1hour')
    return normalize_metoffice_datahub_observation_metadata(stations, spec)


read_station_metadata = build_mixed_station_metadata_reader(
    read_national_station_metadata=read_station_metadata_metoffice_datahub,
    read_ghcnd_station_metadata=read_station_metadata_ghcnd,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
)
read_station_observation_metadata = build_mixed_station_observation_metadata_reader(
    read_national_station_observation_metadata=read_station_observation_metadata_metoffice_datahub,
    read_ghcnd_station_observation_metadata=read_station_observation_metadata_ghcnd,
)
