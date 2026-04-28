from __future__ import annotations

from .metadata import read_station_metadata_ghcnd, read_station_observation_metadata_ghcnd
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..ghcnd.wrappers import build_country_provider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_max',
    'tas_min',
    'precipitation',
)

from .observations import download_daily_observations_ghcnd


PROVIDER = build_country_provider(
    country_code='NZ',
    read_station_metadata=read_station_metadata_ghcnd,
    read_station_observation_metadata=read_station_observation_metadata_ghcnd,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_daily_observations=download_daily_observations_ghcnd,
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
)
