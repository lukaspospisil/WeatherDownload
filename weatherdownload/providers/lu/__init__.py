from __future__ import annotations

from .daily import download_daily_observations_lu
from .metadata import read_station_metadata_lu, read_station_observation_metadata_lu
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider


SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_max',
    'tas_min',
    'precipitation',
)


PROVIDER = WeatherProvider(
    country_code='LU',
    name='MeteoLux Luxembourg',
    read_station_metadata=read_station_metadata_lu,
    read_station_observation_metadata=read_station_observation_metadata_lu,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=download_daily_observations_lu,
    supported_country_codes=('LU',),
    supported_providers=('meteolux',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
