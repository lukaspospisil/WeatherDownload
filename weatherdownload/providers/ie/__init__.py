from __future__ import annotations

from .daily import download_daily_observations_ie
from .metadata import read_station_metadata_ie, read_station_observation_metadata_ie
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider


SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_max',
    'tas_min',
    'precipitation',
    'wind_speed',
    'sunshine_duration',
)


PROVIDER = WeatherProvider(
    country_code='IE',
    name='Met Eireann Ireland',
    read_station_metadata=read_station_metadata_ie,
    read_station_observation_metadata=read_station_observation_metadata_ie,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=download_daily_observations_ie,
    supported_country_codes=('IE',),
    supported_providers=('meteireann',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
