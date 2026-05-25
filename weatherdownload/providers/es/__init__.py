from __future__ import annotations

from .daily import download_daily_observations_es
from .metadata import read_station_metadata_es, read_station_observation_metadata_es
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'wind_speed',
    'relative_humidity',
    'sunshine_duration',
)


PROVIDER = WeatherProvider(
    country_code='ES',
    name='AEMET Spain',
    read_station_metadata=read_station_metadata_es,
    read_station_observation_metadata=read_station_observation_metadata_es,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=download_daily_observations_es,
    supported_country_codes=('ES',),
    supported_providers=('aemet',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
