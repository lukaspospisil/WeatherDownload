from __future__ import annotations

from .metadata import (
    read_station_metadata,
    read_station_metadata_ghcnd,
    read_station_metadata_metoffice_datahub,
    read_station_observation_metadata,
    read_station_observation_metadata_ghcnd,
    read_station_observation_metadata_metoffice_datahub,
)
from .observations import (
    download_daily_observations_ghcnd,
    download_hourly_observations_metoffice_datahub,
    download_observations,
)
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'snow_depth',
    'relative_humidity',
    'wind_speed',
    'pressure',
)


PROVIDER = WeatherProvider(
    country_code='GB',
    name='Met Office Weather DataHub recent hourly + NOAA GHCN-Daily',
    read_station_metadata=read_station_metadata,
    read_station_observation_metadata=read_station_observation_metadata,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=download_observations,
    supported_country_codes=('GB',),
    supported_providers=('ghcnd', 'metoffice_datahub'),
    supported_resolutions=('daily', '1hour'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
)

__all__ = [
    'PROVIDER',
    'SUPPORTED_CANONICAL_ELEMENTS',
    'download_daily_observations_ghcnd',
    'download_hourly_observations_metoffice_datahub',
    'read_station_metadata',
    'read_station_metadata_ghcnd',
    'read_station_metadata_metoffice_datahub',
    'read_station_observation_metadata',
    'read_station_observation_metadata_ghcnd',
    'read_station_observation_metadata_metoffice_datahub',
]
