from __future__ import annotations

from .metadata import (
    read_station_metadata_ghcnd,
    read_station_metadata_rmi,
    read_station_observation_metadata_ghcnd,
    read_station_observation_metadata_rmi,
)
from .observations import download_daily_observations_ghcnd, download_daily_observations_rmi
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider
from ..ghcnd.mixed import (
    build_mixed_station_metadata_reader,
    build_mixed_station_observation_metadata_reader,
)

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'wind_speed',
    'wind_speed_max',
    'relative_humidity',
    'pressure',
    'sunshine_duration',
    'snow_depth',
)

_read_station_metadata = build_mixed_station_metadata_reader(
    read_national_station_metadata=read_station_metadata_rmi,
    read_ghcnd_station_metadata=read_station_metadata_ghcnd,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
)
_read_station_observation_metadata = build_mixed_station_observation_metadata_reader(
    read_national_station_observation_metadata=read_station_observation_metadata_rmi,
    read_ghcnd_station_observation_metadata=read_station_observation_metadata_ghcnd,
)


def _download_be_observations(*args, **kwargs):
    query = args[0] if args else kwargs.get('query')
    if getattr(query, 'provider', None) == 'ghcnd':
        return download_daily_observations_ghcnd(*args, **kwargs)
    if getattr(query, 'provider', None) == 'rmi':
        return download_daily_observations_rmi(*args, **kwargs)
    from .hourly import download_hourly_observations_be
    from .tenmin import download_tenmin_observations_be

    if getattr(query, 'provider', None) == 'historical' and getattr(query, 'resolution', None) == '1hour':
        return download_hourly_observations_be(*args, **kwargs)
    if getattr(query, 'provider', None) == 'historical' and getattr(query, 'resolution', None) == '10min':
        return download_tenmin_observations_be(*args, **kwargs)
    raise NotImplementedError(
        'RMI/KMI Belgium support currently implements rmi/daily, ghcnd/daily, historical/1hour, and historical/10min station observations.'
    )


PROVIDER = WeatherProvider(
    country_code='BE',
    name='RMI/KMI daily + NOAA GHCN-Daily + RMI/KMI historical subdaily',
    read_station_metadata=_read_station_metadata,
    read_station_observation_metadata=_read_station_observation_metadata,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_be_observations,
    supported_country_codes=('BE',),
    supported_providers=('ghcnd', 'historical', 'rmi'),
    supported_resolutions=('daily', '1hour', '10min'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
