from __future__ import annotations

from .daily_fmi import download_daily_observations_fmi
from .hourly_fmi import download_hourly_observations_fmi
from .metadata import (
    read_station_metadata_fmi,
    read_station_metadata_ghcnd,
    read_station_observation_metadata_fmi,
    read_station_observation_metadata_ghcnd,
)
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider
from ..ghcnd.mixed import (
    build_mixed_observation_downloader,
    build_mixed_station_metadata_reader,
    build_mixed_station_observation_metadata_reader,
)

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'wind_speed',
    'snow_depth',
)

from .observations import download_daily_observations_ghcnd


def _download_national_observations(*args, **kwargs):
    query = args[0] if args else kwargs.get('query')
    if getattr(query, 'provider', None) == 'fmi' and getattr(query, 'resolution', None) == '1hour':
        return download_hourly_observations_fmi(*args, **kwargs)
    if getattr(query, 'provider', None) == 'fmi' and getattr(query, 'resolution', None) == 'daily':
        return download_daily_observations_fmi(*args, **kwargs)
    raise NotImplementedError('Finland national provider support currently implements only fmi/1hour and fmi/daily.')


_read_station_metadata = build_mixed_station_metadata_reader(
    read_national_station_metadata=read_station_metadata_fmi,
    read_ghcnd_station_metadata=read_station_metadata_ghcnd,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
)
_read_station_observation_metadata = build_mixed_station_observation_metadata_reader(
    read_national_station_observation_metadata=read_station_observation_metadata_fmi,
    read_ghcnd_station_observation_metadata=read_station_observation_metadata_ghcnd,
)
_download_observations = build_mixed_observation_downloader(
    download_national_observations=_download_national_observations,
    download_ghcnd_observations=download_daily_observations_ghcnd,
)


PROVIDER = WeatherProvider(
    country_code='FI',
    name='FMI Open Data + NOAA GHCN-Daily',
    read_station_metadata=_read_station_metadata,
    read_station_observation_metadata=_read_station_observation_metadata,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('FI',),
    supported_providers=('fmi', 'ghcnd'),
    supported_resolutions=('daily', '1hour'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
