from __future__ import annotations

from .metadata import (
    read_station_metadata_ghcnd,
    read_station_metadata_ipma,
    read_station_observation_metadata_ghcnd,
    read_station_observation_metadata_ipma,
)
from .observations import download_daily_observations_ghcnd, download_hourly_observations_ipma
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
    'relative_humidity',
    'solar_radiation',
    'snow_depth',
)


def _download_national_observations(*args, **kwargs):
    query = args[0] if args else kwargs.get('query')
    if getattr(query, 'provider', None) == 'ipma' and getattr(query, 'resolution', None) == '1hour':
        return download_hourly_observations_ipma(*args, **kwargs)
    raise NotImplementedError('Portugal national provider support currently implements only ipma/1hour.')


_read_station_metadata = build_mixed_station_metadata_reader(
    read_national_station_metadata=read_station_metadata_ipma,
    read_ghcnd_station_metadata=read_station_metadata_ghcnd,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
)
_read_station_observation_metadata = build_mixed_station_observation_metadata_reader(
    read_national_station_observation_metadata=read_station_observation_metadata_ipma,
    read_ghcnd_station_observation_metadata=read_station_observation_metadata_ghcnd,
)
_download_observations = build_mixed_observation_downloader(
    download_national_observations=_download_national_observations,
    download_ghcnd_observations=download_daily_observations_ghcnd,
)


PROVIDER = WeatherProvider(
    country_code='PT',
    name='IPMA recent hourly + NOAA GHCN-Daily',
    read_station_metadata=_read_station_metadata,
    read_station_observation_metadata=_read_station_observation_metadata,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('PT',),
    supported_providers=('ghcnd', 'ipma'),
    supported_resolutions=('daily', '1hour'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
)
