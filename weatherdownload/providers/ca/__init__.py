from __future__ import annotations

from .metadata import (
    read_station_metadata_eccc,
    read_station_metadata_ghcnd,
    read_station_observation_metadata_eccc,
    read_station_observation_metadata_ghcnd,
)
from .hourly import download_hourly_observations_eccc
from .observations import download_daily_observations_eccc, download_daily_observations_ghcnd
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
    'relative_humidity',
    'snow_depth',
)


def _download_national_observations(*args, **kwargs):
    query = args[0] if args else kwargs.get('query')
    if getattr(query, 'resolution', None) == '1hour':
        return download_hourly_observations_eccc(*args, **kwargs)
    return download_daily_observations_eccc(*args, **kwargs)


_read_station_metadata = build_mixed_station_metadata_reader(
    read_national_station_metadata=read_station_metadata_eccc,
    read_ghcnd_station_metadata=read_station_metadata_ghcnd,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
)
_read_station_observation_metadata = build_mixed_station_observation_metadata_reader(
    read_national_station_observation_metadata=read_station_observation_metadata_eccc,
    read_ghcnd_station_observation_metadata=read_station_observation_metadata_ghcnd,
)
_download_observations = build_mixed_observation_downloader(
    download_national_observations=_download_national_observations,
    download_ghcnd_observations=download_daily_observations_ghcnd,
)


PROVIDER = WeatherProvider(
    country_code='CA',
    name='ECCC GeoMet + NOAA GHCN-Daily',
    read_station_metadata=_read_station_metadata,
    read_station_observation_metadata=_read_station_observation_metadata,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('CA',),
    supported_providers=('eccc', 'ghcnd'),
    supported_resolutions=('daily', '1hour'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
