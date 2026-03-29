from __future__ import annotations

from .metadata import read_station_metadata_be, read_station_observation_metadata_be
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'wind_speed',
    'relative_humidity',
    'pressure',
    'sunshine_duration',
)


def _download_be_observations(*args, **kwargs):
    from ...observations import _download_observations_be

    return _download_observations_be(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='BE',
    name='RMI/KMI Belgium',
    read_station_metadata=read_station_metadata_be,
    read_station_observation_metadata=read_station_observation_metadata_be,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_be_observations,
    supported_country_codes=('BE',),
    supported_dataset_scopes=('historical',),
    supported_resolutions=('daily', '1hour', '10min'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)

