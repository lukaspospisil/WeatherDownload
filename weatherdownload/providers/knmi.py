from __future__ import annotations

from ..knmi_metadata import read_station_metadata_knmi, read_station_observation_metadata_knmi
from ..knmi_registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from .base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'sunshine_duration',
    'wind_speed',
    'pressure',
    'relative_humidity',
)


def _download_observations(*args, **kwargs):
    from ..observations import _download_observations_knmi

    return _download_observations_knmi(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='NL',
    name='KNMI',
    read_station_metadata=read_station_metadata_knmi,
    read_station_observation_metadata=read_station_observation_metadata_knmi,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('NL',),
    supported_dataset_scopes=('historical',),
    supported_resolutions=('10min', '1hour', 'daily'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)


