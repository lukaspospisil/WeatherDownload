from __future__ import annotations

from ..dk_metadata import read_station_metadata_dk, read_station_observation_metadata_dk
from ..dk_registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from .base import WeatherProvider

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


def _download_observations(*args, **kwargs):
    from ..observations import _download_observations_dk

    return _download_observations_dk(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='DK',
    name='DMI Denmark',
    read_station_metadata=read_station_metadata_dk,
    read_station_observation_metadata=read_station_observation_metadata_dk,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('DK',),
    supported_dataset_scopes=('historical',),
    supported_resolutions=('1hour', 'daily'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
