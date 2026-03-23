from __future__ import annotations

from ..geosphere_metadata import read_station_metadata_geosphere, read_station_observation_metadata_geosphere
from ..geosphere_registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
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
    from ..observations import _download_observations_geosphere

    return _download_observations_geosphere(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='AT',
    name='GeoSphere Austria',
    read_station_metadata=read_station_metadata_geosphere,
    read_station_observation_metadata=read_station_observation_metadata_geosphere,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('AT',),
    supported_dataset_scopes=('historical',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
