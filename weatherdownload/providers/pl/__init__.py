from __future__ import annotations

from .metadata import read_station_metadata_pl, read_station_observation_metadata_pl
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'sunshine_duration',
    'wind_speed',
    'wind_speed_max',
    'relative_humidity',
    'vapour_pressure',
    'pressure',
)



def _download_observations(*args, **kwargs):
    from ...observations import _download_observations_pl

    return _download_observations_pl(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='PL',
    name='IMGW-PIB Poland synop and klimat archives',
    read_station_metadata=read_station_metadata_pl,
    read_station_observation_metadata=read_station_observation_metadata_pl,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('PL',),
    supported_dataset_scopes=('historical', 'historical_klimat'),
    supported_resolutions=('1hour', 'daily'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
