from __future__ import annotations

from .metadata import read_station_metadata_ch, read_station_observation_metadata_ch
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'precipitation',
    'wind_speed',
    'wind_speed_max',
    'relative_humidity',
    'vapour_pressure',
    'pressure',
    'sunshine_duration',
)


def _download_observations(*args, **kwargs):
    from ...observations import _download_observations_ch as download_observations_ch

    return download_observations_ch(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='CH',
    name='MeteoSwiss Switzerland A1',
    read_station_metadata=read_station_metadata_ch,
    read_station_observation_metadata=read_station_observation_metadata_ch,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('CH',),
    supported_dataset_scopes=('historical',),
    supported_resolutions=('daily', '1hour', '10min'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)

