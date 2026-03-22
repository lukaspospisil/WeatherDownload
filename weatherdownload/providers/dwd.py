from __future__ import annotations

from ..dwd_metadata import read_station_metadata_dwd, read_station_observation_metadata_dwd
from ..dwd_registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from .base import WeatherProvider


def _not_implemented(*args, **kwargs):
    raise NotImplementedError('The DWD observations downloader is not implemented yet.')


PROVIDER = WeatherProvider(
    country_code='DE',
    name='DWD',
    read_station_metadata=read_station_metadata_dwd,
    read_station_observation_metadata=read_station_observation_metadata_dwd,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_not_implemented,
)
