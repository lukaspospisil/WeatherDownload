from __future__ import annotations

from .metadata import read_station_metadata_dwd, read_station_observation_metadata_dwd
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider


def _download_observations(*args, **kwargs):
    from ...observations import _download_observations_dwd

    return _download_observations_dwd(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='DE',
    name='DWD',
    read_station_metadata=read_station_metadata_dwd,
    read_station_observation_metadata=read_station_observation_metadata_dwd,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
)

