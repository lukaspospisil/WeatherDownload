from __future__ import annotations

from ..chmi_registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..metadata import _read_station_metadata_chmi, _read_station_observation_metadata_chmi
from .base import WeatherProvider


def _download_observations(*args, **kwargs):
    from ..observations import _download_observations_chmi

    return _download_observations_chmi(*args, **kwargs)


PROVIDER = WeatherProvider(
    country_code='CZ',
    name='CHMI',
    read_station_metadata=_read_station_metadata_chmi,
    read_station_observation_metadata=_read_station_observation_metadata_chmi,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
)
