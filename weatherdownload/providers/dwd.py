from __future__ import annotations

import pandas as pd

from .base import WeatherProvider


def _not_implemented(*args, **kwargs):
    raise NotImplementedError('The DWD provider is not implemented yet.')


def _list_dataset_specs() -> list[object]:
    return []


def _list_implemented_dataset_specs() -> list[object]:
    return []


PROVIDER = WeatherProvider(
    country_code='DE',
    name='DWD',
    read_station_metadata=_not_implemented,
    read_station_observation_metadata=_not_implemented,
    list_dataset_specs=_list_dataset_specs,
    list_implemented_dataset_specs=_list_implemented_dataset_specs,
    get_dataset_spec=_not_implemented,
    download_observations=_not_implemented,
)
