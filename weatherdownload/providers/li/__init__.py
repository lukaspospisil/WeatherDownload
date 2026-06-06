from __future__ import annotations

from .daily import download_daily_observations_li
from .metadata import read_station_metadata_li, read_station_observation_metadata_li
from .registry import (
    LI_DAILY_CANONICAL_ELEMENTS,
    get_dataset_spec,
    list_dataset_specs,
    list_implemented_dataset_specs,
)
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = tuple(LI_DAILY_CANONICAL_ELEMENTS)

PROVIDER = WeatherProvider(
    country_code='LI',
    name='MeteoSwiss Liechtenstein A1',
    read_station_metadata=read_station_metadata_li,
    read_station_observation_metadata=read_station_observation_metadata_li,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=download_daily_observations_li,
    supported_country_codes=('LI',),
    supported_providers=('meteoswiss',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
