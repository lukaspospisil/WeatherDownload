from __future__ import annotations

from .daily import download_daily_observations_ad
from .metadata import read_station_metadata_ad, read_station_observation_metadata_ad
from .registry import AD_DAILY_CANONICAL_ELEMENTS, get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ..base import WeatherProvider

SUPPORTED_CANONICAL_ELEMENTS = tuple(AD_DAILY_CANONICAL_ELEMENTS)

PROVIDER = WeatherProvider(
    country_code='AD',
    name='Meteo.ad Andorra',
    read_station_metadata=read_station_metadata_ad,
    read_station_observation_metadata=read_station_observation_metadata_ad,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=download_daily_observations_ad,
    supported_country_codes=('AD',),
    supported_providers=('meteo_ad',),
    supported_resolutions=('daily',),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
    experimental=False,
)
