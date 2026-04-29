from __future__ import annotations

from dataclasses import dataclass

from ..ghcnd.registry import GhcndDatasetSpec
from .ghcnd import (
    get_dataset_spec as get_ghcnd_dataset_spec,
    list_dataset_specs as list_ghcnd_dataset_specs,
    list_implemented_dataset_specs as list_ghcnd_implemented_dataset_specs,
)


@dataclass(frozen=True)
class SwitzerlandDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    parameter_metadata_url: str
    data_inventory_url: str
    item_url_template: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


CH_COLLECTION_ID = 'ch.meteoschweiz.ogd-smn'
CH_STAC_BASE_URL = 'https://data.geo.admin.ch/api/stac/v1/collections'
CH_COLLECTION_URL = f'{CH_STAC_BASE_URL}/{CH_COLLECTION_ID}'
CH_ITEMS_URL = f'{CH_COLLECTION_URL}/items'
CH_ITEM_URL_TEMPLATE = f'{CH_ITEMS_URL}/{{station_id}}'
CH_METADATA_BASE_URL = f'https://data.geo.admin.ch/{CH_COLLECTION_ID}'
CH_STATION_METADATA_URL = f'{CH_METADATA_BASE_URL}/ogd-smn_meta_stations.csv'
CH_PARAMETER_METADATA_URL = f'{CH_METADATA_BASE_URL}/ogd-smn_meta_parameters.csv'
CH_DATA_INVENTORY_URL = f'{CH_METADATA_BASE_URL}/ogd-smn_meta_datainventory.csv'

CH_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('tre200d0',),
    'tas_max': ('tre200dx',),
    'tas_min': ('tre200dn',),
    'precipitation': ('rre150d0',),
    'wind_speed': ('fkl010d0',),
    'wind_speed_max': ('fkl010d1',),
    'relative_humidity': ('ure200d0',),
    'vapour_pressure': ('pva200d0',),
    'pressure': ('prestad0',),
    'sunshine_duration': ('sre000d0',),
}

CH_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('tre200h0',),
    'precipitation': ('rre150h0',),
    'wind_speed': ('fkl010h0',),
    'wind_speed_max': ('fkl010h1',),
    'relative_humidity': ('ure200h0',),
    'vapour_pressure': ('pva200h0',),
    'pressure': ('prestah0',),
    'sunshine_duration': ('sre000h0',),
}

CH_TENMIN_CANONICAL_ELEMENTS = {
    'tas_mean': ('tre200s0',),
    'precipitation': ('rre150z0',),
    'wind_speed': ('fkl010z0',),
    'wind_speed_max': ('fkl010z1',),
    'relative_humidity': ('ure200s0',),
    'vapour_pressure': ('pva200s0',),
    'pressure': ('prestas0',),
    'sunshine_duration': ('sre000z0',),
}


def _parameter_metadata(raw_code: str, name: str, description: str, obs_type: str, schedule: str) -> dict[str, str]:
    return {
        'name': name,
        'description': description,
        'obs_type': obs_type,
        'schedule': schedule,
        'raw_code': raw_code,
    }


CH_DAILY_PARAMETER_METADATA = {
    'tre200d0': _parameter_metadata('tre200d0', 'Daily mean air temperature', 'Official MeteoSwiss A1 daily mean air temperature at 2 m above ground.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'tre200dx': _parameter_metadata('tre200dx', 'Daily maximum air temperature', 'Official MeteoSwiss A1 daily maximum air temperature at 2 m above ground.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'tre200dn': _parameter_metadata('tre200dn', 'Daily minimum air temperature', 'Official MeteoSwiss A1 daily minimum air temperature at 2 m above ground.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'rre150d0': _parameter_metadata('rre150d0', 'Daily precipitation total', 'Official MeteoSwiss A1 daily precipitation total for the documented 6 UTC to 6 UTC following-day window.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'fkl010d0': _parameter_metadata('fkl010d0', 'Daily mean wind speed', 'Official MeteoSwiss A1 daily mean scalar wind speed.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'fkl010d1': _parameter_metadata('fkl010d1', 'Daily maximum gust speed', 'Official MeteoSwiss A1 daily maximum one-second gust speed.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'ure200d0': _parameter_metadata('ure200d0', 'Daily mean relative humidity', 'Official MeteoSwiss A1 daily mean relative humidity at 2 m above ground.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'pva200d0': _parameter_metadata('pva200d0', 'Daily mean vapour pressure', 'Official MeteoSwiss A1 daily mean vapour pressure at 2 m above ground.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'prestad0': _parameter_metadata('prestad0', 'Daily mean station pressure', 'Official MeteoSwiss A1 daily mean atmospheric pressure at barometric altitude (QFE).', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
    'sre000d0': _parameter_metadata('sre000d0', 'Daily sunshine duration', 'Official MeteoSwiss A1 daily sunshine duration total.', 'HISTORICAL_DAILY', 'P1D MeteoSwiss A1 daily'),
}

CH_HOURLY_PARAMETER_METADATA = {
    'tre200h0': _parameter_metadata('tre200h0', 'Hourly mean air temperature', 'Official MeteoSwiss A1 hourly mean air temperature at 2 m above ground.', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
    'rre150h0': _parameter_metadata('rre150h0', 'Hourly precipitation total', 'Official MeteoSwiss A1 hourly precipitation total.', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
    'fkl010h0': _parameter_metadata('fkl010h0', 'Hourly mean wind speed', 'Official MeteoSwiss A1 hourly mean scalar wind speed.', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
    'fkl010h1': _parameter_metadata('fkl010h1', 'Hourly maximum gust speed', 'Official MeteoSwiss A1 hourly maximum one-second gust speed.', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
    'ure200h0': _parameter_metadata('ure200h0', 'Hourly mean relative humidity', 'Official MeteoSwiss A1 hourly mean relative humidity at 2 m above ground.', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
    'pva200h0': _parameter_metadata('pva200h0', 'Hourly mean vapour pressure', 'Official MeteoSwiss A1 hourly mean vapour pressure at 2 m above ground.', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
    'prestah0': _parameter_metadata('prestah0', 'Hourly mean station pressure', 'Official MeteoSwiss A1 hourly mean atmospheric pressure at barometric altitude (QFE).', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
    'sre000h0': _parameter_metadata('sre000h0', 'Hourly sunshine duration', 'Official MeteoSwiss A1 hourly sunshine duration total.', 'HISTORICAL_HOURLY', 'PT1H MeteoSwiss A1 hourly'),
}

CH_TENMIN_PARAMETER_METADATA = {
    'tre200s0': _parameter_metadata('tre200s0', '10-minute air temperature', 'Official MeteoSwiss A1 10-minute air temperature at 2 m above ground.', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
    'rre150z0': _parameter_metadata('rre150z0', '10-minute precipitation total', 'Official MeteoSwiss A1 10-minute precipitation total.', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
    'fkl010z0': _parameter_metadata('fkl010z0', '10-minute mean wind speed', 'Official MeteoSwiss A1 10-minute mean scalar wind speed.', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
    'fkl010z1': _parameter_metadata('fkl010z1', '10-minute maximum gust speed', 'Official MeteoSwiss A1 10-minute maximum one-second gust speed.', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
    'ure200s0': _parameter_metadata('ure200s0', '10-minute relative humidity', 'Official MeteoSwiss A1 10-minute relative humidity at 2 m above ground.', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
    'pva200s0': _parameter_metadata('pva200s0', '10-minute vapour pressure', 'Official MeteoSwiss A1 10-minute vapour pressure at 2 m above ground.', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
    'prestas0': _parameter_metadata('prestas0', '10-minute station pressure', 'Official MeteoSwiss A1 10-minute atmospheric pressure at barometric altitude (QFE).', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
    'sre000z0': _parameter_metadata('sre000z0', '10-minute sunshine duration', 'Official MeteoSwiss A1 10-minute sunshine duration total.', 'HISTORICAL_10MIN', 'PT10M MeteoSwiss A1 10-minute'),
}

_CH_DATASET_SPECS = [
    SwitzerlandDatasetSpec(
        provider='historical',
        resolution='daily',
        label='MeteoSwiss A1 historical daily station observations',
        station_metadata_url=CH_STATION_METADATA_URL,
        parameter_metadata_url=CH_PARAMETER_METADATA_URL,
        data_inventory_url=CH_DATA_INVENTORY_URL,
        item_url_template=CH_ITEM_URL_TEMPLATE,
        supported_elements=tuple(CH_DAILY_PARAMETER_METADATA),
        canonical_elements=CH_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
    SwitzerlandDatasetSpec(
        provider='historical',
        resolution='1hour',
        label='MeteoSwiss A1 historical hourly station observations',
        station_metadata_url=CH_STATION_METADATA_URL,
        parameter_metadata_url=CH_PARAMETER_METADATA_URL,
        data_inventory_url=CH_DATA_INVENTORY_URL,
        item_url_template=CH_ITEM_URL_TEMPLATE,
        supported_elements=tuple(CH_HOURLY_PARAMETER_METADATA),
        canonical_elements=CH_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    SwitzerlandDatasetSpec(
        provider='historical',
        resolution='10min',
        label='MeteoSwiss A1 historical 10-minute station observations',
        station_metadata_url=CH_STATION_METADATA_URL,
        parameter_metadata_url=CH_PARAMETER_METADATA_URL,
        data_inventory_url=CH_DATA_INVENTORY_URL,
        item_url_template=CH_ITEM_URL_TEMPLATE,
        supported_elements=tuple(CH_TENMIN_PARAMETER_METADATA),
        canonical_elements=CH_TENMIN_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[SwitzerlandDatasetSpec]:
    return [*list(_CH_DATASET_SPECS), *list_ghcnd_dataset_specs()]


def list_implemented_dataset_specs() -> list[SwitzerlandDatasetSpec]:
    return [
        *(spec for spec in _CH_DATASET_SPECS if spec.implemented),
        *list_ghcnd_implemented_dataset_specs(),
    ]


def get_dataset_spec(provider: str, resolution: str) -> SwitzerlandDatasetSpec | GhcndDatasetSpec:
    normalized_scope = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_scope == 'ghcnd':
        return get_ghcnd_dataset_spec(normalized_scope, normalized_resolution)
    for spec in _CH_DATASET_SPECS:
        if spec.provider == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported MeteoSwiss Switzerland dataset combination: {provider}/{resolution}')
