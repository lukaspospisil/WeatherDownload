from __future__ import annotations

from ..ch.registry import (
    CH_DATA_INVENTORY_URL,
    CH_ITEM_URL_TEMPLATE,
    CH_PARAMETER_METADATA_URL,
    CH_STATION_METADATA_URL,
    SwitzerlandDatasetSpec,
)


LI_METEOSWISS_STATION_IDS = frozenset({'VAD'})

LI_DAILY_CANONICAL_ELEMENTS = {
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
    'solar_radiation': ('gre000d0',),
}


def _parameter_metadata(raw_code: str, name: str, description: str) -> dict[str, str]:
    return {
        'name': name,
        'description': description,
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D MeteoSwiss A1 daily',
        'raw_code': raw_code,
    }


LI_DAILY_PARAMETER_METADATA = {
    'tre200d0': _parameter_metadata('tre200d0', 'Daily mean air temperature', 'Official MeteoSwiss A1 daily mean air temperature at 2 m above ground for Vaduz, Liechtenstein.'),
    'tre200dx': _parameter_metadata('tre200dx', 'Daily maximum air temperature', 'Official MeteoSwiss A1 daily maximum air temperature at 2 m above ground for Vaduz, Liechtenstein.'),
    'tre200dn': _parameter_metadata('tre200dn', 'Daily minimum air temperature', 'Official MeteoSwiss A1 daily minimum air temperature at 2 m above ground for Vaduz, Liechtenstein.'),
    'rre150d0': _parameter_metadata('rre150d0', 'Daily precipitation total', 'Official MeteoSwiss A1 daily precipitation total for the documented 6 UTC to 6 UTC following-day window for Vaduz, Liechtenstein.'),
    'fkl010d0': _parameter_metadata('fkl010d0', 'Daily mean wind speed', 'Official MeteoSwiss A1 daily mean scalar wind speed for Vaduz, Liechtenstein.'),
    'fkl010d1': _parameter_metadata('fkl010d1', 'Daily maximum gust speed', 'Official MeteoSwiss A1 daily maximum one-second gust speed for Vaduz, Liechtenstein.'),
    'ure200d0': _parameter_metadata('ure200d0', 'Daily mean relative humidity', 'Official MeteoSwiss A1 daily mean relative humidity at 2 m above ground for Vaduz, Liechtenstein.'),
    'pva200d0': _parameter_metadata('pva200d0', 'Daily mean vapour pressure', 'Official MeteoSwiss A1 daily mean vapour pressure at 2 m above ground for Vaduz, Liechtenstein.'),
    'prestad0': _parameter_metadata('prestad0', 'Daily mean station pressure', 'Official MeteoSwiss A1 daily mean atmospheric pressure at barometric altitude (QFE) for Vaduz, Liechtenstein.'),
    'sre000d0': _parameter_metadata('sre000d0', 'Daily sunshine duration', 'Official MeteoSwiss A1 daily sunshine duration total for Vaduz, Liechtenstein.'),
    'gre000d0': _parameter_metadata('gre000d0', 'Daily mean global radiation', 'Official MeteoSwiss A1 daily mean global radiation for Vaduz, Liechtenstein.'),
}

_LI_DATASET_SPECS = [
    SwitzerlandDatasetSpec(
        provider='meteoswiss',
        resolution='daily',
        label='MeteoSwiss A1 Liechtenstein daily station observations',
        station_metadata_url=CH_STATION_METADATA_URL,
        parameter_metadata_url=CH_PARAMETER_METADATA_URL,
        data_inventory_url=CH_DATA_INVENTORY_URL,
        item_url_template=CH_ITEM_URL_TEMPLATE,
        supported_elements=tuple(LI_DAILY_PARAMETER_METADATA),
        canonical_elements=LI_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[SwitzerlandDatasetSpec]:
    return list(_LI_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[SwitzerlandDatasetSpec]:
    return list(_LI_DATASET_SPECS)


def get_dataset_spec(provider: str, resolution: str) -> SwitzerlandDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    for spec in _LI_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported MeteoSwiss Liechtenstein dataset combination: {provider}/{resolution}')
