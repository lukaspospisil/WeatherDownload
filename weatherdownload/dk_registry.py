from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DenmarkDatasetSpec:
    dataset_scope: str
    resolution: str
    label: str
    metadata_url: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


DMI_CLIMATE_API_BASE = 'https://opendataapi.dmi.dk/v2/climateData/collections'
DMI_CLIMATE_STATION_URL = f'{DMI_CLIMATE_API_BASE}/station/items'
DMI_CLIMATE_STATION_VALUE_URL = f'{DMI_CLIMATE_API_BASE}/stationValue/items'
DMI_DENMARK_COUNTRY_CODE = 'DNK'

_DK_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('mean_temp',),
    'tas_max': ('mean_daily_max_temp',),
    'tas_min': ('mean_daily_min_temp',),
    'precipitation': ('acc_precip',),
    'wind_speed': ('mean_wind_speed',),
    'relative_humidity': ('mean_relative_hum',),
    'pressure': ('mean_pressure',),
    'sunshine_duration': ('bright_sunshine',),
}

DK_DAILY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'mean_temp': {
        'name': 'Daily mean temperature',
        'description': 'Official DMI Climate Data daily mean temperature in degrees Celsius from the stationValue collection.',
    },
    'mean_daily_max_temp': {
        'name': 'Daily mean of daily maximum temperature',
        'description': 'Official DMI Climate Data daily mean of daily maximum temperature in degrees Celsius from the stationValue collection.',
    },
    'mean_daily_min_temp': {
        'name': 'Daily mean of daily minimum temperature',
        'description': 'Official DMI Climate Data daily mean of daily minimum temperature in degrees Celsius from the stationValue collection.',
    },
    'acc_precip': {
        'name': 'Daily accumulated precipitation',
        'description': 'Official DMI Climate Data daily accumulated precipitation in millimetres from the stationValue collection.',
    },
    'mean_wind_speed': {
        'name': 'Daily mean wind speed',
        'description': 'Official DMI Climate Data daily mean wind speed in metres per second from the stationValue collection.',
    },
    'mean_relative_hum': {
        'name': 'Daily mean relative humidity',
        'description': 'Official DMI Climate Data daily mean relative humidity in percent from the stationValue collection.',
    },
    'mean_pressure': {
        'name': 'Daily mean pressure',
        'description': 'Official DMI Climate Data daily mean pressure in hPa from the stationValue collection.',
    },
    'bright_sunshine': {
        'name': 'Daily bright sunshine duration',
        'description': 'Official DMI Climate Data daily bright sunshine duration in hours from the stationValue collection.',
    },
}

_DK_DATASET_SPECS = [
    DenmarkDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        label='DMI Climate Data historical daily station observations',
        metadata_url=f'{DMI_CLIMATE_STATION_URL}?limit=300000',
        data_url=DMI_CLIMATE_STATION_VALUE_URL,
        supported_elements=(
            'mean_temp',
            'mean_daily_max_temp',
            'mean_daily_min_temp',
            'acc_precip',
            'mean_wind_speed',
            'mean_relative_hum',
            'mean_pressure',
            'bright_sunshine',
        ),
        canonical_elements=_DK_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[DenmarkDatasetSpec]:
    return list(_DK_DATASET_SPECS)



def list_implemented_dataset_specs() -> list[DenmarkDatasetSpec]:
    return [spec for spec in _DK_DATASET_SPECS if spec.implemented]



def get_dataset_spec(dataset_scope: str, resolution: str) -> DenmarkDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _DK_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported DMI Denmark dataset combination: {dataset_scope}/{resolution}')
