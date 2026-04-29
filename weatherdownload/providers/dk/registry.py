from __future__ import annotations

from dataclasses import dataclass

from ..ghcnd.registry import GhcndDatasetSpec
from .ghcnd import (
    get_dataset_spec as get_ghcnd_dataset_spec,
    list_dataset_specs as list_ghcnd_dataset_specs,
    list_implemented_dataset_specs as list_ghcnd_implemented_dataset_specs,
)


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
DMI_METOBS_API_BASE = 'https://opendataapi.dmi.dk/v2/metObs/collections'
DMI_METOBS_OBSERVATION_URL = f'{DMI_METOBS_API_BASE}/observation/items'
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

_DK_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('mean_temp',),
    'precipitation': ('acc_precip',),
    'wind_speed': ('mean_wind_speed',),
    'relative_humidity': ('mean_relative_hum',),
    'pressure': ('mean_pressure',),
    'sunshine_duration': ('bright_sunshine',),
}

_DK_TENMIN_CANONICAL_ELEMENTS = {
    'tas_mean': ('temp_dry',),
    'precipitation': ('precip_past10min',),
    'wind_speed': ('wind_speed',),
    'relative_humidity': ('humidity',),
    'pressure': ('pressure',),
    'sunshine_duration': ('sun_last10min_glob',),
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

DK_HOURLY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'mean_temp': {
        'name': 'Hourly mean temperature',
        'description': 'Official DMI Climate Data hourly mean temperature in degrees Celsius from the stationValue collection.',
    },
    'acc_precip': {
        'name': 'Hourly accumulated precipitation',
        'description': 'Official DMI Climate Data hourly accumulated precipitation in millimetres from the stationValue collection.',
    },
    'mean_wind_speed': {
        'name': 'Hourly mean wind speed',
        'description': 'Official DMI Climate Data hourly mean wind speed in metres per second from the stationValue collection.',
    },
    'mean_relative_hum': {
        'name': 'Hourly mean relative humidity',
        'description': 'Official DMI Climate Data hourly mean relative humidity in percent from the stationValue collection.',
    },
    'mean_pressure': {
        'name': 'Hourly mean pressure',
        'description': 'Official DMI Climate Data hourly mean pressure in hPa from the stationValue collection.',
    },
    'bright_sunshine': {
        'name': 'Hourly bright sunshine duration',
        'description': 'Official DMI Climate Data hourly bright sunshine duration in minutes from the stationValue collection.',
    },
}

DK_TENMIN_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'temp_dry': {
        'name': 'Present air temperature',
        'description': 'Official DMI Meteorological Observation API 10-minute present air temperature in degrees Celsius.',
    },
    'precip_past10min': {
        'name': 'Accumulated precipitation in the latest 10 minutes',
        'description': 'Official DMI Meteorological Observation API accumulated precipitation in millimetres for the latest 10 minutes.',
    },
    'wind_speed': {
        'name': 'Latest 10 minutes mean wind speed',
        'description': 'Official DMI Meteorological Observation API latest 10 minutes mean wind speed in metres per second.',
    },
    'humidity': {
        'name': 'Present relative humidity',
        'description': 'Official DMI Meteorological Observation API present relative humidity in percent.',
    },
    'pressure': {
        'name': 'Atmospheric pressure at station level',
        'description': 'Official DMI Meteorological Observation API atmospheric pressure at station level in hPa.',
    },
    'sun_last10min_glob': {
        'name': 'Number of minutes with sunshine in the latest 10 minutes',
        'description': 'Official DMI Meteorological Observation API sunshine duration in minutes for the latest 10 minutes.',
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
    DenmarkDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        label='DMI Climate Data historical hourly station observations',
        metadata_url=f'{DMI_CLIMATE_STATION_URL}?limit=300000',
        data_url=DMI_CLIMATE_STATION_VALUE_URL,
        supported_elements=(
            'mean_temp',
            'acc_precip',
            'mean_wind_speed',
            'mean_relative_hum',
            'mean_pressure',
            'bright_sunshine',
        ),
        canonical_elements=_DK_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    DenmarkDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        label='DMI Meteorological Observation API historical 10-minute station observations',
        metadata_url=f'{DMI_CLIMATE_STATION_URL}?limit=300000',
        data_url=DMI_METOBS_OBSERVATION_URL,
        supported_elements=(
            'temp_dry',
            'precip_past10min',
            'wind_speed',
            'humidity',
            'pressure',
            'sun_last10min_glob',
        ),
        canonical_elements=_DK_TENMIN_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[DenmarkDatasetSpec]:
    return [*list(_DK_DATASET_SPECS), *list_ghcnd_dataset_specs()]


def list_implemented_dataset_specs() -> list[DenmarkDatasetSpec]:
    return [
        *(spec for spec in _DK_DATASET_SPECS if spec.implemented),
        *list_ghcnd_implemented_dataset_specs(),
    ]


def get_dataset_spec(dataset_scope: str, resolution: str) -> DenmarkDatasetSpec | GhcndDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    if normalized_scope == 'ghcnd':
        return get_ghcnd_dataset_spec(normalized_scope, normalized_resolution)
    for spec in _DK_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported DMI Denmark dataset combination: {dataset_scope}/{resolution}')
