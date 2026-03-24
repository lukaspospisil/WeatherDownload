from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class KnmiDatasetSpec:
    dataset_scope: str
    resolution: str
    dataset_name: str
    dataset_version: str
    label: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


KNMI_STATION_DATASET_NAME = 'waarneemstations_csv'
KNMI_STATION_DATASET_VERSION = '1.0'
KNMI_OPEN_DATA_BASE_URL = 'https://api.dataplatform.knmi.nl/open-data/v1'
KNMI_DAILY_FILENAME_PREFIX = 'daily-observations-'

_KNMI_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('TG',),
    'tas_max': ('TX',),
    'tas_min': ('TN',),
    'precipitation': ('RH',),
    'sunshine_duration': ('SQ',),
    'wind_speed': ('FG',),
    'pressure': ('PG',),
    'relative_humidity': ('UG',),
}

KNMI_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'TG': {
        'name': 'Daily mean air temperature',
        'description': 'Daily mean air temperature at 1.50 meters, in degrees Celsius.',
    },
    'TX': {
        'name': 'Daily maximum air temperature',
        'description': 'Daily maximum air temperature at 1.50 meters, in degrees Celsius.',
    },
    'TN': {
        'name': 'Daily minimum air temperature',
        'description': 'Daily minimum air temperature at 1.50 meters, in degrees Celsius.',
    },
    'RH': {
        'name': 'Daily precipitation amount',
        'description': 'Daily precipitation amount, in millimeters.',
    },
    'SQ': {
        'name': 'Daily sunshine duration',
        'description': 'Daily sunshine duration in hours, calculated from global solar radiation.',
    },
    'FG': {
        'name': 'Daily mean wind speed',
        'description': 'Daily mean wind speed, representative for 10 meters, in meters per second.',
    },
    'PG': {
        'name': 'Daily mean sea level pressure',
        'description': 'Daily mean sea level pressure, in hectopascal.',
    },
    'UG': {
        'name': 'Daily mean relative humidity',
        'description': 'Daily mean relative atmospheric humidity, in percent.',
    },
}

_KNMI_DATASET_SPECS = [
    KnmiDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        dataset_name='daily-in-situ-meteorological-observations-validated',
        dataset_version='1.0',
        label='KNMI validated daily in-situ meteorological observations',
        supported_elements=('TG', 'TX', 'TN', 'RH', 'SQ', 'FG', 'PG', 'UG'),
        canonical_elements=_KNMI_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[KnmiDatasetSpec]:
    return list(_KNMI_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[KnmiDatasetSpec]:
    return [spec for spec in _KNMI_DATASET_SPECS if spec.implemented]


def get_dataset_spec(dataset_scope: str, resolution: str) -> KnmiDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _KNMI_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported KNMI dataset combination: {dataset_scope}/{resolution}')
