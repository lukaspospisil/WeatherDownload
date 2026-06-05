from __future__ import annotations

from dataclasses import dataclass

from ..ghcnd.registry import (
    GHCND_STANDARD_CANONICAL_ELEMENTS,
    GhcndDatasetSpec,
    build_country_dataset_specs,
    get_country_dataset_spec,
    list_country_dataset_specs,
    list_country_implemented_dataset_specs,
)


@dataclass(frozen=True)
class KnmiDatasetSpec:
    provider: str
    resolution: str
    label: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


KNMI_DAILY_DATA_URL = 'https://www.daggegevens.knmi.nl/klimatologie/daggegevens'

KNMI_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('TG',),
    'tas_max': ('TX',),
    'tas_min': ('TN',),
    'precipitation': ('RH',),
    'wind_speed': ('FG',),
    'wind_speed_max': ('FXX',),
    'relative_humidity': ('UG',),
    'pressure': ('PG',),
    'sunshine_duration': ('SQ',),
    'solar_radiation': ('Q',),
}

KNMI_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'TG': {
        'name': 'Daily mean air temperature',
        'description': 'Official KNMI daily mean air temperature in 0.1 degrees Celsius, converted to degrees Celsius.',
        'unit': 'degC',
        'source_unit': '0.1 degC',
    },
    'TX': {
        'name': 'Daily maximum air temperature',
        'description': 'Official KNMI daily maximum air temperature in 0.1 degrees Celsius, converted to degrees Celsius.',
        'unit': 'degC',
        'source_unit': '0.1 degC',
    },
    'TN': {
        'name': 'Daily minimum air temperature',
        'description': 'Official KNMI daily minimum air temperature in 0.1 degrees Celsius, converted to degrees Celsius.',
        'unit': 'degC',
        'source_unit': '0.1 degC',
    },
    'RH': {
        'name': 'Daily precipitation amount',
        'description': 'Official KNMI daily precipitation amount in 0.1 millimetres; coded -1 is normalized to observed 0.0 mm for trace precipitation below 0.05 mm.',
        'unit': 'mm',
        'source_unit': '0.1 mm',
    },
    'FG': {
        'name': 'Daily mean wind speed',
        'description': 'Official KNMI daily mean wind speed in 0.1 m/s, converted to m/s.',
        'unit': 'm/s',
        'source_unit': '0.1 m/s',
    },
    'FXX': {
        'name': 'Daily maximum wind gust',
        'description': 'Official KNMI daily maximum wind gust in 0.1 m/s, converted to m/s.',
        'unit': 'm/s',
        'source_unit': '0.1 m/s',
    },
    'UG': {
        'name': 'Daily mean relative humidity',
        'description': 'Official KNMI daily mean relative humidity in percent.',
        'unit': '%',
        'source_unit': '%',
    },
    'PG': {
        'name': 'Daily mean sea level pressure',
        'description': 'Official KNMI daily mean sea level pressure in 0.1 hPa, converted to hPa.',
        'unit': 'hPa',
        'source_unit': '0.1 hPa',
    },
    'SQ': {
        'name': 'Daily sunshine duration',
        'description': 'Official KNMI daily sunshine duration in 0.1 hour; coded -1 is normalized to observed 0.0 hour for sunshine below 0.05 hour.',
        'unit': 'hour',
        'source_unit': '0.1 hour',
    },
    'Q': {
        'name': 'Daily global solar radiation',
        'description': 'Official KNMI daily global solar radiation in J/cm^2, converted to canonical MJ m^-2.',
        'unit': 'MJ m^-2',
        'source_unit': 'J/cm^2',
    },
}

_KNMI_DATASET_SPECS = [
    KnmiDatasetSpec(
        provider='knmi',
        resolution='daily',
        label='KNMI public daily station observations via daggegevens CSV',
        data_url=KNMI_DAILY_DATA_URL,
        supported_elements=('TG', 'TX', 'TN', 'RH', 'FG', 'FXX', 'UG', 'PG', 'SQ', 'Q'),
        canonical_elements=KNMI_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[KnmiDatasetSpec | GhcndDatasetSpec]:
    return [*_KNMI_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[KnmiDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _KNMI_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> KnmiDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _KNMI_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported KNMI dataset combination: {provider}/{resolution}')
