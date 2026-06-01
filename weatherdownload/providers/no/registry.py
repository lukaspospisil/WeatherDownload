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


FROST_BASE_URL = 'https://frost.met.no'
FROST_SOURCES_URL = f'{FROST_BASE_URL}/sources/v0.jsonld'
FROST_OBSERVATIONS_URL = f'{FROST_BASE_URL}/observations/v0.jsonld'

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)

NO_FROST_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('mean(air_temperature P1D)',),
    'tas_max': ('max(air_temperature P1D)',),
    'tas_min': ('min(air_temperature P1D)',),
    'precipitation': ('sum(precipitation_amount P1D)',),
    'wind_speed': ('mean(wind_speed P1D)',),
    'snow_depth': ('surface_snow_thickness',),
}

NO_FROST_DAILY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'mean(air_temperature P1D)': {
        'name': 'Daily mean air temperature',
        'description': 'Official MET Norway Frost daily mean air temperature in degrees Celsius.',
    },
    'max(air_temperature P1D)': {
        'name': 'Daily maximum air temperature',
        'description': 'Official MET Norway Frost daily maximum air temperature in degrees Celsius.',
    },
    'min(air_temperature P1D)': {
        'name': 'Daily minimum air temperature',
        'description': 'Official MET Norway Frost daily minimum air temperature in degrees Celsius.',
    },
    'sum(precipitation_amount P1D)': {
        'name': 'Daily precipitation amount',
        'description': 'Official MET Norway Frost daily precipitation amount in millimetres. Frost coded value -1 is normalized to 0.0 mm for no precipitation.',
    },
    'mean(wind_speed P1D)': {
        'name': 'Daily mean wind speed',
        'description': 'Official MET Norway Frost daily mean wind speed in metres per second.',
    },
    'surface_snow_thickness': {
        'name': 'Surface snow thickness',
        'description': 'Official MET Norway Frost daily surface snow thickness. Frost reports this element in centimetres, and WeatherDownload converts observed non-coded values to canonical millimetres.',
    },
}


@dataclass(frozen=True)
class NorwayDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


_NO_DATASET_SPECS = [
    NorwayDatasetSpec(
        provider='frost',
        resolution='daily',
        label='MET Norway Frost official daily station observations',
        station_metadata_url=FROST_SOURCES_URL,
        data_url=FROST_OBSERVATIONS_URL,
        supported_elements=(
            'mean(air_temperature P1D)',
            'max(air_temperature P1D)',
            'min(air_temperature P1D)',
            'sum(precipitation_amount P1D)',
            'mean(wind_speed P1D)',
            'surface_snow_thickness',
        ),
        canonical_elements=NO_FROST_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[NorwayDatasetSpec | GhcndDatasetSpec]:
    return [*list(_NO_DATASET_SPECS), *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[NorwayDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _NO_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> NorwayDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _NO_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Norway dataset combination: {provider}/{resolution}')
