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
class RomaniaDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    observation_base_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


RO_ANM_NETWORK_URL = 'https://inspire.meteoromania.ro/ids/EnvironmentalMonitoringNetwork.N100'
RO_ANM_WFS_BASE_URL = 'https://inspire.meteoromania.ro/WIGOS/WFS/wfs'
RO_ANM_OBSERVATION_BASE_URL = 'https://inspire.meteoromania.ro/ids'

RO_ANM_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('TemperatureAverageDailyCLIMAT',),
    'tas_max': ('TemperatureMaximumDailyCLIMAT',),
    'tas_min': ('TemperatureMinimumDailyCLIMAT',),
    'precipitation': ('TotalPrecipitationCLIMAT',),
}

RO_ANM_PARAMETER_METADATA = {
    'TemperatureAverageDailyCLIMAT': {
        'name': 'Daily mean air temperature',
        'description': 'ANM M201 CLIMAT daily 24-hour mean air temperature from the official station-level INSPIRE WaterML series.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D ANM M201 CLIMAT daily',
        'unit': 'K',
    },
    'TemperatureMaximumDailyCLIMAT': {
        'name': 'Daily maximum air temperature',
        'description': 'ANM M201 CLIMAT daily 24-hour maximum air temperature from the official station-level INSPIRE WaterML series.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D ANM M201 CLIMAT daily',
        'unit': 'K',
    },
    'TemperatureMinimumDailyCLIMAT': {
        'name': 'Daily minimum air temperature',
        'description': 'ANM M201 CLIMAT daily 24-hour minimum air temperature from the official station-level INSPIRE WaterML series.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D ANM M201 CLIMAT daily',
        'unit': 'K',
    },
    'TotalPrecipitationCLIMAT': {
        'name': 'Daily total precipitation',
        'description': 'ANM M201 CLIMAT total precipitation over the past 24 hours from the official station-level INSPIRE WaterML series.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D ANM M201 CLIMAT daily',
        'unit': 'kg/m2',
    },
}

_RO_ANM_DATASET_SPECS = [
    RomaniaDatasetSpec(
        provider='anm',
        resolution='daily',
        label='Romanian National Meteorological Administration INSPIRE WFS daily CLIMAT observations',
        station_metadata_url=RO_ANM_NETWORK_URL,
        observation_base_url=RO_ANM_OBSERVATION_BASE_URL,
        supported_elements=(
            'TemperatureAverageDailyCLIMAT',
            'TemperatureMaximumDailyCLIMAT',
            'TemperatureMinimumDailyCLIMAT',
            'TotalPrecipitationCLIMAT',
        ),
        canonical_elements=RO_ANM_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[RomaniaDatasetSpec | GhcndDatasetSpec]:
    return [*_RO_ANM_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[RomaniaDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _RO_ANM_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> RomaniaDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _RO_ANM_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Romania dataset combination: {provider}/{resolution}')
