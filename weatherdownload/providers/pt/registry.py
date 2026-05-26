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


IPMA_STATIONS_URL = 'https://api.ipma.pt/open-data/observation/meteorology/stations/stations.json'
IPMA_OBSERVATIONS_URL = 'https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json'

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)

_IPMA_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('temperatura',),
    'precipitation': ('precAcumulada',),
    'wind_speed': ('intensidadeVento',),
    'relative_humidity': ('humidade',),
    'solar_radiation': ('radiacao',),
}


@dataclass(frozen=True)
class PortugalDatasetSpec:
    provider: str
    resolution: str
    label: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None
    station_metadata_url: str | None = None
    data_url: str | None = None


_PT_DATASET_SPECS = [
    PortugalDatasetSpec(
        provider='ipma',
        resolution='1hour',
        label='IPMA recent hourly station observations',
        supported_elements=('temperatura', 'precAcumulada', 'intensidadeVento', 'humidade', 'radiacao'),
        canonical_elements=_IPMA_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
        station_metadata_url=IPMA_STATIONS_URL,
        data_url=IPMA_OBSERVATIONS_URL,
    ),
]


def list_dataset_specs() -> list[PortugalDatasetSpec | GhcndDatasetSpec]:
    return [*list(_PT_DATASET_SPECS), *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[PortugalDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _PT_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> PortugalDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _PT_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Portugal dataset combination: {provider}/{resolution}')
