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


_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)

FMI_WFS_URL = 'https://opendata.fmi.fi/wfs'
FMI_WEATHER_TIMEVALUEPAIR_STORED_QUERY = 'fmi::observations::weather::timevaluepair'

FMI_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('t2m',),
    'wind_speed': ('ws_10min',),
    'relative_humidity': ('rh',),
    'pressure': ('p_sea',),
    'precipitation': ('r_1h',),
}


@dataclass(frozen=True)
class FinlandDatasetSpec:
    provider: str
    resolution: str
    label: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None
    station_metadata_url: str | None = None
    wfs_url: str = FMI_WFS_URL
    storedquery_id: str = FMI_WEATHER_TIMEVALUEPAIR_STORED_QUERY
    timestep_minutes: int = 60


_FI_DATASET_SPECS = [
    FinlandDatasetSpec(
        provider='fmi',
        resolution='1hour',
        label='Finnish Meteorological Institute (FMI) Open Data WFS weather time series (timevaluepair)',
        supported_elements=('t2m', 'ws_10min', 'rh', 'p_sea', 'r_1h'),
        canonical_elements=FMI_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[GhcndDatasetSpec]:
    return [*list(_FI_DATASET_SPECS), *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[GhcndDatasetSpec]:
    return [
        *(spec for spec in _FI_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> FinlandDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _FI_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Finland dataset combination: {provider}/{resolution}')
