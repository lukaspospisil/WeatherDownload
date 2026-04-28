from __future__ import annotations

from ..ghcnd.registry import (
    GhcndDatasetSpec,
    build_country_dataset_specs,
    get_country_dataset_spec,
    list_country_dataset_specs,
    list_country_implemented_dataset_specs,
)


_GHCND_DAILY_CANONICAL_ELEMENTS = {
    'tas_max': ('TMAX',),
    'tas_min': ('TMIN',),
    'precipitation': ('PRCP',),
    'open_water_evaporation': ('EVAP',),
}


_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TMAX', 'TMIN', 'PRCP', 'EVAP'),
    canonical_elements=_GHCND_DAILY_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[GhcndDatasetSpec]:
    return list_country_dataset_specs(_GHCND_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[GhcndDatasetSpec]:
    return list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS)


def get_dataset_spec(dataset_scope: str, resolution: str) -> GhcndDatasetSpec:
    return get_country_dataset_spec(_GHCND_DATASET_SPECS, dataset_scope, resolution)
