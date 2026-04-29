from __future__ import annotations

from ..ghcnd.registry import (
    GHCND_US_CANONICAL_ELEMENTS,
    GhcndDatasetSpec,
    build_country_dataset_specs,
    get_country_dataset_spec,
    list_country_dataset_specs,
    list_country_implemented_dataset_specs,
)


_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD', 'EVAP'),
    canonical_elements=GHCND_US_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[GhcndDatasetSpec]:
    return list_country_dataset_specs(_GHCND_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[GhcndDatasetSpec]:
    return list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS)


def get_dataset_spec(provider: str, resolution: str) -> GhcndDatasetSpec:
    return get_country_dataset_spec(_GHCND_DATASET_SPECS, provider, resolution)
