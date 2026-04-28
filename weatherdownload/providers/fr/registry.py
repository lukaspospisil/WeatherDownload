from __future__ import annotations

from ..ghcnd.registry import GhcndDatasetSpec, build_dataset_spec


_GHCND_DAILY_CANONICAL_ELEMENTS = {
    'tas_max': ('TMAX',),
    'tas_min': ('TMIN',),
    'precipitation': ('PRCP',),
}


_GHCND_DATASET_SPECS = [
    build_dataset_spec(
        supported_elements=('TMAX', 'TMIN', 'PRCP'),
        canonical_elements=_GHCND_DAILY_CANONICAL_ELEMENTS,
    ),
]


def list_dataset_specs() -> list[GhcndDatasetSpec]:
    return list(_GHCND_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[GhcndDatasetSpec]:
    return [spec for spec in _GHCND_DATASET_SPECS if spec.implemented]


def get_dataset_spec(dataset_scope: str, resolution: str) -> GhcndDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _GHCND_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported GHCN-Daily dataset combination: {dataset_scope}/{resolution}')
