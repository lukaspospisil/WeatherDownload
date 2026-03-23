from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShmuDatasetSpec:
    dataset_scope: str
    resolution: str
    source_id: str
    label: str
    metadata_url: str
    data_index_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    experimental: bool = True
    canonical_elements: dict[str, tuple[str, ...]] | None = None


_SHMU_RECENT_DAILY_CANONICAL_ELEMENTS = {
    'tas_max': ('t_max',),
    'tas_min': ('t_min',),
    'sunshine_duration': ('sln_svit',),
    'precipitation': ('zra_uhrn',),
}


_SHMU_DATASET_SPECS = [
    ShmuDatasetSpec(
        dataset_scope='recent',
        resolution='daily',
        source_id='recent_daily_kli_inter',
        label='SHMU recent daily climatological stations',
        metadata_url='https://opendata.shmu.sk/meteorology/climate/recent/metadata/kli_inter_metadata.json',
        data_index_url='https://opendata.shmu.sk/meteorology/climate/recent/data/daily/',
        supported_elements=('t_max', 't_min', 'sln_svit', 'zra_uhrn'),
        canonical_elements=_SHMU_RECENT_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
        experimental=True,
    ),
]


def list_dataset_specs() -> list[ShmuDatasetSpec]:
    return list(_SHMU_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[ShmuDatasetSpec]:
    return [spec for spec in _SHMU_DATASET_SPECS if spec.implemented]


def get_dataset_spec(dataset_scope: str, resolution: str) -> ShmuDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _SHMU_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported SHMU dataset combination: {dataset_scope}/{resolution}')
