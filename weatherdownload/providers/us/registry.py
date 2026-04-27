from __future__ import annotations

from dataclasses import dataclass


GHCND_STATIONS_URL = 'https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
GHCND_INVENTORY_URL = 'https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt'
GHCND_ALL_BASE_URL = 'https://www.ncei.noaa.gov/pub/data/ghcn/daily/all'


@dataclass(frozen=True)
class GhcndDatasetSpec:
    dataset_scope: str
    resolution: str
    source_id: str
    label: str
    stations_url: str
    inventory_url: str
    data_base_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


_GHCND_DAILY_CANONICAL_ELEMENTS = {
    'open_water_evaporation': ('EVAP',),
}


_GHCND_DATASET_SPECS = [
    GhcndDatasetSpec(
        dataset_scope='ghcnd',
        resolution='daily',
        source_id='ncei_ghcnd_daily',
        label='NOAA NCEI GHCN-Daily station observations',
        stations_url=GHCND_STATIONS_URL,
        inventory_url=GHCND_INVENTORY_URL,
        data_base_url=GHCND_ALL_BASE_URL,
        supported_elements=('EVAP',),
        canonical_elements=_GHCND_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
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
