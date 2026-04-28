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


def build_dataset_spec(
    *,
    supported_elements: tuple[str, ...],
    canonical_elements: dict[str, tuple[str, ...]],
    label: str = 'NOAA NCEI GHCN-Daily station observations',
    source_id: str = 'ncei_ghcnd_daily',
) -> GhcndDatasetSpec:
    return GhcndDatasetSpec(
        dataset_scope='ghcnd',
        resolution='daily',
        source_id=source_id,
        label=label,
        stations_url=GHCND_STATIONS_URL,
        inventory_url=GHCND_INVENTORY_URL,
        data_base_url=GHCND_ALL_BASE_URL,
        supported_elements=supported_elements,
        canonical_elements=canonical_elements,
        time_semantics='date',
        implemented=True,
    )


def build_country_dataset_specs(
    *,
    supported_elements: tuple[str, ...],
    canonical_elements: dict[str, tuple[str, ...]],
    label: str = 'NOAA NCEI GHCN-Daily station observations',
    source_id: str = 'ncei_ghcnd_daily',
) -> list[GhcndDatasetSpec]:
    return [
        build_dataset_spec(
            supported_elements=supported_elements,
            canonical_elements=canonical_elements,
            label=label,
            source_id=source_id,
        )
    ]


def list_country_dataset_specs(specs: list[GhcndDatasetSpec]) -> list[GhcndDatasetSpec]:
    return list(specs)


def list_country_implemented_dataset_specs(specs: list[GhcndDatasetSpec]) -> list[GhcndDatasetSpec]:
    return [spec for spec in specs if spec.implemented]


def get_country_dataset_spec(
    specs: list[GhcndDatasetSpec],
    dataset_scope: str,
    resolution: str,
) -> GhcndDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in specs:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported GHCN-Daily dataset combination: {dataset_scope}/{resolution}')
