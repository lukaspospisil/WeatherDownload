from __future__ import annotations

from dataclasses import dataclass

from ..ghcnd.registry import GhcndDatasetSpec
from .ghcnd import (
    get_dataset_spec as get_ghcnd_dataset_spec,
    list_dataset_specs as list_ghcnd_dataset_specs,
    list_implemented_dataset_specs as list_ghcnd_implemented_dataset_specs,
)


@dataclass(frozen=True)
class FranceDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    data_base_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


FR_STATION_METADATA_URL = (
    'https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/METADONNEES_STATION/fiches.json'
)
FR_DAILY_BASE_URL = 'https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT'

FR_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('TM',),
    'tas_max': ('TX',),
    'tas_min': ('TN',),
    'precipitation': ('RR',),
}

FR_DAILY_PARAMETER_METADATA = {
    'RR': {
        'name': 'Daily precipitation total',
        'description': 'Official Meteo-France daily precipitation total from the RR-T-Vent family.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Meteo-France RR-T-Vent daily',
        'raw_code': 'RR',
    },
    'TN': {
        'name': 'Daily minimum air temperature',
        'description': 'Official Meteo-France daily minimum air temperature under shelter.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Meteo-France RR-T-Vent daily',
        'raw_code': 'TN',
    },
    'TX': {
        'name': 'Daily maximum air temperature',
        'description': 'Official Meteo-France daily maximum air temperature under shelter.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Meteo-France RR-T-Vent daily',
        'raw_code': 'TX',
    },
    'TM': {
        'name': 'Daily mean air temperature',
        'description': 'Official Meteo-France daily mean of hourly air temperatures under shelter.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Meteo-France RR-T-Vent daily',
        'raw_code': 'TM',
    },
}

_FR_DATASET_SPECS = [
    FranceDatasetSpec(
        provider='meteo_france',
        resolution='daily',
        label='Meteo-France daily climatological base RR-T-Vent observations',
        station_metadata_url=FR_STATION_METADATA_URL,
        data_base_url=FR_DAILY_BASE_URL,
        supported_elements=('RR', 'TN', 'TX', 'TM'),
        canonical_elements=FR_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[FranceDatasetSpec | GhcndDatasetSpec]:
    return [*list(_FR_DATASET_SPECS), *list_ghcnd_dataset_specs()]


def list_implemented_dataset_specs() -> list[FranceDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _FR_DATASET_SPECS if spec.implemented),
        *list_ghcnd_implemented_dataset_specs(),
    ]


def get_dataset_spec(provider: str, resolution: str) -> FranceDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_ghcnd_dataset_spec(normalized_provider, normalized_resolution)
    for spec in _FR_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported France dataset combination: {provider}/{resolution}')
