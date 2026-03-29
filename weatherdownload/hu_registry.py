from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HungaryDatasetSpec:
    dataset_scope: str
    resolution: str
    label: str
    metadata_url: str
    historical_data_url: str
    recent_data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


HU_BASE_URL = 'https://odp.met.hu/climate/observations_hungary'
HU_METADATA_URL = f'{HU_BASE_URL}/meta/station_meta_auto.csv'
HU_DAILY_HISTORICAL_URL = f'{HU_BASE_URL}/daily/historical/'
HU_DAILY_RECENT_URL = f'{HU_BASE_URL}/daily/recent/'

HU_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('t',),
    'tas_max': ('tx',),
    'tas_min': ('tn',),
    'precipitation': ('rau',),
    'wind_speed': ('fs',),
    'relative_humidity': ('u',),
    'sunshine_duration': ('f',),
}

HU_DAILY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    't': {
        'name': 'Daily mean air temperature',
        'description': 'Official HungaroMet daily mean air temperature from the HABP_1D station observation files.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D HungaroMet HABP_1D',
    },
    'tx': {
        'name': 'Daily maximum air temperature',
        'description': 'Official HungaroMet daily maximum air temperature from the HABP_1D station observation files.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D HungaroMet HABP_1D',
    },
    'tn': {
        'name': 'Daily minimum air temperature',
        'description': 'Official HungaroMet daily minimum air temperature from the HABP_1D station observation files.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D HungaroMet HABP_1D',
    },
    'rau': {
        'name': 'Daily precipitation sum',
        'description': 'Official HungaroMet daily precipitation sum from the HABP_1D station observation files.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D HungaroMet HABP_1D',
    },
    'fs': {
        'name': 'Daily mean wind speed',
        'description': 'Official HungaroMet daily mean wind speed from the HABP_1D station observation files.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D HungaroMet HABP_1D',
    },
    'u': {
        'name': 'Daily mean relative humidity',
        'description': 'Official HungaroMet daily mean relative humidity from the HABP_1D station observation files.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D HungaroMet HABP_1D',
    },
    'f': {
        'name': 'Daily sunshine duration',
        'description': 'Official HungaroMet daily sunshine duration from the HABP_1D station observation files.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D HungaroMet HABP_1D',
    },
}

_HU_DATASET_SPECS = [
    HungaryDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        label='HungaroMet historical daily station observations',
        metadata_url=HU_METADATA_URL,
        historical_data_url=HU_DAILY_HISTORICAL_URL,
        recent_data_url=HU_DAILY_RECENT_URL,
        supported_elements=tuple(HU_DAILY_PARAMETER_METADATA),
        canonical_elements=HU_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[HungaryDatasetSpec]:
    return list(_HU_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[HungaryDatasetSpec]:
    return [spec for spec in _HU_DATASET_SPECS if spec.implemented]


def get_dataset_spec(dataset_scope: str, resolution: str) -> HungaryDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _HU_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported HungaroMet Hungary dataset combination: {dataset_scope}/{resolution}')

