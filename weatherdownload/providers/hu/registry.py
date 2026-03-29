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
HU_HOURLY_HISTORICAL_URL = f'{HU_BASE_URL}/hourly/historical/'
HU_HOURLY_RECENT_URL = f'{HU_BASE_URL}/hourly/recent/'
HU_TENMIN_HISTORICAL_URL = f'{HU_BASE_URL}/10_minutes/historical/'
HU_TENMIN_RECENT_URL = f'{HU_BASE_URL}/10_minutes/recent/'
HU_TENMIN_WIND_BASE_URL = f'{HU_BASE_URL}/10_minutes_wind'
HU_TENMIN_WIND_METADATA_URL = f'{HU_TENMIN_WIND_BASE_URL}/station_meta_auto_wind.csv'
HU_TENMIN_WIND_HISTORICAL_URL = f'{HU_TENMIN_WIND_BASE_URL}/historical/'
HU_TENMIN_WIND_RECENT_URL = f'{HU_TENMIN_WIND_BASE_URL}/recent/'

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

HU_HOURLY_CANONICAL_ELEMENTS = {
    'precipitation': ('r',),
    'tas_mean': ('ta',),
    'pressure': ('p',),
    'relative_humidity': ('u',),
    'wind_speed': ('f',),
}

HU_HOURLY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'r': {
        'name': 'Hourly precipitation sum',
        'description': 'Official HungaroMet hourly precipitation sum from the HABP_1H station observation files.',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H HungaroMet HABP_1H',
    },
    'ta': {
        'name': 'Hourly mean air temperature',
        'description': 'Official HungaroMet hourly mean air temperature from the HABP_1H station observation files.',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H HungaroMet HABP_1H',
    },
    'p': {
        'name': 'Hourly station pressure',
        'description': 'Official HungaroMet hourly station pressure from the HABP_1H station observation files.',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H HungaroMet HABP_1H',
    },
    'u': {
        'name': 'Hourly relative humidity',
        'description': 'Official HungaroMet hourly relative humidity from the HABP_1H station observation files.',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H HungaroMet HABP_1H',
    },
    'f': {
        'name': 'Hourly mean wind speed',
        'description': 'Official HungaroMet hourly mean wind speed from the HABP_1H station observation files.',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H HungaroMet HABP_1H',
    },
}

HU_TENMIN_CANONICAL_ELEMENTS = {
    'precipitation': ('r',),
    'tas_mean': ('ta',),
    'pressure': ('p',),
    'relative_humidity': ('u',),
    'wind_speed': ('fs',),
}

HU_TENMIN_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'r': {
        'name': '10-minute precipitation sum',
        'description': 'Official HungaroMet 10-minute precipitation sum from the HABP_10M station observation files.',
        'obs_type': 'HISTORICAL_10MIN',
        'schedule': 'PT10M HungaroMet HABP_10M',
    },
    'ta': {
        'name': '10-minute mean air temperature',
        'description': 'Official HungaroMet 10-minute mean air temperature from the HABP_10M station observation files.',
        'obs_type': 'HISTORICAL_10MIN',
        'schedule': 'PT10M HungaroMet HABP_10M',
    },
    'p': {
        'name': '10-minute station pressure',
        'description': 'Official HungaroMet 10-minute station pressure from the HABP_10M station observation files.',
        'obs_type': 'HISTORICAL_10MIN',
        'schedule': 'PT10M HungaroMet HABP_10M',
    },
    'u': {
        'name': '10-minute relative humidity',
        'description': 'Official HungaroMet 10-minute relative humidity from the HABP_10M station observation files.',
        'obs_type': 'HISTORICAL_10MIN',
        'schedule': 'PT10M HungaroMet HABP_10M',
    },
    'fs': {
        'name': '10-minute mean wind speed',
        'description': 'Official HungaroMet 10-minute mean wind speed from the HABP_10M station observation files.',
        'obs_type': 'HISTORICAL_10MIN',
        'schedule': 'PT10M HungaroMet HABP_10M',
    },
}

HU_TENMIN_WIND_CANONICAL_ELEMENTS = {
    'wind_speed': ('fs',),
    'wind_speed_max': ('fx',),
}

HU_TENMIN_WIND_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'fs': {
        'name': '10-minute mean wind speed',
        'description': 'Official HungaroMet 10-minute mean wind speed from the HABP_10MWIND station observation files.',
        'obs_type': 'HISTORICAL_10MIN_WIND',
        'schedule': 'PT10M HungaroMet HABP_10MWIND',
    },
    'fx': {
        'name': '10-minute maximum wind speed',
        'description': 'Official HungaroMet 10-minute maximum wind speed from the HABP_10MWIND station observation files.',
        'obs_type': 'HISTORICAL_10MIN_WIND',
        'schedule': 'PT10M HungaroMet HABP_10MWIND',
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
    HungaryDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        label='HungaroMet historical hourly station observations',
        metadata_url=HU_METADATA_URL,
        historical_data_url=HU_HOURLY_HISTORICAL_URL,
        recent_data_url=HU_HOURLY_RECENT_URL,
        supported_elements=tuple(HU_HOURLY_PARAMETER_METADATA),
        canonical_elements=HU_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    HungaryDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        label='HungaroMet historical 10-minute station observations',
        metadata_url=HU_METADATA_URL,
        historical_data_url=HU_TENMIN_HISTORICAL_URL,
        recent_data_url=HU_TENMIN_RECENT_URL,
        supported_elements=tuple(HU_TENMIN_PARAMETER_METADATA),
        canonical_elements=HU_TENMIN_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    HungaryDatasetSpec(
        dataset_scope='historical_wind',
        resolution='10min',
        label='HungaroMet historical 10-minute wind station observations',
        metadata_url=HU_TENMIN_WIND_METADATA_URL,
        historical_data_url=HU_TENMIN_WIND_HISTORICAL_URL,
        recent_data_url=HU_TENMIN_WIND_RECENT_URL,
        supported_elements=tuple(HU_TENMIN_WIND_PARAMETER_METADATA),
        canonical_elements=HU_TENMIN_WIND_CANONICAL_ELEMENTS,
        time_semantics='datetime',
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

