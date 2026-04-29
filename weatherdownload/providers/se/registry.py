from __future__ import annotations

from dataclasses import dataclass

from ..ghcnd.registry import GhcndDatasetSpec
from .ghcnd import (
    get_dataset_spec as get_ghcnd_dataset_spec,
    list_dataset_specs as list_ghcnd_dataset_specs,
    list_implemented_dataset_specs as list_ghcnd_implemented_dataset_specs,
)


@dataclass(frozen=True)
class SwedenDatasetSpec:
    dataset_scope: str
    resolution: str
    label: str
    metadata_url: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


SMHI_METOBS_API_BASE = 'https://opendata-download-metobs.smhi.se/api/version/1.0'
SMHI_PARAMETER_URL_TEMPLATE = SMHI_METOBS_API_BASE + '/parameter/{parameter_id}.json'
SMHI_DATA_URL_TEMPLATE = SMHI_METOBS_API_BASE + '/parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv'
SMHI_DAILY_PERIOD_KEY = 'corrected-archive'
SMHI_HOURLY_PERIOD_KEY = 'corrected-archive'

SE_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('2',),
    'tas_max': ('20',),
    'tas_min': ('19',),
    'precipitation': ('5',),
}

SE_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('1',),
    'wind_speed': ('4',),
    'relative_humidity': ('6',),
    'precipitation': ('7',),
    'pressure': ('9',),
}

SE_DAILY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    '2': {
        'name': 'Daily mean air temperature',
        'description': 'Official SMHI Meteorological Observations daily mean air temperature from the corrected-archive path.',
        'summary': 'daily mean, once per day at 00',
        'unit': 'celsius',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D SMHI metObs corrected-archive',
    },
    '20': {
        'name': 'Daily maximum air temperature',
        'description': 'Official SMHI Meteorological Observations daily maximum air temperature from the corrected-archive path.',
        'summary': 'daily max, once per day',
        'unit': 'celsius',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D SMHI metObs corrected-archive',
    },
    '19': {
        'name': 'Daily minimum air temperature',
        'description': 'Official SMHI Meteorological Observations daily minimum air temperature from the corrected-archive path.',
        'summary': 'daily min, once per day',
        'unit': 'celsius',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D SMHI metObs corrected-archive',
    },
    '5': {
        'name': 'Daily precipitation sum',
        'description': 'Official SMHI Meteorological Observations daily precipitation sum from the corrected-archive path.',
        'summary': 'daily sum, once per day at 06',
        'unit': 'millimeter',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D SMHI metObs corrected-archive',
    },
}

SE_HOURLY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    '1': {
        'name': 'Hourly air temperature',
        'description': 'Official SMHI Meteorological Observations hourly air temperature from the corrected-archive path.',
        'summary': 'instantaneous value, once per hour',
        'unit': 'celsius',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H SMHI metObs corrected-archive',
    },
    '4': {
        'name': 'Hourly wind speed',
        'description': 'Official SMHI Meteorological Observations hourly wind speed from the corrected-archive path.',
        'summary': '10-minute mean, once per hour',
        'unit': 'meter per second',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H SMHI metObs corrected-archive',
    },
    '6': {
        'name': 'Hourly relative humidity',
        'description': 'Official SMHI Meteorological Observations hourly relative humidity from the corrected-archive path.',
        'summary': 'instantaneous value, once per hour',
        'unit': 'percent',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H SMHI metObs corrected-archive',
    },
    '7': {
        'name': 'Hourly precipitation sum',
        'description': 'Official SMHI Meteorological Observations hourly precipitation sum from the corrected-archive path.',
        'summary': 'hourly sum, once per hour',
        'unit': 'millimeter',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H SMHI metObs corrected-archive',
    },
    '9': {
        'name': 'Hourly sea-level pressure',
        'description': 'Official SMHI Meteorological Observations hourly sea-level pressure from the corrected-archive path.',
        'summary': 'sea-level pressure instantaneous value, once per hour',
        'unit': 'hectopascal',
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H SMHI metObs corrected-archive',
    },
}

SE_DAILY_PARAMETER_IDS = tuple(SE_DAILY_PARAMETER_METADATA)
SE_HOURLY_PARAMETER_IDS = tuple(SE_HOURLY_PARAMETER_METADATA)
SE_IMPLEMENTED_PARAMETER_IDS = SE_DAILY_PARAMETER_IDS + SE_HOURLY_PARAMETER_IDS
SE_PARAMETER_METADATA = {
    **SE_DAILY_PARAMETER_METADATA,
    **SE_HOURLY_PARAMETER_METADATA,
}

_SE_DATASET_SPECS = [
    SwedenDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        label='SMHI Meteorological Observations historical daily corrected-archive station observations',
        metadata_url=SMHI_PARAMETER_URL_TEMPLATE.format(parameter_id='2'),
        data_url=SMHI_DATA_URL_TEMPLATE.format(parameter_id='{parameter_id}', station_id='{station_id}'),
        supported_elements=SE_DAILY_PARAMETER_IDS,
        canonical_elements=SE_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
    SwedenDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        label='SMHI Meteorological Observations historical hourly corrected-archive station observations',
        metadata_url=SMHI_PARAMETER_URL_TEMPLATE.format(parameter_id='1'),
        data_url=SMHI_DATA_URL_TEMPLATE.format(parameter_id='{parameter_id}', station_id='{station_id}'),
        supported_elements=SE_HOURLY_PARAMETER_IDS,
        canonical_elements=SE_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[SwedenDatasetSpec]:
    return [*list(_SE_DATASET_SPECS), *list_ghcnd_dataset_specs()]


def list_implemented_dataset_specs() -> list[SwedenDatasetSpec]:
    return [
        *(spec for spec in _SE_DATASET_SPECS if spec.implemented),
        *list_ghcnd_implemented_dataset_specs(),
    ]


def get_dataset_spec(dataset_scope: str, resolution: str) -> SwedenDatasetSpec | GhcndDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    if normalized_scope == 'ghcnd':
        return get_ghcnd_dataset_spec(normalized_scope, normalized_resolution)
    for spec in _SE_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported SMHI Sweden dataset combination: {dataset_scope}/{resolution}')
