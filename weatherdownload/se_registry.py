from __future__ import annotations

from dataclasses import dataclass


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

SE_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('2',),
    'tas_max': ('20',),
    'tas_min': ('19',),
    'precipitation': ('5',),
}

SE_DAILY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    '2': {
        'name': 'Daily mean air temperature',
        'description': 'Official SMHI Meteorological Observations daily mean air temperature from the corrected-archive path.',
        'summary': 'medelvarde 1 dygn, 1 gang/dygn, kl 00',
        'unit': 'celsius',
    },
    '20': {
        'name': 'Daily maximum air temperature',
        'description': 'Official SMHI Meteorological Observations daily maximum air temperature from the corrected-archive path.',
        'summary': 'max, 1 gang per dygn',
        'unit': 'celsius',
    },
    '19': {
        'name': 'Daily minimum air temperature',
        'description': 'Official SMHI Meteorological Observations daily minimum air temperature from the corrected-archive path.',
        'summary': 'min, 1 gang per dygn',
        'unit': 'celsius',
    },
    '5': {
        'name': 'Daily precipitation sum',
        'description': 'Official SMHI Meteorological Observations daily precipitation sum from the corrected-archive path.',
        'summary': 'summa 1 dygn, 1 gang/dygn, kl 06',
        'unit': 'millimeter',
    },
}

SE_DAILY_PARAMETER_IDS = tuple(SE_DAILY_PARAMETER_METADATA)

_PARAMETER_URL_TEMPLATE = SMHI_METOBS_API_BASE + '/parameter/{parameter_id}.json'
_DATA_URL_TEMPLATE = SMHI_METOBS_API_BASE + '/parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv'

_SE_DATASET_SPECS = [
    SwedenDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        label='SMHI Meteorological Observations historical daily corrected-archive station observations',
        metadata_url=_PARAMETER_URL_TEMPLATE.format(parameter_id='2'),
        data_url=_DATA_URL_TEMPLATE.format(parameter_id='{parameter_id}', station_id='{station_id}'),
        supported_elements=SE_DAILY_PARAMETER_IDS,
        canonical_elements=SE_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[SwedenDatasetSpec]:
    return list(_SE_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[SwedenDatasetSpec]:
    return [spec for spec in _SE_DATASET_SPECS if spec.implemented]


def get_dataset_spec(dataset_scope: str, resolution: str) -> SwedenDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _SE_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported SMHI Sweden dataset combination: {dataset_scope}/{resolution}')

