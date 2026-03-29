from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PolandDatasetSpec:
    dataset_scope: str
    resolution: str
    label: str
    station_metadata_url: str
    data_base_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


PL_BASE_URL = 'https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne'
PL_DESCRIPTION_URL = f'{PL_BASE_URL}/Opis.txt'
PL_STATION_METADATA_URL = f'{PL_BASE_URL}/wykaz_stacji.csv'
PL_DAILY_SYNOP_BASE_URL = f'{PL_BASE_URL}/dobowe/synop'

PL_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('STD',),
    'tas_max': ('TMAX',),
    'tas_min': ('TMIN',),
    'precipitation': ('SMDB',),
    'sunshine_duration': ('USL',),
}


def _parameter_metadata(raw_code: str, name: str, description: str) -> dict[str, str]:
    return {
        'name': name,
        'description': description,
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D IMGW synop daily',
        'raw_code': raw_code,
    }


PL_DAILY_PARAMETER_METADATA = {
    'STD': _parameter_metadata('STD', 'Daily mean air temperature', 'Official IMGW synop daily mean air temperature.'),
    'TMAX': _parameter_metadata('TMAX', 'Daily maximum air temperature', 'Official IMGW synop daily maximum air temperature.'),
    'TMIN': _parameter_metadata('TMIN', 'Daily minimum air temperature', 'Official IMGW synop daily minimum air temperature.'),
    'SMDB': _parameter_metadata('SMDB', 'Daily precipitation total', 'Official IMGW synop daily precipitation total.'),
    'USL': _parameter_metadata('USL', 'Daily sunshine duration', 'Official IMGW synop daily sunshine duration.'),
}

_PL_DATASET_SPECS = [
    PolandDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        label='IMGW-PIB historical daily synop station observations',
        station_metadata_url=PL_STATION_METADATA_URL,
        data_base_url=PL_DAILY_SYNOP_BASE_URL,
        supported_elements=tuple(PL_DAILY_PARAMETER_METADATA),
        canonical_elements=PL_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[PolandDatasetSpec]:
    return list(_PL_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[PolandDatasetSpec]:
    return [spec for spec in _PL_DATASET_SPECS if spec.implemented]


def get_dataset_spec(dataset_scope: str, resolution: str) -> PolandDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _PL_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported IMGW Poland dataset combination: {dataset_scope}/{resolution}')
