from __future__ import annotations

from dataclasses import dataclass

from ..ghcnd.registry import (
    GHCND_STANDARD_CANONICAL_ELEMENTS,
    GhcndDatasetSpec,
    build_country_dataset_specs,
    get_country_dataset_spec,
    list_country_dataset_specs,
    list_country_implemented_dataset_specs,
)


@dataclass(frozen=True)
class BulgariaDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    rain_page_url: str
    snow_page_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


BG_NIMH_OPEN_DATA_URL = 'https://info.meteo.bg/openData/'
BG_NIMH_RAIN_PAGE_URL = 'https://info.meteo.bg/openData/rain'
BG_NIMH_SNOW_PAGE_URL = 'https://info.meteo.bg/openData/snow'

BG_NIMH_DAILY_CANONICAL_ELEMENTS = {
    'precipitation': ('precipitation',),
    'snow_depth': ('snow_cover_depth',),
}

BG_NIMH_PARAMETER_METADATA = {
    'precipitation': {
        'name': 'Daily precipitation total',
        'description': 'Official NIMH Bulgaria daily operational precipitation totals from the open-data rain archive.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D NIMH daily operational observations',
        'unit': 'mm',
    },
    'snow_cover_depth': {
        'name': 'Daily snow cover depth',
        'description': 'Official NIMH Bulgaria daily operational snow cover depth from the open-data snow archive.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D NIMH daily operational observations',
        'unit': 'cm',
    },
}

_BG_DATASET_SPECS = [
    BulgariaDatasetSpec(
        provider='nimh',
        resolution='daily',
        label='NIMH Bulgaria daily operational station observations',
        station_metadata_url=BG_NIMH_RAIN_PAGE_URL,
        rain_page_url=BG_NIMH_RAIN_PAGE_URL,
        snow_page_url=BG_NIMH_SNOW_PAGE_URL,
        supported_elements=tuple(BG_NIMH_PARAMETER_METADATA),
        canonical_elements=BG_NIMH_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[BulgariaDatasetSpec | GhcndDatasetSpec]:
    return [*_BG_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[BulgariaDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _BG_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> BulgariaDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _BG_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Bulgaria dataset combination: {provider}/{resolution}')
