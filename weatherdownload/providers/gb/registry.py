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

METOFFICE_DATAHUB_SAMPLE_STATION_METADATA_URL = (
    'https://datahub.metoffice.gov.uk/sample-model-data/observations/download/land-observations-nearest-geohash-sample'
)
# The public docs describe a geohash-based location lookup and an hourly observations API,
# but they do not expose a static station-list endpoint in the checked-in docs. Keep the live
# URL template isolated so it can be adjusted without affecting the parser/tests.
METOFFICE_DATAHUB_OBSERVATIONS_URL_TEMPLATE = (
    'https://datahub.metoffice.gov.uk/api/observations/land/{station_id}'
)

GB_METOFFICE_DATAHUB_CANONICAL_ELEMENTS = {
    'tas_mean': ('temperature',),
    'relative_humidity': ('humidity',),
    'wind_speed': ('wind_speed',),
    'pressure': ('mslp',),
}


@dataclass(frozen=True)
class GreatBritainDatasetSpec:
    provider: str
    resolution: str
    label: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None
    station_metadata_url: str | None = None
    data_url: str | None = None


_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)

_GB_DATASET_SPECS = [
    GreatBritainDatasetSpec(
        provider='metoffice_datahub',
        resolution='1hour',
        label='Met Office Weather DataHub Land Observations recent hourly station observations',
        supported_elements=('temperature', 'humidity', 'wind_speed', 'mslp'),
        canonical_elements=GB_METOFFICE_DATAHUB_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
        station_metadata_url=METOFFICE_DATAHUB_SAMPLE_STATION_METADATA_URL,
        data_url=METOFFICE_DATAHUB_OBSERVATIONS_URL_TEMPLATE,
    ),
]


def list_dataset_specs() -> list[GreatBritainDatasetSpec | GhcndDatasetSpec]:
    return [*list(_GB_DATASET_SPECS), *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[GreatBritainDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _GB_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> GreatBritainDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _GB_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Great Britain dataset combination: {provider}/{resolution}')
