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
class CanadaDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    daily_data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


CA_ECCC_STATION_METADATA_URL = 'https://api.weather.gc.ca/collections/climate-stations/items?f=json'
CA_ECCC_DAILY_DATA_URL = 'https://api.weather.gc.ca/collections/climate-daily/items?f=json'
CA_ECCC_HOURLY_DATA_URL = 'https://api.weather.gc.ca/collections/climate-hourly/items?f=json'

CA_ECCC_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('MEAN_TEMPERATURE',),
    'tas_max': ('MAX_TEMPERATURE',),
    'tas_min': ('MIN_TEMPERATURE',),
    'precipitation': ('TOTAL_PRECIPITATION',),
}

CA_ECCC_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('TEMP',),
    'relative_humidity': ('RELATIVE_HUMIDITY',),
}

_CA_ECCC_DATASET_SPECS = [
    CanadaDatasetSpec(
        provider='eccc',
        resolution='daily',
        label='Environment and Climate Change Canada GeoMet daily climate observations',
        station_metadata_url=CA_ECCC_STATION_METADATA_URL,
        daily_data_url=CA_ECCC_DAILY_DATA_URL,
        supported_elements=('MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'),
        canonical_elements=CA_ECCC_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
    CanadaDatasetSpec(
        provider='eccc',
        resolution='1hour',
        label='Environment and Climate Change Canada GeoMet hourly climate observations',
        station_metadata_url=CA_ECCC_STATION_METADATA_URL,
        daily_data_url=CA_ECCC_HOURLY_DATA_URL,
        supported_elements=('TEMP', 'RELATIVE_HUMIDITY'),
        canonical_elements=CA_ECCC_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[CanadaDatasetSpec | GhcndDatasetSpec]:
    return [*_CA_ECCC_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[CanadaDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _CA_ECCC_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> CanadaDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _CA_ECCC_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Canada dataset combination: {provider}/{resolution}')
