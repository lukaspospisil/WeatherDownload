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
class SloveniaDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    observation_base_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


SI_ARSO_SETTINGS_URL = 'https://meteo.arso.gov.si/webmet/archive/settings.xml'
SI_ARSO_LOCATIONS_URL = 'https://meteo.arso.gov.si/webmet/archive/locations.xml?type=1,2,3&d1=2024-01-01'
SI_ARSO_OBSERVATION_BASE_URL = 'https://meteo.arso.gov.si/webmet/archive/data.xml'
SI_ARSO_MIN_DATE = '1948-01-01T00:00Z'
SI_ARSO_OPEN_END_DATE = '3999-12-31T23:59Z'
SI_ARSO_EPOCH = '1800-01-01T00:00:00'

SI_ARSO_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('t2m_klima',),
    'tas_max': ('tmax',),
    'tas_min': ('tmin',),
    'precipitation': ('padavine_klima',),
    'snow_depth': ('sneg_skupni',),
    'sunshine_duration': ('trajanje_so',),
}

SI_ARSO_PARAMETER_METADATA = {
    't2m_klima': {
        'pid': 35,
        'name': 'Mean daily air temperature',
        'description': 'ARSO daily archive mean daily air temperature from station-level climate observations.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'ARSO daily archive',
        'unit': 'degC',
    },
    'tmax': {
        'pid': 38,
        'name': 'Daily maximum air temperature',
        'description': 'ARSO daily archive daily maximum air temperature from station-level climate observations.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'ARSO daily archive',
        'unit': 'degC',
    },
    'tmin': {
        'pid': 36,
        'name': 'Daily minimum air temperature',
        'description': 'ARSO daily archive daily minimum air temperature from station-level climate observations.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'ARSO daily archive',
        'unit': 'degC',
    },
    'padavine_klima': {
        'pid': 85,
        'name': 'Daily precipitation sum',
        'description': 'ARSO daily archive daily precipitation sum from station-level climate observations.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'ARSO daily archive',
        'unit': 'mm',
    },
    'sneg_skupni': {
        'pid': 88,
        'name': 'Snow depth',
        'description': 'ARSO daily archive station-level total snow depth.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'ARSO daily archive',
        'unit': 'cm',
    },
    'trajanje_so': {
        'pid': 41,
        'name': 'Bright sunshine duration',
        'description': 'ARSO daily archive bright sunshine duration from station-level climate observations.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'ARSO daily archive',
        'unit': 'h',
    },
}

SI_ARSO_RAW_BY_PID = {
    str(metadata['pid']): raw_name
    for raw_name, metadata in SI_ARSO_PARAMETER_METADATA.items()
}

SI_ARSO_PID_BY_RAW = {
    raw_name: str(metadata['pid'])
    for raw_name, metadata in SI_ARSO_PARAMETER_METADATA.items()
}

SI_ARSO_STATION_TYPE_RAW_ELEMENTS = {
    1: ('padavine_klima', 'sneg_skupni'),
    2: ('t2m_klima', 'tmax', 'tmin', 'padavine_klima', 'sneg_skupni', 'trajanje_so'),
    3: ('t2m_klima', 'tmax', 'tmin', 'padavine_klima', 'sneg_skupni', 'trajanje_so'),
}

_SI_ARSO_DATASET_SPECS = [
    SloveniaDatasetSpec(
        provider='arso',
        resolution='daily',
        label='ARSO station-level daily climate archive observations',
        station_metadata_url=SI_ARSO_LOCATIONS_URL,
        observation_base_url=SI_ARSO_OBSERVATION_BASE_URL,
        supported_elements=(
            't2m_klima',
            'tmax',
            'tmin',
            'padavine_klima',
            'sneg_skupni',
            'trajanje_so',
        ),
        canonical_elements=SI_ARSO_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[SloveniaDatasetSpec | GhcndDatasetSpec]:
    return [*_SI_ARSO_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[SloveniaDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _SI_ARSO_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> SloveniaDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _SI_ARSO_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Slovenia dataset combination: {provider}/{resolution}')
