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
class EstoniaDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    observation_base_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


EE_ILMATEENISTUS_STATION_METADATA_URL = 'https://keskkonnaandmed.envir.ee/f_kliima_jaam_vaatlus'
EE_ILMATEENISTUS_ELEMENT_METADATA_URL = 'https://keskkonnaandmed.envir.ee/f_kliima_element'
EE_ILMATEENISTUS_DAILY_URL = 'https://keskkonnaandmed.envir.ee/f_kliima_paev'

EE_ILMATEENISTUS_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('DTA08',),
    'tas_max': ('DTAX',),
    'tas_min': ('DTAN',),
    'precipitation': ('DPREC',),
    'wind_speed': ('DWS08',),
    'relative_humidity': ('DRH08',),
    'sunshine_duration': ('DSDUR',),
    'pressure': ('DPA008',),
    'snow_depth': ('DSND',),
    'solar_radiation': ('DRQS',),
}

EE_ILMATEENISTUS_PARAMETER_METADATA = {
    'DPA008': {
        'name': 'Air pressure at sea level (daily avg)',
        'description': 'Official Estonian climate daily mean sea-level air pressure.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'hPa',
    },
    'DPREC': {
        'name': 'Precipitation (daily sum)',
        'description': 'Official Estonian climate daily precipitation sum.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'mm',
    },
    'DRH08': {
        'name': 'Relative humidity (daily avg)',
        'description': 'Official Estonian climate daily mean relative humidity.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': '%',
    },
    'DRQS': {
        'name': 'Global radiation (daily sum)',
        'description': 'Official Estonian climate daily global radiation sum.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'MJ/m2',
    },
    'DSDUR': {
        'name': 'Sunshine duration (daily sum)',
        'description': 'Official Estonian climate daily sunshine duration sum.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'h',
    },
    'DSND': {
        'name': 'Snow depth (at 06:00UTC)',
        'description': 'Official Estonian climate daily snow depth at 06:00 UTC.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'cm',
    },
    'DTAN': {
        'name': 'Air temperature (daily min)',
        'description': 'Official Estonian climate daily minimum air temperature.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'degC',
    },
    'DTAX': {
        'name': 'Air temperature (daily max)',
        'description': 'Official Estonian climate daily maximum air temperature.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'degC',
    },
    'DTA08': {
        'name': 'Air temperature (daily avg)',
        'description': 'Official Estonian climate daily mean air temperature.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'degC',
    },
    'DWS08': {
        'name': 'Wind speed (daily avg)',
        'description': 'Official Estonian climate daily mean wind speed.',
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D Estonian climate daily observations',
        'unit': 'm/s',
    },
}

_EE_DATASET_SPECS = [
    EstoniaDatasetSpec(
        provider='ilmateenistus',
        resolution='daily',
        label='Estonian Environment Agency climate daily observations',
        station_metadata_url=EE_ILMATEENISTUS_STATION_METADATA_URL,
        observation_base_url=EE_ILMATEENISTUS_DAILY_URL,
        supported_elements=tuple(EE_ILMATEENISTUS_PARAMETER_METADATA),
        canonical_elements=EE_ILMATEENISTUS_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[EstoniaDatasetSpec | GhcndDatasetSpec]:
    return [*_EE_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[EstoniaDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _EE_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> EstoniaDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _EE_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Estonia dataset combination: {provider}/{resolution}')
