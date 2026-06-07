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
class IcelandDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    parameter_metadata_url: str
    synop_observation_url: str
    aws_observation_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


IS_VEDUR_BASE_URL = 'https://api.vedur.is/weather'
IS_VEDUR_STATIONS_URL = f'{IS_VEDUR_BASE_URL}/stations'
IS_VEDUR_PARAMETERS_URL = f'{IS_VEDUR_BASE_URL}/parameters'
IS_VEDUR_SYNOP_DAILY_URL = f'{IS_VEDUR_BASE_URL}/observations/synop/day'
IS_VEDUR_AWS_DAILY_URL = f'{IS_VEDUR_BASE_URL}/observations/aws/day'

IS_VEDUR_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('t',),
    'tas_max': ('txx', 'tx'),
    'tas_min': ('tnn', 'tn'),
    'precipitation': ('r',),
    'wind_speed': ('f',),
    'wind_speed_max': ('fg',),
    'relative_humidity': ('rh',),
    'pressure': ('p',),
    'vapour_pressure': ('vp',),
    'snow_depth': ('snd',),
    'sunshine_duration': ('sun', 'rsun'),
}

IS_VEDUR_SYNOP_DAILY_PARAMETER_METADATA = {
    't': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily mean air temperature',
        'description': 'Official Vedur synop daily mean air temperature.',
        'unit': 'degC',
    },
    'txx': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily maximum air temperature',
        'description': 'Official Vedur synop daily maximum air temperature.',
        'unit': 'degC',
    },
    'tnn': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily minimum air temperature',
        'description': 'Official Vedur synop daily minimum air temperature.',
        'unit': 'degC',
    },
    'r': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily precipitation total',
        'description': 'Official Vedur synop daily precipitation total from 09 to 09.',
        'unit': 'mm',
    },
    'f': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily mean wind speed',
        'description': 'Official Vedur synop daily mean wind speed.',
        'unit': 'm/s',
    },
    'fg': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily maximum wind gust',
        'description': 'Official Vedur synop daily maximum three-second wind gust.',
        'unit': 'm/s',
    },
    'rh': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily mean relative humidity',
        'description': 'Official Vedur synop daily mean relative humidity.',
        'unit': '%',
    },
    'p': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily mean sea-level pressure',
        'description': 'Official Vedur synop daily mean atmospheric pressure at sea level.',
        'unit': 'hPa',
    },
    'vp': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily mean vapour pressure',
        'description': 'Official Vedur synop daily mean vapour pressure.',
        'unit': 'hPa',
    },
    'snd': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily snow depth',
        'description': 'Official Vedur synop daily snow depth measured at 09.',
        'unit': 'cm',
    },
    'sun': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur synop daily',
        'name': 'Daily sunshine duration',
        'description': 'Official Vedur synop daily total sunshine hours.',
        'unit': 'hr',
    },
}

IS_VEDUR_AWS_DAILY_PARAMETER_METADATA = {
    't': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily mean air temperature',
        'description': 'Official Vedur AWS daily mean air temperature.',
        'unit': 'degC',
    },
    'tx': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily maximum air temperature',
        'description': 'Official Vedur AWS daily maximum air temperature.',
        'unit': 'degC',
    },
    'tn': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily minimum air temperature',
        'description': 'Official Vedur AWS daily minimum air temperature.',
        'unit': 'degC',
    },
    'r': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily precipitation total',
        'description': 'Official Vedur AWS daily quality-checked and corrected 24-hour precipitation total.',
        'unit': 'mm',
    },
    'f': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily mean wind speed',
        'description': 'Official Vedur AWS daily mean wind speed.',
        'unit': 'm/s',
    },
    'fg': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily maximum wind gust',
        'description': 'Official Vedur AWS daily maximum three-second wind gust.',
        'unit': 'm/s',
    },
    'rh': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily mean relative humidity',
        'description': 'Official Vedur AWS daily mean relative humidity.',
        'unit': '%',
    },
    'p': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily mean sea-level pressure',
        'description': 'Official Vedur AWS daily mean atmospheric pressure at sea level.',
        'unit': 'hPa',
    },
    'vp': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily mean vapour pressure',
        'description': 'Official Vedur AWS daily mean vapour pressure.',
        'unit': 'hPa',
    },
    'rsun': {
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D official Vedur AWS daily',
        'name': 'Daily sunshine duration',
        'description': 'Official Vedur AWS daily total sunshine hours.',
        'unit': 'hr',
    },
}

IS_VEDUR_PARAMETER_METADATA_BY_SOURCE = {
    'synop': IS_VEDUR_SYNOP_DAILY_PARAMETER_METADATA,
    'aws': IS_VEDUR_AWS_DAILY_PARAMETER_METADATA,
}

_IS_DATASET_SPECS = [
    IcelandDatasetSpec(
        provider='vedur',
        resolution='daily',
        label='Icelandic Meteorological Office Vedur official daily station observations',
        station_metadata_url=IS_VEDUR_STATIONS_URL,
        parameter_metadata_url=IS_VEDUR_PARAMETERS_URL,
        synop_observation_url=IS_VEDUR_SYNOP_DAILY_URL,
        aws_observation_url=IS_VEDUR_AWS_DAILY_URL,
        supported_elements=(
            't',
            'txx',
            'tx',
            'tnn',
            'tn',
            'r',
            'f',
            'fg',
            'rh',
            'p',
            'vp',
            'snd',
            'sun',
            'rsun',
        ),
        time_semantics='date',
        canonical_elements=IS_VEDUR_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[IcelandDatasetSpec | GhcndDatasetSpec]:
    return [*_IS_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[IcelandDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _IS_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> IcelandDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _IS_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Iceland dataset combination: {provider}/{resolution}')
