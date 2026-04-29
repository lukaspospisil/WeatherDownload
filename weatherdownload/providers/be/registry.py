from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BelgiumDatasetSpec:
    provider: str
    resolution: str
    label: str
    metadata_url: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


RMI_AWS_WFS_URL = 'https://opendata.meteo.be/geoserver/aws/ows'
RMI_AWS_STATION_LAYER = 'aws:aws_station'
RMI_AWS_DAILY_LAYER = 'aws:aws_1day'
RMI_AWS_HOURLY_LAYER = 'aws:aws_1hour'
RMI_AWS_TENMIN_LAYER = 'aws:aws_10min'

_BE_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('temp_avg',),
    'tas_max': ('temp_max',),
    'tas_min': ('temp_min',),
    'precipitation': ('precip_quantity',),
    'wind_speed': ('wind_speed_10m',),
    'relative_humidity': ('humidity_rel_shelter_avg',),
    'pressure': ('pressure',),
    'sunshine_duration': ('sun_duration',),
}

_BE_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('temp_dry_shelter_avg',),
    'precipitation': ('precip_quantity',),
    'wind_speed': ('wind_speed_10m',),
    'relative_humidity': ('humidity_rel_shelter_avg',),
    'pressure': ('pressure',),
    'sunshine_duration': ('sun_duration',),
}

_BE_TENMIN_CANONICAL_ELEMENTS = {
    'tas_mean': ('temp_dry_shelter_avg',),
    'precipitation': ('precip_quantity',),
    'wind_speed': ('wind_speed_10m',),
    'relative_humidity': ('humidity_rel_shelter_avg',),
    'pressure': ('pressure',),
    'sunshine_duration': ('sun_duration',),
}

BE_DAILY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'precip_quantity': {
        'name': 'Daily precipitation amount',
        'description': 'Official RMI/KMI provider-side daily sum of precipitation quantity in millimeters from the documented AWS daily aggregation.',
    },
    'temp_avg': {
        'name': 'Daily mean air temperature',
        'description': 'Official RMI/KMI provider-side daily average air temperature in degrees Celsius from the documented AWS daily aggregation.',
    },
    'temp_max': {
        'name': 'Daily maximum air temperature',
        'description': 'Official RMI/KMI provider-side daily maximum air temperature in degrees Celsius from the documented AWS daily aggregation.',
    },
    'temp_min': {
        'name': 'Daily minimum air temperature',
        'description': 'Official RMI/KMI provider-side daily minimum air temperature in degrees Celsius from the documented AWS daily aggregation.',
    },
    'wind_speed_10m': {
        'name': 'Daily mean wind speed at 10 m',
        'description': 'Official RMI/KMI provider-side daily mean wind speed at 10 meters in meters per second from the documented AWS daily aggregation.',
    },
    'humidity_rel_shelter_avg': {
        'name': 'Daily mean relative humidity',
        'description': 'Official RMI/KMI provider-side daily mean relative humidity in the shelter, in percent, from the documented AWS daily aggregation.',
    },
    'pressure': {
        'name': 'Daily mean station pressure',
        'description': 'Official RMI/KMI provider-side daily average atmospheric pressure at station level in hectopascal from the documented AWS daily aggregation.',
    },
    'sun_duration': {
        'name': 'Daily sunshine duration',
        'description': 'Official RMI/KMI provider-side daily sunshine duration in minutes from the documented AWS daily aggregation.',
    },
}

BE_HOURLY_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'precip_quantity': {
        'name': 'Hourly precipitation amount',
        'description': 'Official RMI/KMI provider-side hourly precipitation sum in millimeters from the documented aws_1hour aggregation.',
    },
    'temp_dry_shelter_avg': {
        'name': 'Hourly mean air temperature',
        'description': 'Official RMI/KMI provider-side hourly mean dry-bulb air temperature in degrees Celsius from the documented aws_1hour aggregation.',
    },
    'wind_speed_10m': {
        'name': 'Hourly mean wind speed at 10 m',
        'description': 'Official RMI/KMI provider-side hourly mean wind speed at 10 meters in meters per second from the documented aws_1hour aggregation.',
    },
    'humidity_rel_shelter_avg': {
        'name': 'Hourly mean relative humidity',
        'description': 'Official RMI/KMI provider-side hourly mean relative humidity under shelter in percent from the documented aws_1hour aggregation.',
    },
    'pressure': {
        'name': 'Hourly mean station pressure',
        'description': 'Official RMI/KMI provider-side hourly mean station pressure in hectopascal from the documented aws_1hour aggregation of the 10-minute pressure field.',
    },
    'sun_duration': {
        'name': 'Hourly sunshine duration',
        'description': 'Official RMI/KMI provider-side hourly sunshine duration in minutes from the documented aws_1hour aggregation.',
    },
}

BE_TENMIN_PARAMETER_METADATA: dict[str, dict[str, str]] = {
    'precip_quantity': {
        'name': '10-minute precipitation amount',
        'description': 'Official RMI/KMI precipitation amount in millimeters over the published aws_10min interval.',
    },
    'temp_dry_shelter_avg': {
        'name': '10-minute mean air temperature',
        'description': 'Official RMI/KMI 10-minute mean dry-bulb air temperature in degrees Celsius under shelter over the published aws_10min interval.',
    },
    'wind_speed_10m': {
        'name': '10-minute mean wind speed at 10 m',
        'description': 'Official RMI/KMI 10-minute mean wind speed at 10 meters in meters per second over the published aws_10min interval.',
    },
    'humidity_rel_shelter_avg': {
        'name': '10-minute mean relative humidity',
        'description': 'Official RMI/KMI 10-minute mean relative humidity under shelter in percent over the published aws_10min interval.',
    },
    'pressure': {
        'name': 'Station pressure',
        'description': 'Official RMI/KMI station pressure in hectopascal from the aws_10min layer; the source documents this field as a last-minute average at station level.',
    },
    'sun_duration': {
        'name': '10-minute sunshine duration',
        'description': 'Official RMI/KMI sunshine duration in minutes over the published aws_10min interval.',
    },
}

BE_PARAMETER_METADATA = {**BE_DAILY_PARAMETER_METADATA, **BE_HOURLY_PARAMETER_METADATA, **BE_TENMIN_PARAMETER_METADATA}


_BE_DATASET_SPECS = [
    BelgiumDatasetSpec(
        provider='historical',
        resolution='daily',
        label='RMI/KMI AWS historical daily observations',
        metadata_url=(
            f'{RMI_AWS_WFS_URL}?service=WFS&version=1.0.0&request=GetFeature'
            f'&typeName={RMI_AWS_STATION_LAYER}&outputFormat=application/json&srsName=EPSG:4326'
        ),
        data_url=RMI_AWS_WFS_URL,
        supported_elements=(
            'temp_avg',
            'temp_max',
            'temp_min',
            'precip_quantity',
            'wind_speed_10m',
            'humidity_rel_shelter_avg',
            'pressure',
            'sun_duration',
        ),
        canonical_elements=_BE_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
    BelgiumDatasetSpec(
        provider='historical',
        resolution='1hour',
        label='RMI/KMI AWS historical hourly observations',
        metadata_url=(
            f'{RMI_AWS_WFS_URL}?service=WFS&version=1.0.0&request=GetFeature'
            f'&typeName={RMI_AWS_STATION_LAYER}&outputFormat=application/json&srsName=EPSG:4326'
        ),
        data_url=RMI_AWS_WFS_URL,
        supported_elements=(
            'temp_dry_shelter_avg',
            'precip_quantity',
            'wind_speed_10m',
            'humidity_rel_shelter_avg',
            'pressure',
            'sun_duration',
        ),
        canonical_elements=_BE_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    BelgiumDatasetSpec(
        provider='historical',
        resolution='10min',
        label='RMI/KMI AWS historical 10-minute observations',
        metadata_url=(
            f'{RMI_AWS_WFS_URL}?service=WFS&version=1.0.0&request=GetFeature'
            f'&typeName={RMI_AWS_STATION_LAYER}&outputFormat=application/json&srsName=EPSG:4326'
        ),
        data_url=RMI_AWS_WFS_URL,
        supported_elements=(
            'temp_dry_shelter_avg',
            'precip_quantity',
            'wind_speed_10m',
            'humidity_rel_shelter_avg',
            'pressure',
            'sun_duration',
        ),
        canonical_elements=_BE_TENMIN_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[BelgiumDatasetSpec]:
    return list(_BE_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[BelgiumDatasetSpec]:
    return [spec for spec in _BE_DATASET_SPECS if spec.implemented]


def get_dataset_spec(provider: str, resolution: str) -> BelgiumDatasetSpec:
    normalized_scope = provider.strip()
    normalized_resolution = resolution.strip()
    for spec in _BE_DATASET_SPECS:
        if spec.provider == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported RMI/KMI Belgium dataset combination: {provider}/{resolution}')

