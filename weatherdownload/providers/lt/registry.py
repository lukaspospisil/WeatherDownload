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
class LithuaniaDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    station_observation_range_url_template: str
    observation_url_template: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


LT_METEO_LT_BASE_URL = 'https://api.meteo.lt/v1'
LT_METEO_LT_STATIONS_URL = f'{LT_METEO_LT_BASE_URL}/stations'
LT_METEO_LT_OBSERVATIONS_RANGE_URL_TEMPLATE = f'{LT_METEO_LT_BASE_URL}/stations/{{station_code}}/observations'
LT_METEO_LT_DAILY_URL_TEMPLATE = f'{LT_METEO_LT_BASE_URL}/stations/{{station_code}}/observations/{{observation_date}}'

LT_METEO_LT_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('airTemperature_mean',),
    'tas_max': ('airTemperature_max',),
    'tas_min': ('airTemperature_min',),
    'precipitation': ('precipitation',),
    'wind_speed': ('windSpeed',),
    'wind_speed_max': ('windGust',),
    'relative_humidity': ('relativeHumidity',),
    'pressure': ('seaLevelPressure',),
    'snow_depth': ('snowDepth',),
    'cloud_cover': ('cloudCover',),
}

LT_METEO_LT_PARAMETER_METADATA = {
    'airTemperature_mean': {
        'name': 'UTC-date mean air temperature',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt intra-day observed airTemperature values for one UTC date using the mean over non-null records.',
        'unit': 'degC',
    },
    'airTemperature_max': {
        'name': 'UTC-date maximum air temperature',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt intra-day observed airTemperature values for one UTC date using the maximum over non-null records.',
        'unit': 'degC',
    },
    'airTemperature_min': {
        'name': 'UTC-date minimum air temperature',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt intra-day observed airTemperature values for one UTC date using the minimum over non-null records.',
        'unit': 'degC',
    },
    'precipitation': {
        'name': 'UTC-date precipitation sum',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt hourly precipitation observation values for one UTC date.',
        'unit': 'mm',
    },
    'windSpeed': {
        'name': 'UTC-date mean wind speed',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt observed windSpeed values for one UTC date.',
        'unit': 'm/s',
    },
    'windGust': {
        'name': 'UTC-date maximum wind gust',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt observed windGust values for one UTC date.',
        'unit': 'm/s',
    },
    'relativeHumidity': {
        'name': 'UTC-date mean relative humidity',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt observed relativeHumidity values for one UTC date.',
        'unit': '%',
    },
    'seaLevelPressure': {
        'name': 'UTC-date mean sea-level pressure',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt observed seaLevelPressure values for one UTC date.',
        'unit': 'hPa',
    },
    'snowDepth': {
        'name': 'UTC-date last non-null snow depth',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt observed snowDepth values for one UTC date using the last non-null UTC reading.',
        'unit': 'cm',
    },
    'cloudCover': {
        'name': 'UTC-date mean cloud cover',
        'description': 'Daily WeatherDownload aggregate from Meteo.lt observed cloudCover values for one UTC date.',
        'unit': '%',
    },
}

_LT_DATASET_SPECS = [
    LithuaniaDatasetSpec(
        provider='meteo_lt',
        resolution='daily',
        label='LHMT Meteo.lt meteorological station observations aggregated to daily UTC-date values',
        station_metadata_url=LT_METEO_LT_STATIONS_URL,
        station_observation_range_url_template=LT_METEO_LT_OBSERVATIONS_RANGE_URL_TEMPLATE,
        observation_url_template=LT_METEO_LT_DAILY_URL_TEMPLATE,
        supported_elements=(
            'airTemperature_mean',
            'airTemperature_max',
            'airTemperature_min',
            'precipitation',
            'windSpeed',
            'windGust',
            'relativeHumidity',
            'seaLevelPressure',
            'snowDepth',
            'cloudCover',
        ),
        time_semantics='date',
        canonical_elements=LT_METEO_LT_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[LithuaniaDatasetSpec | GhcndDatasetSpec]:
    return [*_LT_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[LithuaniaDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _LT_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> LithuaniaDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _LT_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Lithuania dataset combination: {provider}/{resolution}')
