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
class LatviaDatasetSpec:
    provider: str
    resolution: str
    label: str
    metadata_api_url: str
    observation_sql_api_url: str
    station_resource_id: str
    parameter_resource_id: str
    archive_hourly_resource_id: str
    factual_archive_resource_id: str
    operational_resource_id: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


LV_CKAN_ACTION_BASE_URL = 'https://data.gov.lv/dati/api/action'
LV_CKAN_DATASTORE_SEARCH_URL = f'{LV_CKAN_ACTION_BASE_URL}/datastore_search'
LV_CKAN_DATASTORE_SEARCH_SQL_URL = f'{LV_CKAN_ACTION_BASE_URL}/datastore_search_sql'

LV_LVGMC_STATION_RESOURCE_ID = 'c32c7afd-0d05-44fd-8b24-1de85b4bf11d'
LV_LVGMC_PARAMETER_RESOURCE_ID = '38b462ac-08b9-4168-9d6e-cbaedc2e775d'
LV_LVGMC_ARCHIVE_HOURLY_RESOURCE_ID = 'ecc62e27-2071-483c-bca9-5e53d979faa8'
LV_LVGMC_FACTUAL_ARCHIVE_RESOURCE_ID = '339f73e4-20cf-4cea-be65-dcfd4b3b742c'
LV_LVGMC_OPERATIONAL_RESOURCE_ID = '17460efb-ae99-4d1d-8144-1068f184b05f'

LV_LVGMC_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('HTDRY',),
    'tas_max': ('HATMX',),
    'tas_min': ('HATMN',),
    'precipitation': ('HPRAB',),
    'wind_speed': ('HWNDS',),
    'wind_speed_max': ('HWSMX',),
    'relative_humidity': ('HRLH',),
    'pressure': ('HPRSL',),
    'snow_depth': ('HSNOW',),
}

LV_LVGMC_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('HTDRY',),
    'precipitation': ('HPRAB',),
    'wind_speed': ('HWNDS',),
    'wind_speed_max': ('HWSMX',),
    'relative_humidity': ('HRLH',),
    'pressure': ('HPRSL',),
    'snow_depth': ('HSNOW',),
}

LV_LVGMC_PARAMETER_METADATA = {
    'HTDRY': {
        'name': 'UTC-date mean air temperature',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly average air temperature records assigned by represented UTC hour.',
        'unit': 'degC',
    },
    'HATMX': {
        'name': 'UTC-date maximum air temperature',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly maximum air temperature records assigned by represented UTC hour.',
        'unit': 'degC',
    },
    'HATMN': {
        'name': 'UTC-date minimum air temperature',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly minimum air temperature records assigned by represented UTC hour.',
        'unit': 'degC',
    },
    'HPRAB': {
        'name': 'UTC-date precipitation sum',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly precipitation-sum records assigned by represented UTC hour.',
        'unit': 'mm',
    },
    'HWNDS': {
        'name': 'UTC-date mean wind speed',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly average wind-speed records assigned by represented UTC hour.',
        'unit': 'm/s',
    },
    'HWSMX': {
        'name': 'UTC-date maximum wind speed',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly maximum wind-speed records assigned by represented UTC hour.',
        'unit': 'm/s',
    },
    'HRLH': {
        'name': 'UTC-date mean relative humidity',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly average relative-humidity records assigned by represented UTC hour.',
        'unit': '%',
    },
    'HPRSL': {
        'name': 'UTC-date mean sea-level pressure',
        'description': 'Daily WeatherDownload aggregate from LVGMC hourly average sea-level pressure records assigned by represented UTC hour.',
        'unit': 'hPa',
    },
    'HSNOW': {
        'name': 'UTC-date last non-null snow depth',
        'description': 'Daily WeatherDownload aggregate from LVGMC snow-depth records using the last non-null value assigned to the UTC day.',
        'unit': 'cm',
    },
}

_LV_DATASET_SPECS = [
    LatviaDatasetSpec(
        provider='lvgmc',
        resolution='1hour',
        label='LVGMC recent meteorological archive hourly observations',
        metadata_api_url=LV_CKAN_DATASTORE_SEARCH_URL,
        observation_sql_api_url=LV_CKAN_DATASTORE_SEARCH_SQL_URL,
        station_resource_id=LV_LVGMC_STATION_RESOURCE_ID,
        parameter_resource_id=LV_LVGMC_PARAMETER_RESOURCE_ID,
        archive_hourly_resource_id=LV_LVGMC_ARCHIVE_HOURLY_RESOURCE_ID,
        factual_archive_resource_id=LV_LVGMC_FACTUAL_ARCHIVE_RESOURCE_ID,
        operational_resource_id=LV_LVGMC_OPERATIONAL_RESOURCE_ID,
        supported_elements=('HTDRY', 'HPRAB', 'HWNDS', 'HWSMX', 'HRLH', 'HPRSL', 'HSNOW'),
        time_semantics='datetime',
        canonical_elements=LV_LVGMC_HOURLY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
    LatviaDatasetSpec(
        provider='lvgmc',
        resolution='daily',
        label='LVGMC recent meteorological archive observations aggregated to daily UTC-date values',
        metadata_api_url=LV_CKAN_DATASTORE_SEARCH_URL,
        observation_sql_api_url=LV_CKAN_DATASTORE_SEARCH_SQL_URL,
        station_resource_id=LV_LVGMC_STATION_RESOURCE_ID,
        parameter_resource_id=LV_LVGMC_PARAMETER_RESOURCE_ID,
        archive_hourly_resource_id=LV_LVGMC_ARCHIVE_HOURLY_RESOURCE_ID,
        factual_archive_resource_id=LV_LVGMC_FACTUAL_ARCHIVE_RESOURCE_ID,
        operational_resource_id=LV_LVGMC_OPERATIONAL_RESOURCE_ID,
        supported_elements=('HTDRY', 'HATMX', 'HATMN', 'HPRAB', 'HWNDS', 'HWSMX', 'HRLH', 'HPRSL', 'HSNOW'),
        time_semantics='date',
        canonical_elements=LV_LVGMC_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
]

_GHCND_DATASET_SPECS = build_country_dataset_specs(
    supported_elements=('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD'),
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)


def list_dataset_specs() -> list[LatviaDatasetSpec | GhcndDatasetSpec]:
    return [*_LV_DATASET_SPECS, *list_country_dataset_specs(_GHCND_DATASET_SPECS)]


def list_implemented_dataset_specs() -> list[LatviaDatasetSpec | GhcndDatasetSpec]:
    return [
        *(spec for spec in _LV_DATASET_SPECS if spec.implemented),
        *list_country_implemented_dataset_specs(_GHCND_DATASET_SPECS),
    ]


def get_dataset_spec(provider: str, resolution: str) -> LatviaDatasetSpec | GhcndDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    if normalized_provider == 'ghcnd':
        return get_country_dataset_spec(_GHCND_DATASET_SPECS, normalized_provider, normalized_resolution)
    for spec in _LV_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Latvia dataset combination: {provider}/{resolution}')
