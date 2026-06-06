from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AndorraDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    station_detail_url_template: str
    daily_variables_url: str
    daily_export_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


AD_BASE_URL = 'https://www.meteo.ad'
AD_CLIMATOLOGY_URL = f'{AD_BASE_URL}/climatologia'
AD_STATION_DETAIL_URL_TEMPLATE = f'{AD_BASE_URL}/estacions/{{station_id}}'
AD_DAILY_VARIABLES_URL = f'{AD_BASE_URL}/Climatologia/GetDadesMesuraEstacio'
AD_DAILY_EXPORT_URL = f'{AD_BASE_URL}/climatologia/list2xls'

AD_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('temp_mitjana',),
    'tas_max': ('temp_max',),
    'tas_min': ('temp_min',),
    'precipitation': ('prec_total',),
    'wind_speed': ('vel_vent_mitjana',),
    'wind_speed_max': ('vel_vent_max',),
    'relative_humidity': ('hum_mitjana',),
    'sunshine_duration': ('insolacio_total',),
    'solar_radiation': ('irradiacio_total',),
}

AD_DAILY_PARAMETER_METADATA = {
    'temp_mitjana': {
        'name': 'Daily mean air temperature',
        'description': 'Official Meteo.ad daily mean air temperature from the public climatology export in degrees Celsius.',
        'unit': 'degC',
    },
    'temp_max': {
        'name': 'Daily maximum air temperature',
        'description': 'Official Meteo.ad daily maximum air temperature from the public climatology export in degrees Celsius.',
        'unit': 'degC',
    },
    'temp_min': {
        'name': 'Daily minimum air temperature',
        'description': 'Official Meteo.ad daily minimum air temperature from the public climatology export in degrees Celsius.',
        'unit': 'degC',
    },
    'prec_total': {
        'name': 'Daily precipitation total',
        'description': 'Official Meteo.ad daily precipitation total from the public climatology export in millimetres.',
        'unit': 'mm',
    },
    'vel_vent_mitjana': {
        'name': 'Daily mean wind speed',
        'description': 'Official Meteo.ad daily mean wind speed from the public climatology export in metres per second.',
        'unit': 'm/s',
    },
    'vel_vent_max': {
        'name': 'Daily maximum wind speed',
        'description': 'Official Meteo.ad daily maximum wind speed from the public climatology export in metres per second.',
        'unit': 'm/s',
    },
    'hum_mitjana': {
        'name': 'Daily mean relative humidity',
        'description': 'Official Meteo.ad daily mean relative humidity from the public climatology export in percent.',
        'unit': '%',
    },
    'insolacio_total': {
        'name': 'Daily sunshine duration',
        'description': 'Official Meteo.ad daily sunshine duration from the public climatology export. Source minutes are normalized to canonical hours.',
        'unit': 'hours',
        'source_unit': 'minutes',
    },
    'irradiacio_total': {
        'name': 'Daily solar radiation',
        'description': 'Official Meteo.ad daily solar radiation from the public climatology export. Source joules per square metre are normalized to canonical MJ m^-2.',
        'unit': 'MJ m^-2',
        'source_unit': 'J m^-2',
    },
}

_AD_DATASET_SPECS = [
    AndorraDatasetSpec(
        provider='meteo_ad',
        resolution='daily',
        label='Meteo.ad public daily climatology export',
        station_metadata_url=AD_CLIMATOLOGY_URL,
        station_detail_url_template=AD_STATION_DETAIL_URL_TEMPLATE,
        daily_variables_url=AD_DAILY_VARIABLES_URL,
        daily_export_url=AD_DAILY_EXPORT_URL,
        supported_elements=tuple(AD_DAILY_PARAMETER_METADATA),
        time_semantics='date',
        canonical_elements=AD_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
]


def list_dataset_specs() -> list[AndorraDatasetSpec]:
    return list(_AD_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[AndorraDatasetSpec]:
    return [spec for spec in _AD_DATASET_SPECS if spec.implemented]


def get_dataset_spec(provider: str, resolution: str) -> AndorraDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    for spec in _AD_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Meteo.ad Andorra dataset combination: {provider}/{resolution}')
