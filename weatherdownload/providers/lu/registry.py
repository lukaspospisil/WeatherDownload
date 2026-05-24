from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LuxembourgDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


LU_METEOLUX_WFS_URL = 'https://wms.inspire.geoportail.lu/geoserver/mf/wfs'
LU_METEOLUX_DATASET_PAGE_URL = (
    'https://data.public.lu/en/datasets/'
    'inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-daily-weather-measurements-at-luxembourg-findel-airport/'
)
LU_METEOLUX_STATION_ID = '0-20000-0-06590'
LU_METEOLUX_STATION_NAME = 'Luxembourg/Findel Airport'
LU_METEOLUX_STATION_LATITUDE = 49.63265182
LU_METEOLUX_STATION_LONGITUDE = 6.232928668
LU_METEOLUX_STATION_ELEVATION_M = 376.1

LU_DAILY_CANONICAL_ELEMENTS = {
    'tas_max': ('maxtemperature',),
    'tas_min': ('mintemperature',),
    'precipitation': ('totalprecipitation',),
}

LU_DAILY_PARAMETER_METADATA = {
    'maxtemperature': {
        'name': 'Daily maximum air temperature',
        'description': 'Official MeteoLux Findel daily maximum air temperature at 2 m in degrees Celsius.',
        'unit': 'degC',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_FindelAirport_maxtemperature',
    },
    'mintemperature': {
        'name': 'Daily minimum air temperature',
        'description': 'Official MeteoLux Findel daily minimum air temperature at 2 m in degrees Celsius.',
        'unit': 'degC',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_FindelAirport_mintemperature',
    },
    'totalprecipitation': {
        'name': 'Daily precipitation amount',
        'description': (
            'Official MeteoLux Findel daily precipitation amount in millimeters. '
            'Observational days are defined from 06:00 UTC to 06:00 UTC of the following day.'
        ),
        'unit': 'mm',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_FindelAirport_totalprecipitation',
    },
}

_LU_DATASET_SPECS = [
    LuxembourgDatasetSpec(
        provider='meteolux',
        resolution='daily',
        label='MeteoLux INSPIRE WFS daily weather measurements at Luxembourg Findel Airport',
        station_metadata_url=LU_METEOLUX_DATASET_PAGE_URL,
        data_url=LU_METEOLUX_WFS_URL,
        supported_elements=('maxtemperature', 'mintemperature', 'totalprecipitation'),
        time_semantics='date',
        canonical_elements=LU_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
]


def list_dataset_specs() -> list[LuxembourgDatasetSpec]:
    return list(_LU_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[LuxembourgDatasetSpec]:
    return [spec for spec in _LU_DATASET_SPECS if spec.implemented]


def get_dataset_spec(provider: str, resolution: str) -> LuxembourgDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    for spec in _LU_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Luxembourg dataset combination: {provider}/{resolution}')
