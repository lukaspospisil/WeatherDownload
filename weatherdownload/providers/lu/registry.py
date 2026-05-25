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
LU_METEOLUX_DAILY_CSV_URL = 'https://data.public.lu/en/datasets/r/a67bd8c0-b036-4761-b161-bdab272302e5'
LU_ASTA_DATASET_PAGE_URL = (
    'https://data.public.lu/en/datasets/'
    'inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-daily-weather-measurements-of-asta-1/'
)
LU_ASTA_STATION_METADATA_URL = (
    'https://data.public.lu/en/datasets/'
    'inspire-annex-iii-meteorological-geographical-features-spatial-sampling-features-location-of-weather-stations-managed-by-asta/'
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
    'sunshine_duration': ('DINS',),
}
LU_ASTA_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('avg_ta200',),
    'tas_max': ('max_ta200max',),
    'tas_min': ('min_ta200min',),
    'precipitation': ('sum_nn050',),
    'wind_speed': ('avg_wv200',),
    'relative_humidity': ('avg_rh200',),
    'sunshine_duration': ('sum_ssd',),
}

LU_DAILY_PARAMETER_METADATA = {
    'maxtemperature': {
        'name': 'Daily maximum air temperature',
        'description': 'Official MeteoLux Findel daily maximum air temperature at 2 m in degrees Celsius.',
        'unit': 'degC',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_FindelAirport_maxtemperature',
        'source_kind': 'wfs',
    },
    'mintemperature': {
        'name': 'Daily minimum air temperature',
        'description': 'Official MeteoLux Findel daily minimum air temperature at 2 m in degrees Celsius.',
        'unit': 'degC',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_FindelAirport_mintemperature',
        'source_kind': 'wfs',
    },
    'totalprecipitation': {
        'name': 'Daily precipitation amount',
        'description': (
            'Official MeteoLux Findel daily precipitation amount in millimeters. '
            'Observational days are defined from 06:00 UTC to 06:00 UTC of the following day.'
        ),
        'unit': 'mm',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_FindelAirport_totalprecipitation',
        'source_kind': 'wfs',
    },
    'DINS': {
        'name': 'Daily sunshine duration',
        'description': (
            'Official MeteoLux Findel daily sunshine duration from the MeteoLux daily CSV resource. '
            'The source labels DINS in hours and describes it as daily sunshine duration by observer.'
        ),
        'unit': 'hours',
        'csv_column': 'DINS',
        'source_kind': 'csv',
    },
}
LU_ASTA_DAILY_PARAMETER_METADATA = {
    'avg_ta200': {
        'name': 'Daily mean air temperature at 2 m',
        'description': (
            'Official ASTA daily average air temperature at 2 m from the Luxembourg agrometeorological station network. '
            'This conservative first slice exposes only the clearly identified 2 m temperature layers.'
        ),
        'unit': 'degC',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_ASTA_avg_ta200',
        'source_kind': 'wfs',
    },
    'max_ta200max': {
        'name': 'Daily maximum air temperature at 2 m',
        'description': 'Official ASTA daily maximum air temperature at 2 m from the Luxembourg agrometeorological station network.',
        'unit': 'degC',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_ASTA_max_ta200max',
        'source_kind': 'wfs',
    },
    'min_ta200min': {
        'name': 'Daily minimum air temperature at 2 m',
        'description': 'Official ASTA daily minimum air temperature at 2 m from the Luxembourg agrometeorological station network.',
        'unit': 'degC',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_ASTA_min_ta200min',
        'source_kind': 'wfs',
    },
    'sum_nn050': {
        'name': 'Daily precipitation amount',
        'description': (
            'Official ASTA daily precipitation amount, including snow and hail, '
            'from the Luxembourg agrometeorological station network.'
        ),
        'unit': 'mm',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_ASTA_sum_nn050',
        'source_kind': 'wfs',
    },
    'avg_wv200': {
        'name': 'Daily average wind speed at 2 m',
        'description': 'Official ASTA daily average wind speed at 2 m from the Luxembourg agrometeorological station network.',
        'unit': 'm/s',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_ASTA_avg_wv200',
        'source_kind': 'wfs',
    },
    'avg_rh200': {
        'name': 'Daily average relative humidity at 2 m',
        'description': 'Official ASTA daily average relative humidity at 2 m from the Luxembourg agrometeorological station network.',
        'unit': '%',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_ASTA_avg_rh200',
        'source_kind': 'wfs',
    },
    'sum_ssd': {
        'name': 'Daily measured sunshine duration',
        'description': (
            'Official ASTA daily measured sunshine duration from the Luxembourg agrometeorological station network. '
            'The measured sunshine layer is exposed here; the separate calculated sunshine layer is intentionally not mapped.'
        ),
        'unit': 'hours',
        'layer_name': 'MF.PointTimeSeriesObservation_Daily_ASTA_sum_ssd',
        'source_kind': 'wfs',
    },
}

_LU_DATASET_SPECS = [
    LuxembourgDatasetSpec(
        provider='meteolux',
        resolution='daily',
        label='MeteoLux daily Findel observations from INSPIRE WFS plus official MeteoLux daily CSV sunshine duration',
        station_metadata_url=LU_METEOLUX_DATASET_PAGE_URL,
        data_url=LU_METEOLUX_WFS_URL,
        supported_elements=('maxtemperature', 'mintemperature', 'totalprecipitation', 'DINS'),
        time_semantics='date',
        canonical_elements=LU_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
    LuxembourgDatasetSpec(
        provider='asta',
        resolution='daily',
        label='ASTA daily weather measurements from the Luxembourg agrometeorological station network',
        station_metadata_url=LU_ASTA_STATION_METADATA_URL,
        data_url=LU_METEOLUX_WFS_URL,
        supported_elements=('avg_ta200', 'max_ta200max', 'min_ta200min', 'sum_nn050', 'avg_wv200', 'avg_rh200', 'sum_ssd'),
        time_semantics='date',
        canonical_elements=LU_ASTA_DAILY_CANONICAL_ELEMENTS,
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
