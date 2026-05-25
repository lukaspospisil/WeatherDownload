from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path


@dataclass(frozen=True)
class IrelandDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


IE_METEIREANN_DAILY_DATA_PAGE_URL = 'https://www.met.ie/climate/available-data/daily-data'
IE_METEIREANN_HISTORICAL_DATA_PAGE_URL = 'https://www.met.ie/climate/available-data/historical-data%C2%A0'
IE_METEIREANN_STATION_DETAILS_URL = 'https://clidata.met.ie/cli/climate_data/webdata/StationDetails.csv'
IE_METEIREANN_DAILY_CSV_URL_TEMPLATE = 'https://clidata.met.ie/cli/climate_data/webdata/dly{station_id}.csv'
IE_METEIREANN_KEY_DAILY_URL = 'https://opendata2.met.ie/opendata2/docs/KeyDaily.txt'
IE_AUDITED_DAILY_STATIONS_PATH = Path(__file__).with_name('daily_stations.json')
IE_AUDITED_DAILY_STATIONS_RESOURCE = 'daily_stations.json'
IE_AUDIT_REQUIRED_RAW_COLUMNS = ('date', 'maxtp', 'mintp', 'rain', 'wdsp', 'sun')

IE_DAILY_CANONICAL_ELEMENTS = {
    'tas_max': ('maxtp',),
    'tas_min': ('mintp',),
    'precipitation': ('rain',),
    'wind_speed': ('wdsp',),
    'sunshine_duration': ('sun',),
}

IE_DAILY_PARAMETER_METADATA = {
    'maxtp': {
        'name': 'Daily maximum air temperature',
        'description': 'Official Met Eireann daily maximum air temperature in degrees Celsius.',
        'unit': 'degC',
    },
    'mintp': {
        'name': 'Daily minimum air temperature',
        'description': 'Official Met Eireann daily minimum air temperature in degrees Celsius.',
        'unit': 'degC',
    },
    'rain': {
        'name': 'Daily precipitation amount',
        'description': 'Official Met Eireann daily precipitation amount in millimetres.',
        'unit': 'mm',
    },
    'wdsp': {
        'name': 'Daily mean wind speed',
        'description': 'Official Met Eireann daily mean wind speed. The source CSV labels wdsp in knots; WeatherDownload converts it to canonical m/s.',
        'unit': 'm/s',
        'source_unit': 'knots',
    },
    'sun': {
        'name': 'Daily sunshine duration',
        'description': 'Official Met Eireann daily sunshine duration in hours.',
        'unit': 'hours',
    },
}

_IE_DATASET_SPECS = [
    IrelandDatasetSpec(
        provider='meteireann',
        resolution='daily',
        label='Met Eireann official daily station CSV observations (station-metadata-driven conservative verified daily subset)',
        station_metadata_url=IE_METEIREANN_STATION_DETAILS_URL,
        data_url=IE_METEIREANN_DAILY_CSV_URL_TEMPLATE,
        supported_elements=('maxtp', 'mintp', 'rain', 'wdsp', 'sun'),
        time_semantics='date',
        canonical_elements=IE_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
]


def list_dataset_specs() -> list[IrelandDatasetSpec]:
    return list(_IE_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[IrelandDatasetSpec]:
    return [spec for spec in _IE_DATASET_SPECS if spec.implemented]


def get_dataset_spec(provider: str, resolution: str) -> IrelandDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    for spec in _IE_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported Ireland dataset combination: {provider}/{resolution}')


def read_audited_daily_stations_text() -> str:
    return files('weatherdownload.providers.ie').joinpath(IE_AUDITED_DAILY_STATIONS_RESOURCE).read_text(encoding='utf-8')
