from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpainDatasetSpec:
    provider: str
    resolution: str
    label: str
    station_metadata_url: str
    data_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


AEMET_OPEN_DATA_BASE_URL = 'https://opendata.aemet.es/opendata'
AEMET_STATION_INVENTORY_ENDPOINT = '/api/valores/climatologicos/inventarioestaciones/todasestaciones'
AEMET_DAILY_ENDPOINT_TEMPLATE = (
    '/api/valores/climatologicos/diarios/datos/fechaini/{fecha_ini}/fechafin/{fecha_fin}/estacion/{station_ids}'
)

AEMET_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('tmed',),
    'tas_max': ('tmax',),
    'tas_min': ('tmin',),
    'precipitation': ('prec',),
    'wind_speed': ('velmedia',),
    'sunshine_duration': ('sol',),
}

AEMET_DAILY_PARAMETER_METADATA = {
    'tmed': {
        'name': 'Daily mean air temperature',
        'description': 'Official AEMET daily mean air temperature in degrees Celsius.',
        'unit': 'degC',
    },
    'tmax': {
        'name': 'Daily maximum air temperature',
        'description': 'Official AEMET daily maximum air temperature in degrees Celsius.',
        'unit': 'degC',
    },
    'tmin': {
        'name': 'Daily minimum air temperature',
        'description': 'Official AEMET daily minimum air temperature in degrees Celsius.',
        'unit': 'degC',
    },
    'prec': {
        'name': 'Daily precipitation amount',
        'description': 'Official AEMET daily precipitation amount in millimetres. Trace precipitation "Ip" is normalized to 0.0 mm.',
        'unit': 'mm',
    },
    'velmedia': {
        'name': 'Daily mean wind speed',
        'description': 'Official AEMET daily mean wind speed. The source value is treated as kilometres per hour and converted to canonical metres per second.',
        'unit': 'm/s',
        'source_unit': 'km/h',
    },
    'sol': {
        'name': 'Daily sunshine duration',
        'description': 'Official AEMET daily sunshine duration in hours.',
        'unit': 'hours',
    },
}

_ES_DATASET_SPECS = [
    SpainDatasetSpec(
        provider='aemet',
        resolution='daily',
        label='AEMET OpenData daily climatological observations',
        station_metadata_url=f'{AEMET_OPEN_DATA_BASE_URL}{AEMET_STATION_INVENTORY_ENDPOINT}',
        data_url=f'{AEMET_OPEN_DATA_BASE_URL}{AEMET_DAILY_ENDPOINT_TEMPLATE}',
        supported_elements=('tmed', 'tmax', 'tmin', 'prec', 'velmedia', 'sol'),
        time_semantics='date',
        canonical_elements=AEMET_DAILY_CANONICAL_ELEMENTS,
        implemented=True,
    ),
]


def list_dataset_specs() -> list[SpainDatasetSpec]:
    return list(_ES_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[SpainDatasetSpec]:
    return [spec for spec in _ES_DATASET_SPECS if spec.implemented]


def get_dataset_spec(provider: str, resolution: str) -> SpainDatasetSpec:
    normalized_provider = provider.strip()
    normalized_resolution = resolution.strip()
    for spec in _ES_DATASET_SPECS:
        if spec.provider == normalized_provider and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported AEMET Spain dataset combination: {provider}/{resolution}')
