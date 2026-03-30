from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PolandDatasetSpec:
    dataset_scope: str
    resolution: str
    label: str
    station_metadata_url: str
    data_base_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


PL_BASE_URL = 'https://danepubliczne.imgw.pl/data/dane_pomiarowo_obserwacyjne/dane_meteorologiczne'
PL_DESCRIPTION_URL = f'{PL_BASE_URL}/Opis.txt'
PL_STATION_METADATA_URL = f'{PL_BASE_URL}/wykaz_stacji.csv'
PL_DAILY_SYNOP_BASE_URL = f'{PL_BASE_URL}/dobowe/synop'
PL_DAILY_KLIMAT_BASE_URL = f'{PL_BASE_URL}/dobowe/klimat'
PL_HOURLY_SYNOP_BASE_URL = f'{PL_BASE_URL}/terminowe/synop'

PL_DAILY_CANONICAL_ELEMENTS = {
    'tas_mean': ('STD',),
    'tas_max': ('TMAX',),
    'tas_min': ('TMIN',),
    'precipitation': ('SMDB',),
    'sunshine_duration': ('USL',),
}

PL_DAILY_KLIMAT_CANONICAL_ELEMENTS = {
    'tas_mean': ('STD',),
    'tas_max': ('TMAX',),
    'tas_min': ('TMIN',),
    'precipitation': ('SMDB',),
}

PL_HOURLY_CANONICAL_ELEMENTS = {
    'tas_mean': ('TEMP',),
    'wind_speed': ('FWR',),
    'wind_speed_max': ('PORW',),
    'relative_humidity': ('WLGW',),
    'vapour_pressure': ('CPW',),
    'pressure': ('PPPS',),
}


def _daily_parameter_metadata(raw_code: str, name: str, description: str) -> dict[str, str]:
    return {
        'name': name,
        'description': description,
        'obs_type': 'HISTORICAL_DAILY',
        'schedule': 'P1D IMGW daily',
        'raw_code': raw_code,
    }



def _hourly_parameter_metadata(raw_code: str, name: str, description: str) -> dict[str, str]:
    return {
        'name': name,
        'description': description,
        'obs_type': 'HISTORICAL_HOURLY',
        'schedule': 'PT1H IMGW terminowe synop',
        'raw_code': raw_code,
    }


PL_OBSERVATION_PARAMETER_METADATA = {
    'STD': _daily_parameter_metadata('STD', 'Daily mean air temperature', 'Official IMGW daily mean air temperature in the implemented Poland daily products.'),
    'TMAX': _daily_parameter_metadata('TMAX', 'Daily maximum air temperature', 'Official IMGW daily maximum air temperature in the implemented Poland daily products.'),
    'TMIN': _daily_parameter_metadata('TMIN', 'Daily minimum air temperature', 'Official IMGW daily minimum air temperature in the implemented Poland daily products.'),
    'SMDB': _daily_parameter_metadata('SMDB', 'Daily precipitation total', 'Official IMGW daily precipitation total in the implemented Poland daily products.'),
    'USL': _daily_parameter_metadata('USL', 'Daily sunshine duration', 'Official IMGW synop daily sunshine duration.'),
    'TEMP': _hourly_parameter_metadata('TEMP', 'Hourly air temperature', 'Official IMGW terminowe synop air temperature in degrees Celsius.'),
    'FWR': _hourly_parameter_metadata('FWR', 'Hourly wind speed', 'Official IMGW terminowe synop wind speed in metres per second.'),
    'PORW': _hourly_parameter_metadata('PORW', 'Hourly wind gust speed', 'Official IMGW terminowe synop wind gust speed in metres per second.'),
    'WLGW': _hourly_parameter_metadata('WLGW', 'Hourly relative humidity', 'Official IMGW terminowe synop relative humidity in percent.'),
    'CPW': _hourly_parameter_metadata('CPW', 'Hourly vapour pressure', 'Official IMGW terminowe synop water vapour pressure in hectopascal.'),
    'PPPS': _hourly_parameter_metadata('PPPS', 'Hourly station pressure', 'Official IMGW terminowe synop atmospheric pressure at station level in hectopascal.'),
}

PL_DAILY_SYNOP_PARAMETER_METADATA = {
    raw_code: PL_OBSERVATION_PARAMETER_METADATA[raw_code]
    for raw_code in ('STD', 'TMAX', 'TMIN', 'SMDB', 'USL')
}

PL_DAILY_KLIMAT_PARAMETER_METADATA = {
    raw_code: PL_OBSERVATION_PARAMETER_METADATA[raw_code]
    for raw_code in ('STD', 'TMAX', 'TMIN', 'SMDB')
}

PL_HOURLY_SYNOP_PARAMETER_METADATA = {
    raw_code: PL_OBSERVATION_PARAMETER_METADATA[raw_code]
    for raw_code in ('TEMP', 'FWR', 'PORW', 'WLGW', 'CPW', 'PPPS')
}

_PL_DATASET_SPECS = [
    PolandDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        label='IMGW-PIB historical daily synop station observations',
        station_metadata_url=PL_STATION_METADATA_URL,
        data_base_url=PL_DAILY_SYNOP_BASE_URL,
        supported_elements=tuple(PL_DAILY_SYNOP_PARAMETER_METADATA),
        canonical_elements=PL_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
    PolandDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        label='IMGW-PIB historical hourly synop station observations',
        station_metadata_url=PL_STATION_METADATA_URL,
        data_base_url=PL_HOURLY_SYNOP_BASE_URL,
        supported_elements=tuple(PL_HOURLY_SYNOP_PARAMETER_METADATA),
        canonical_elements=PL_HOURLY_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    PolandDatasetSpec(
        dataset_scope='historical_klimat',
        resolution='daily',
        label='IMGW-PIB historical daily klimat station observations',
        station_metadata_url=PL_STATION_METADATA_URL,
        data_base_url=PL_DAILY_KLIMAT_BASE_URL,
        supported_elements=tuple(PL_DAILY_KLIMAT_PARAMETER_METADATA),
        canonical_elements=PL_DAILY_KLIMAT_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
]


def list_dataset_specs() -> list[PolandDatasetSpec]:
    return list(_PL_DATASET_SPECS)



def list_implemented_dataset_specs() -> list[PolandDatasetSpec]:
    return [spec for spec in _PL_DATASET_SPECS if spec.implemented]



def get_dataset_spec(dataset_scope: str, resolution: str) -> PolandDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    for spec in _PL_DATASET_SPECS:
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution:
            return spec
    raise ValueError(f'Unsupported IMGW Poland dataset combination: {dataset_scope}/{resolution}')
