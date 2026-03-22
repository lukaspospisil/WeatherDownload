from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DwdDatasetSpec:
    dataset_scope: str
    resolution: str
    source_id: str
    label: str
    metadata_url: str
    supported_elements: tuple[str, ...]
    time_semantics: str
    implemented: bool = False
    canonical_elements: dict[str, tuple[str, ...]] | None = None


_DWD_DAILY_CANONICAL_ELEMENTS = {
    'wind_speed_max': ('FX',),
    'wind_speed': ('FM',),
    'precipitation': ('RSK',),
    'precipitation_indicator': ('RSKF',),
    'sunshine_duration': ('SDK',),
    'snow_depth': ('SHK_TAG',),
    'cloud_cover': ('NM',),
    'vapour_pressure': ('VPM',),
    'pressure': ('PM',),
    'tas_mean': ('TMK',),
    'relative_humidity': ('UPM',),
    'tas_max': ('TXK',),
    'tas_min': ('TNK',),
    'ground_temperature_min': ('TGK',),
}

_DWD_HOURLY_AIR_TEMPERATURE_CANONICAL_ELEMENTS = {
    'tas_mean': ('TT_TU',),
    'relative_humidity': ('RF_TU',),
}

_DWD_HOURLY_WIND_CANONICAL_ELEMENTS = {
    'wind_speed': ('FF',),
}

_DWD_HOURLY_PRECIPITATION_CANONICAL_ELEMENTS = {
    'precipitation': ('R1',),
    'precipitation_indicator': ('RS_IND',),
    'precipitation_form': ('WRTR',),
}

_DWD_TENMIN_AIR_TEMPERATURE_CANONICAL_ELEMENTS = {
    'tas_mean': ('TT_10',),
    'relative_humidity': ('RF_10',),
}

_DWD_TENMIN_WIND_CANONICAL_ELEMENTS = {
    'wind_speed': ('FF_10',),
}

_DWD_TENMIN_PRECIPITATION_CANONICAL_ELEMENTS = {
    'precipitation_duration': ('RWS_DAU_10',),
    'precipitation': ('RWS_10',),
    'precipitation_indicator': ('RWS_IND_10',),
}

_DWD_DATASET_SPECS = [
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        source_id='daily_kl',
        label='DWD daily climate summary',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/KL_Tageswerte_Beschreibung_Stationen.txt',
        supported_elements=('FX', 'FM', 'RSK', 'RSKF', 'SDK', 'SHK_TAG', 'NM', 'VPM', 'PM', 'TMK', 'UPM', 'TXK', 'TNK', 'TGK'),
        canonical_elements=_DWD_DAILY_CANONICAL_ELEMENTS,
        time_semantics='date',
        implemented=True,
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        source_id='hourly_air_temperature',
        label='DWD hourly air temperature',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/TU_Stundenwerte_Beschreibung_Stationen.txt',
        supported_elements=('TT_TU', 'RF_TU'),
        canonical_elements=_DWD_HOURLY_AIR_TEMPERATURE_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        source_id='hourly_wind',
        label='DWD hourly wind',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/FF_Stundenwerte_Beschreibung_Stationen.txt',
        supported_elements=('FF',),
        canonical_elements=_DWD_HOURLY_WIND_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        source_id='hourly_precipitation',
        label='DWD hourly precipitation',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/RR_Stundenwerte_Beschreibung_Stationen.txt',
        supported_elements=('R1', 'RS_IND', 'WRTR'),
        canonical_elements=_DWD_HOURLY_PRECIPITATION_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=False,
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        source_id='tenmin_air_temperature',
        label='DWD 10-minute air temperature',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/zehn_min_tu_Beschreibung_Stationen.txt',
        supported_elements=('TT_10', 'RF_10'),
        canonical_elements=_DWD_TENMIN_AIR_TEMPERATURE_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        source_id='tenmin_wind',
        label='DWD 10-minute wind',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/historical/zehn_min_ff_Beschreibung_Stationen.txt',
        supported_elements=('FF_10',),
        canonical_elements=_DWD_TENMIN_WIND_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=True,
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        source_id='tenmin_precipitation',
        label='DWD 10-minute precipitation',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/historical/zehn_min_rr_Beschreibung_Stationen.txt',
        supported_elements=('RWS_DAU_10', 'RWS_10', 'RWS_IND_10'),
        canonical_elements=_DWD_TENMIN_PRECIPITATION_CANONICAL_ELEMENTS,
        time_semantics='datetime',
        implemented=False,
    ),
]


def list_dataset_specs() -> list[DwdDatasetSpec]:
    return list(_DWD_DATASET_SPECS)


def list_implemented_dataset_specs() -> list[DwdDatasetSpec]:
    implemented_pairs: dict[tuple[str, str], DwdDatasetSpec] = {}
    for spec in _DWD_DATASET_SPECS:
        if not spec.implemented:
            continue
        implemented_pairs[(spec.dataset_scope, spec.resolution)] = get_dataset_spec(spec.dataset_scope, spec.resolution)
    return list(implemented_pairs.values())


def get_dataset_spec(dataset_scope: str, resolution: str) -> DwdDatasetSpec:
    normalized_scope = dataset_scope.strip()
    normalized_resolution = resolution.strip()
    matching = [
        spec
        for spec in _DWD_DATASET_SPECS
        if spec.dataset_scope == normalized_scope and spec.resolution == normalized_resolution
    ]
    if not matching:
        raise ValueError(f'Unsupported DWD dataset combination: {dataset_scope}/{resolution}')

    implemented_matching = [spec for spec in matching if spec.implemented]
    effective_specs = implemented_matching or matching
    combined_elements = tuple(sorted({element for spec in effective_specs for element in spec.supported_elements}))
    combined_canonical_elements: dict[str, tuple[str, ...]] = {}
    for spec in effective_specs:
        for canonical_name, raw_codes in (spec.canonical_elements or {}).items():
            existing = combined_canonical_elements.get(canonical_name, ())
            combined_canonical_elements[canonical_name] = tuple(dict.fromkeys(existing + tuple(raw_codes)))
    return DwdDatasetSpec(
        dataset_scope=normalized_scope,
        resolution=normalized_resolution,
        source_id=f'{normalized_scope}_{normalized_resolution}',
        label=f'DWD {normalized_scope} {normalized_resolution}',
        metadata_url='',
        supported_elements=combined_elements,
        canonical_elements=combined_canonical_elements,
        time_semantics=effective_specs[0].time_semantics,
        implemented=any(spec.implemented for spec in matching),
    )
