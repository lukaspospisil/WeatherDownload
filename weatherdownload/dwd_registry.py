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


_DWD_DATASET_SPECS = [
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        source_id='daily_kl',
        label='DWD daily climate summary',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/KL_Tageswerte_Beschreibung_Stationen.txt',
        supported_elements=('FX', 'FM', 'RSK', 'RSKF', 'SDK', 'SHK_TAG', 'NM', 'VPM', 'PM', 'TMK', 'UPM', 'TXK', 'TNK', 'TGK'),
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
        time_semantics='datetime',
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        source_id='hourly_wind',
        label='DWD hourly wind',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/FF_Stundenwerte_Beschreibung_Stationen.txt',
        supported_elements=('FF', 'DD'),
        time_semantics='datetime',
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='1hour',
        source_id='hourly_precipitation',
        label='DWD hourly precipitation',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/RR_Stundenwerte_Beschreibung_Stationen.txt',
        supported_elements=('R1', 'RS_IND', 'WRTR'),
        time_semantics='datetime',
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        source_id='tenmin_air_temperature',
        label='DWD 10-minute air temperature',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/zehn_min_tu_Beschreibung_Stationen.txt',
        supported_elements=('PP_10', 'TT_10', 'TM5_10', 'RF_10', 'TD_10'),
        time_semantics='datetime',
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        source_id='tenmin_wind',
        label='DWD 10-minute wind',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/historical/zehn_min_ff_Beschreibung_Stationen.txt',
        supported_elements=('FF_10', 'DD_10'),
        time_semantics='datetime',
    ),
    DwdDatasetSpec(
        dataset_scope='historical',
        resolution='10min',
        source_id='tenmin_precipitation',
        label='DWD 10-minute precipitation',
        metadata_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/historical/zehn_min_rr_Beschreibung_Stationen.txt',
        supported_elements=('RWS_DAU_10', 'RWS_10', 'RWS_IND_10'),
        time_semantics='datetime',
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
    combined_elements = tuple(sorted({element for spec in matching for element in spec.supported_elements}))
    return DwdDatasetSpec(
        dataset_scope=normalized_scope,
        resolution=normalized_resolution,
        source_id=f'{normalized_scope}_{normalized_resolution}',
        label=f'DWD {normalized_scope} {normalized_resolution}',
        metadata_url='',
        supported_elements=combined_elements,
        time_semantics=matching[0].time_semantics,
        implemented=any(spec.implemented for spec in matching),
    )
