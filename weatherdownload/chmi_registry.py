from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True, slots=True)
class ChmiDatasetSpec:
    dataset_scope: str
    resolution: str
    endpoint_pattern: str | None
    supported_elements: tuple[str, ...]
    station_identifier_type: str
    time_semantics: str
    implemented: bool
    element_groups: Mapping[str, str] | None = None


_DAILY_HISTORICAL_CSV_ELEMENT_GROUPS: dict[str, str] = {
    'E': 'humidity',
    'F': 'wind',
    'HS': 'snow',
    'P': 'air_pressure',
    'RH': 'humidity',
    'SRA': 'precipitation',
    'SSV': 'sunshine',
    'T': 'temperature',
    'TMA': 'temperature',
    'TMI': 'temperature',
    'WDIR': 'wind',
    'WSPD': 'wind',
}

_TENMIN_HISTORICAL_CSV_ELEMENT_GROUPS: dict[str, str] = {
    'T': 'temperature',
    'TMA': 'temperature',
    'TMI': 'temperature',
    'TPM': 'temperature',
    'T10': 'soil_temperature',
    'T100': 'soil_temperature',
    'SSV10M': 'sunshine',
}

_HOURLY_HISTORICAL_CSV_ELEMENT_GROUPS: dict[str, str] = {
    'E': 'humidity',
    'P': 'synop',
    'N': 'synop',
    'W1': 'synop',
    'W2': 'synop',
    'SSV1H': 'sunshine',
}


def _build_spec(
    dataset_scope: str,
    resolution: str,
    *,
    supported_elements: tuple[str, ...] = (),
    station_identifier_type: str = 'wsi',
    time_semantics: str = 'datetime',
    implemented: bool = False,
    endpoint_pattern: str | None = None,
    element_groups: Mapping[str, str] | None = None,
) -> ChmiDatasetSpec:
    effective_supported_elements = tuple(element_groups.keys()) if element_groups is not None else supported_elements
    return ChmiDatasetSpec(
        dataset_scope=dataset_scope,
        resolution=resolution,
        endpoint_pattern=endpoint_pattern,
        supported_elements=effective_supported_elements,
        station_identifier_type=station_identifier_type,
        time_semantics=time_semantics,
        implemented=implemented,
        element_groups=element_groups,
    )


_DATASET_REGISTRY: dict[tuple[str, str], ChmiDatasetSpec] = {
    ('now', '10min'): _build_spec('now', '10min'),
    ('now', '1hour'): _build_spec('now', '1hour'),
    ('now', 'daily'): _build_spec('now', 'daily', time_semantics='date'),
    ('now', 'phenomena'): _build_spec('now', 'phenomena'),
    ('recent', '10min'): _build_spec('recent', '10min'),
    ('recent', '1hour'): _build_spec('recent', '1hour'),
    ('recent', 'daily'): _build_spec('recent', 'daily', time_semantics='date'),
    ('recent', 'monthly'): _build_spec('recent', 'monthly', time_semantics='date'),
    ('recent', 'phenomena'): _build_spec('recent', 'phenomena'),
    ('historical', '10min'): _build_spec('historical', '10min'),
    ('historical', '1hour'): _build_spec('historical', '1hour'),
    ('historical', 'daily'): _build_spec('historical', 'daily', time_semantics='date'),
    ('historical', 'monthly'): _build_spec('historical', 'monthly', time_semantics='date'),
    ('historical', 'yearly'): _build_spec('historical', 'yearly', time_semantics='date'),
    ('historical', 'phenomena'): _build_spec('historical', 'phenomena'),
    ('historical_csv', '10min'): _build_spec(
        'historical_csv',
        '10min',
        implemented=True,
        endpoint_pattern='https://opendata.chmi.cz/meteorology/climate/historical_csv/data/10min/{group}/{year}/10m-{station_id}-{element}-{year_month}.csv',
        element_groups=_TENMIN_HISTORICAL_CSV_ELEMENT_GROUPS,
    ),
    ('historical_csv', '1hour'): _build_spec(
        'historical_csv',
        '1hour',
        implemented=True,
        endpoint_pattern='https://opendata.chmi.cz/meteorology/climate/historical_csv/data/1hour/{group}/{year}/1h-{station_id}-{element}-{year_month}.csv',
        element_groups=_HOURLY_HISTORICAL_CSV_ELEMENT_GROUPS,
    ),
    ('historical_csv', 'daily'): _build_spec(
        'historical_csv',
        'daily',
        time_semantics='date',
        implemented=True,
        endpoint_pattern='https://opendata.chmi.cz/meteorology/climate/historical_csv/data/daily/{group}/dly-{station_id}-{element}.csv',
        element_groups=_DAILY_HISTORICAL_CSV_ELEMENT_GROUPS,
    ),
    ('historical_csv', 'monthly'): _build_spec('historical_csv', 'monthly', time_semantics='date'),
    ('historical_csv', 'pentadic'): _build_spec('historical_csv', 'pentadic', time_semantics='date'),
    ('historical_csv', 'yearly'): _build_spec('historical_csv', 'yearly', time_semantics='date'),
    ('historical_csv', 'phenomena'): _build_spec('historical_csv', 'phenomena'),
}


def get_dataset_spec(dataset_scope: str, resolution: str) -> ChmiDatasetSpec:
    key = (dataset_scope.strip(), resolution.strip())
    try:
        return _DATASET_REGISTRY[key]
    except KeyError as exc:
        raise ValueError(
            f"No CHMI dataset spec is registered for dataset_scope='{dataset_scope}' and resolution='{resolution}'."
        ) from exc


def list_dataset_specs() -> list[ChmiDatasetSpec]:
    return list(_DATASET_REGISTRY.values())


def list_implemented_dataset_specs() -> list[ChmiDatasetSpec]:
    return [spec for spec in _DATASET_REGISTRY.values() if spec.implemented]
