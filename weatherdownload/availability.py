from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime

import pandas as pd

from .elements import element_mapping_dict_for_spec, element_mapping_for_spec, supported_elements_for_spec
from .metadata import filter_stations

AVAILABILITY_COLUMNS = [
    'station_id',
    'gh_id',
    'dataset_scope',
    'resolution',
    'implemented',
    'supported_elements',
]


def station_availability(
    stations: pd.DataFrame,
    station_ids: Sequence[str] | None = None,
    active_on: date | datetime | str | None = None,
    implemented_only: bool = True,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_element_mapping: bool = False,
) -> pd.DataFrame:
    from .providers import get_provider

    provider = get_provider(country)
    filtered = filter_stations(stations, station_ids=station_ids, active_on=active_on)
    specs = provider.list_implemented_dataset_specs() if implemented_only else []
    rows: list[dict[str, object]] = []

    for station in filtered.itertuples(index=False):
        for spec in specs:
            row = {
                'station_id': station.station_id,
                'gh_id': station.gh_id,
                'dataset_scope': spec.dataset_scope,
                'resolution': spec.resolution,
                'implemented': spec.implemented,
                'supported_elements': supported_elements_for_spec(spec, provider_raw=provider_raw),
            }
            if include_element_mapping:
                row['supported_element_mapping'] = element_mapping_dict_for_spec(spec)
            rows.append(row)

    columns = AVAILABILITY_COLUMNS + (['supported_element_mapping'] if include_element_mapping else [])
    return pd.DataFrame.from_records(rows, columns=columns)


def station_supports(
    stations: pd.DataFrame,
    station_id: str,
    dataset_scope: str,
    resolution: str,
    active_on: date | datetime | str | None = None,
    country: str = 'CZ',
) -> bool:
    from .providers import get_provider

    provider = get_provider(country)
    spec = provider.get_dataset_spec(dataset_scope, resolution)
    if not spec.implemented:
        return False
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
        country=country,
    )
    return not availability[
        (availability['dataset_scope'] == spec.dataset_scope)
        & (availability['resolution'] == spec.resolution)
    ].empty


def list_station_paths(
    stations: pd.DataFrame,
    station_id: str,
    active_on: date | datetime | str | None = None,
    include_elements: bool = False,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_element_mapping: bool = False,
) -> pd.DataFrame:
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
        country=country,
        provider_raw=provider_raw,
        include_element_mapping=include_element_mapping,
    )
    if availability.empty:
        return availability
    if include_elements or include_element_mapping:
        return availability.reset_index(drop=True)
    return availability.drop(columns=['supported_elements']).reset_index(drop=True)


def list_station_elements(
    stations: pd.DataFrame,
    station_id: str,
    dataset_scope: str,
    resolution: str,
    active_on: date | datetime | str | None = None,
    country: str = 'CZ',
    provider_raw: bool = False,
    include_mapping: bool = False,
):
    from .providers import get_provider

    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
        country=country,
        provider_raw=provider_raw,
    )
    matches = availability[
        (availability['dataset_scope'] == dataset_scope.strip())
        & (availability['resolution'] == resolution.strip())
    ]
    if matches.empty:
        if include_mapping:
            return pd.DataFrame(columns=['station_id', 'dataset_scope', 'resolution', 'element', 'element_raw', 'raw_elements'])
        return []
    if include_mapping:
        provider = get_provider(country)
        spec = provider.get_dataset_spec(dataset_scope, resolution)
        mapping = element_mapping_for_spec(spec).copy()
        mapping.insert(0, 'resolution', resolution.strip())
        mapping.insert(0, 'dataset_scope', dataset_scope.strip())
        mapping.insert(0, 'station_id', station_id)
        return mapping.reset_index(drop=True)
    return list(matches.iloc[0]['supported_elements'])
