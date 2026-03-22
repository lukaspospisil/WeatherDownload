from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime

import pandas as pd

from .chmi_registry import get_dataset_spec, list_implemented_dataset_specs
from .metadata import filter_stations


AVAILABILITY_COLUMNS = [
    "station_id",
    "gh_id",
    "dataset_scope",
    "resolution",
    "implemented",
    "supported_elements",
]


def station_availability(
    stations: pd.DataFrame,
    station_ids: Sequence[str] | None = None,
    active_on: date | datetime | str | None = None,
    implemented_only: bool = True,
) -> pd.DataFrame:
    filtered = filter_stations(stations, station_ids=station_ids, active_on=active_on)
    specs = list_implemented_dataset_specs() if implemented_only else []
    rows: list[dict[str, object]] = []

    for station in filtered.itertuples(index=False):
        for spec in specs:
            rows.append(
                {
                    "station_id": station.station_id,
                    "gh_id": station.gh_id,
                    "dataset_scope": spec.dataset_scope,
                    "resolution": spec.resolution,
                    "implemented": spec.implemented,
                    "supported_elements": list(spec.supported_elements),
                }
            )

    return pd.DataFrame.from_records(rows, columns=AVAILABILITY_COLUMNS)


def station_supports(
    stations: pd.DataFrame,
    station_id: str,
    dataset_scope: str,
    resolution: str,
    active_on: date | datetime | str | None = None,
) -> bool:
    spec = get_dataset_spec(dataset_scope, resolution)
    if not spec.implemented:
        return False
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
    )
    return not availability[
        (availability["dataset_scope"] == spec.dataset_scope)
        & (availability["resolution"] == spec.resolution)
    ].empty


def list_station_paths(
    stations: pd.DataFrame,
    station_id: str,
    active_on: date | datetime | str | None = None,
    include_elements: bool = False,
) -> pd.DataFrame:
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
    )
    if availability.empty:
        return availability
    if include_elements:
        return availability.reset_index(drop=True)
    return availability.drop(columns=["supported_elements"]).reset_index(drop=True)


def list_station_elements(
    stations: pd.DataFrame,
    station_id: str,
    dataset_scope: str,
    resolution: str,
    active_on: date | datetime | str | None = None,
) -> list[str]:
    availability = station_availability(
        stations,
        station_ids=[station_id],
        active_on=active_on,
        implemented_only=True,
    )
    matches = availability[
        (availability["dataset_scope"] == dataset_scope.strip())
        & (availability["resolution"] == resolution.strip())
    ]
    if matches.empty:
        return []
    return list(matches.iloc[0]["supported_elements"])
