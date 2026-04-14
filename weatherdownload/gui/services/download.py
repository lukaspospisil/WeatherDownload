from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_dataset_scopes,
    list_resolutions,
    list_supported_elements,
)
from weatherdownload.gui.services.stations import normalize_station_ids


@dataclass(frozen=True)
class ObservationSummary:
    rows: int
    columns: int
    date_column: str | None
    min_date: str | None
    max_date: str | None


def dataset_scopes(country: str) -> list[str]:
    return list_dataset_scopes(country=country)


def dataset_resolutions(country: str, dataset_scope: str) -> list[str]:
    return list_resolutions(country=country, dataset_scope=dataset_scope)


def dataset_elements(country: str, dataset_scope: str, resolution: str) -> list[str]:
    return list_supported_elements(country=country, dataset_scope=dataset_scope, resolution=resolution)


def build_observation_query(
    *,
    country: str,
    dataset_scope: str,
    resolution: str,
    station_ids: list[str],
    elements: list[str],
    all_history: bool,
    station_metadata: pd.DataFrame | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
) -> ObservationQuery:
    normalized_station_ids = normalize_station_ids(station_ids, station_metadata) if station_metadata is not None else station_ids
    common = {
        "country": country,
        "dataset_scope": dataset_scope,
        "resolution": resolution,
        "station_ids": normalized_station_ids,
        "elements": elements,
        "all_history": all_history,
    }
    if resolution == "daily":
        return ObservationQuery(
            **common,
            start_date=None if all_history else start_date,
            end_date=None if all_history else end_date,
        )
    return ObservationQuery(
        **common,
        start=None if all_history else start_datetime,
        end=None if all_history else end_datetime,
    )


def run_observation_query(query: ObservationQuery, *, timeout: int = 60, station_metadata: pd.DataFrame | None = None) -> pd.DataFrame:
    return download_observations(query, timeout=timeout, station_metadata=station_metadata, country=query.country)


def observation_query_payload(query: ObservationQuery) -> dict[str, object]:
    payload: dict[str, object] = {
        "country": query.country,
        "dataset_scope": query.dataset_scope,
        "resolution": query.resolution,
        "station_ids": list(query.station_ids),
        "elements": list(query.elements or []),
        "all_history": query.all_history,
    }
    if query.resolution == "daily":
        payload["start_date"] = None if query.start_date is None else query.start_date.isoformat()
        payload["end_date"] = None if query.end_date is None else query.end_date.isoformat()
    else:
        payload["start"] = None if query.start is None else query.start.isoformat()
        payload["end"] = None if query.end is None else query.end.isoformat()
    return payload


def preview_table(table: pd.DataFrame, *, max_rows: int = 50) -> pd.DataFrame:
    if table.empty:
        return table
    display = to_wide_preview(table)
    return display.head(max_rows)


def to_wide_preview(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty or "element" not in observations.columns or "value" not in observations.columns:
        return observations
    if "observation_date" in observations.columns:
        row_key = "observation_date"
    elif "timestamp" in observations.columns:
        row_key = "timestamp"
    else:
        return observations
    index_columns = ["station_id"]
    if "gh_id" in observations.columns and observations["gh_id"].notna().any():
        index_columns.append("gh_id")
    index_columns.append(row_key)
    wide = observations.pivot_table(index=index_columns, columns="element", values="value", aggfunc="first").reset_index()
    wide.columns.name = None
    return wide


def summarize_observations(table: pd.DataFrame) -> ObservationSummary:
    if "observation_date" in table.columns:
        series = pd.to_datetime(table["observation_date"], errors="coerce")
        return ObservationSummary(
            rows=len(table),
            columns=len(table.columns),
            date_column="observation_date",
            min_date=None if series.dropna().empty else series.min().date().isoformat(),
            max_date=None if series.dropna().empty else series.max().date().isoformat(),
        )
    if "timestamp" in table.columns:
        series = pd.to_datetime(table["timestamp"], utc=True, errors="coerce")
        return ObservationSummary(
            rows=len(table),
            columns=len(table.columns),
            date_column="timestamp",
            min_date=None if series.dropna().empty else series.min().isoformat(),
            max_date=None if series.dropna().empty else series.max().isoformat(),
        )
    return ObservationSummary(rows=len(table), columns=len(table.columns), date_column=None, min_date=None, max_date=None)
