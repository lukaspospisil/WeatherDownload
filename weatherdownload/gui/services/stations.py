from __future__ import annotations

from typing import Any

import pandas as pd

from weatherdownload import filter_stations, list_supported_countries, read_station_metadata


def supported_countries() -> list[str]:
    return list_supported_countries()


def load_station_metadata(country: str, *, timeout: int = 60, source_url: str | None = None) -> pd.DataFrame:
    return read_station_metadata(country=country, timeout=timeout, source_url=source_url)


def filter_station_table(
    stations: pd.DataFrame,
    *,
    station_id_filter: str = "",
    station_name_filter: str = "",
) -> pd.DataFrame:
    filtered = stations
    station_id_filter = station_id_filter.strip()
    if station_id_filter:
        filtered = filter_stations(filtered, station_ids=[station_id_filter])
        if filtered.empty:
            filtered = stations[stations["station_id"].astype(str).str.contains(station_id_filter, case=False, na=False)].reset_index(drop=True)
    if station_name_filter.strip():
        filtered = filter_stations(filtered, name_contains=station_name_filter.strip())
    return filtered.reset_index(drop=True)


def station_option_labels(stations: pd.DataFrame) -> dict[str, str]:
    labels: dict[str, str] = {}
    for row in stations.itertuples(index=False):
        full_name = getattr(row, "full_name", "") or "(no name)"
        labels[str(row.station_id)] = f"{row.station_id} - {full_name}"
    return labels


def normalize_station_ids(selected_values: list[str], stations: pd.DataFrame) -> list[str]:
    if not selected_values:
        return []

    station_ids = {str(value): str(value) for value in stations.get("station_id", pd.Series(dtype="string")).astype(str)}
    casefold_lookup = {station_id.casefold(): station_id for station_id in station_ids.values()}
    label_lookup = {label.casefold(): station_id for station_id, label in station_option_labels(stations).items()}
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_value in selected_values:
        cleaned = str(raw_value).strip()
        if not cleaned:
            continue

        canonical_station_id = casefold_lookup.get(cleaned.casefold())
        if canonical_station_id is None:
            canonical_station_id = label_lookup.get(cleaned.casefold())
        if canonical_station_id is None and " - " in cleaned:
            candidate = cleaned.split(" - ", 1)[0].strip()
            canonical_station_id = casefold_lookup.get(candidate.casefold())
        if canonical_station_id is None:
            raise ValueError(f"Unknown station selection: {cleaned}")
        if canonical_station_id not in seen:
            seen.add(canonical_station_id)
            normalized.append(canonical_station_id)

    return normalized


def station_detail(stations: pd.DataFrame, station_id: str) -> dict[str, Any] | None:
    matches = stations[stations["station_id"].astype(str) == str(station_id)]
    if matches.empty:
        return None
    row = matches.iloc[0]
    return {column: row[column] for column in matches.columns}
