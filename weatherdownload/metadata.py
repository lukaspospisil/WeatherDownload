from __future__ import annotations

import csv
import io
from collections.abc import Sequence
from datetime import date, datetime

import pandas as pd
import requests

DEFAULT_META1_URL = "https://opendata.chmi.cz/meteorology/climate/historical_csv/metadata/meta1.csv"

STATION_METADATA_COLUMNS = [
    "station_id",
    "gh_id",
    "begin_date",
    "end_date",
    "full_name",
    "longitude",
    "latitude",
    "elevation_m",
]


def read_station_metadata(source_url: str = DEFAULT_META1_URL, timeout: int = 60) -> pd.DataFrame:
    response = requests.get(source_url, timeout=timeout)
    response.raise_for_status()
    response.encoding = "utf-8"
    return _parse_station_metadata_csv(response.text)


def filter_stations(
    stations: pd.DataFrame,
    station_ids: Sequence[str] | None = None,
    gh_ids: Sequence[str] | None = None,
    name_contains: str | None = None,
    active_on: date | datetime | str | None = None,
) -> pd.DataFrame:
    filtered = stations.copy()

    if station_ids:
        station_id_set = {station_id.strip().upper() for station_id in station_ids}
        filtered = filtered[filtered["station_id"].str.upper().isin(station_id_set)]

    if gh_ids:
        gh_id_set = {gh_id.strip().upper() for gh_id in gh_ids}
        filtered = filtered[filtered["gh_id"].str.upper().isin(gh_id_set)]

    if name_contains:
        filtered = filtered[
            filtered["full_name"].str.contains(name_contains, case=False, na=False)
        ]

    if active_on is not None:
        active_ts = pd.Timestamp(active_on)
        begin = pd.to_datetime(filtered["begin_date"], utc=True)
        end = pd.to_datetime(filtered["end_date"], utc=True)
        filtered = filtered[(begin <= active_ts) & (end >= active_ts)]

    return filtered.reset_index(drop=True)


def _parse_station_metadata_csv(csv_text: str) -> pd.DataFrame:
    reader = csv.DictReader(io.StringIO(csv_text))
    records: list[dict[str, object]] = []

    for row in reader:
        if not row.get("WSI") or not row.get("GH_ID"):
            continue

        records.append(
            {
                "station_id": row["WSI"].strip(),
                "gh_id": row["GH_ID"].strip(),
                "begin_date": _normalize_datetime(row["BEGIN_DATE"]),
                "end_date": _normalize_datetime(row["END_DATE"]),
                "full_name": row["FULL_NAME"].strip(),
                "longitude": _parse_float(row["GEOGR1"]),
                "latitude": _parse_float(row["GEOGR2"]),
                "elevation_m": _parse_float(row["ELEVATION"]),
            }
        )

    return pd.DataFrame.from_records(records, columns=STATION_METADATA_COLUMNS)


def _normalize_datetime(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return cleaned
    parsed = datetime.strptime(cleaned, "%Y-%m-%dT%H:%MZ")
    return parsed.isoformat(timespec="minutes") + "Z"


def _parse_float(value: str) -> float | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    return float(cleaned)
