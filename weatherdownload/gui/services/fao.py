from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone

import pandas as pd

from weatherdownload import ObservationQuery, read_station_metadata
from weatherdownload.fao_config import FILL_MISSING_CHOICES, get_fao_country_config, list_supported_fao_countries
from weatherdownload.fao import (
    FINAL_SERIES_COLUMNS,
    build_data_info,
    build_series_record,
    build_series_table,
    build_station_table,
    prepare_complete_station_series_with_provenance,
    summarize_field_fill_status,
)
from weatherdownload.observations import download_observations
from weatherdownload.providers import normalize_country_code


@dataclass(frozen=True)
class FaoPreview:
    data_info: dict[str, object]
    stations: pd.DataFrame
    series: pd.DataFrame
    field_summary: pd.DataFrame


def fao_fill_choices() -> tuple[str, ...]:
    return tuple(FILL_MISSING_CHOICES)


def supported_fao_countries() -> list[str]:
    return list_supported_fao_countries()


def prepare_fao_preview(
    *,
    country: str,
    station_ids: list[str],
    start_date: date,
    end_date: date,
    fill_missing: str = "none",
    min_complete_days: int = 1,
    timeout: int = 60,
) -> FaoPreview:
    normalized_country = normalize_country_code(country)
    config = get_fao_country_config(normalized_country, fill_missing=fill_missing)
    station_metadata = read_station_metadata(country=normalized_country, timeout=timeout)

    daily_query = ObservationQuery(
        country=config.country,
        dataset_scope=config.dataset_scope,
        resolution=config.resolution,
        station_ids=station_ids,
        start_date=start_date,
        end_date=end_date,
        elements=list(config.query_elements),
    )
    daily_table = download_observations(daily_query, timeout=timeout, station_metadata=station_metadata, country=config.country)

    hourly_table = pd.DataFrame()
    if fill_missing == "allow-hourly-aggregate" and config.hourly_query_elements:
        hourly_query = ObservationQuery(
            country=config.country,
            dataset_scope=config.hourly_dataset_scope,
            resolution=config.hourly_resolution,
            station_ids=station_ids,
            start=_start_of_day(start_date),
            end=_end_of_day(end_date),
            elements=list(config.hourly_query_elements),
        )
        hourly_table = download_observations(hourly_query, timeout=timeout, station_metadata=station_metadata, country=config.country)

    station_rows: list[dict[str, object]] = []
    series_records: list[dict[str, object]] = []
    provenance_tables: list[pd.DataFrame] = []
    applied_rules_by_field: dict[str, set[str]] = {field: set() for field in FINAL_SERIES_COLUMNS}
    station_index = station_metadata.drop_duplicates(subset=["station_id"]).set_index("station_id", drop=False)

    for station_id in station_ids:
        if station_id not in station_index.index:
            continue
        station = station_index.loc[station_id]
        station_daily = daily_table[daily_table["station_id"].astype(str) == str(station_id)].reset_index(drop=True)
        station_hourly = hourly_table[hourly_table["station_id"].astype(str) == str(station_id)].reset_index(drop=True)
        complete, provenance, applied_rules = prepare_complete_station_series_with_provenance(
            station_daily,
            hourly_table=station_hourly,
            config=config,
            fill_missing=fill_missing,
        )
        if complete.empty or len(complete) < min_complete_days:
            continue
        station_rows.append(
            {
                "station_id": station_id,
                "full_name": station["full_name"],
                "latitude": station["latitude"],
                "longitude": station["longitude"],
                "elevation_m": station["elevation_m"],
                "num_complete_days": int(len(complete)),
                "first_complete_date": complete["date"].min().isoformat(),
                "last_complete_date": complete["date"].max().isoformat(),
            }
        )
        series_records.append(
            build_series_record(
                complete,
                station_id=station_id,
                full_name=station["full_name"],
                latitude=station["latitude"],
                longitude=station["longitude"],
                elevation=station["elevation_m"],
            )
        )
        provenance_tables.append(provenance)
        for field_name, rule in applied_rules.items():
            if rule:
                applied_rules_by_field[field_name].add(rule)

    data_info = build_data_info(config, station_rows, min_complete_days=min_complete_days, fill_missing=fill_missing)
    field_summary = summarize_field_fill_status(
        provenance_tables,
        fill_missing=fill_missing,
        applied_rules_by_field=applied_rules_by_field,
    )
    return FaoPreview(
        data_info=data_info,
        stations=build_station_table(station_rows),
        series=build_series_table(series_records),
        field_summary=pd.DataFrame([summary.__dict__ for summary in field_summary]),
    )


def _start_of_day(value: date) -> datetime:
    return datetime.combine(value, time(0, 0), tzinfo=timezone.utc)


def _end_of_day(value: date) -> datetime:
    return datetime.combine(value, time(23, 0), tzinfo=timezone.utc)
