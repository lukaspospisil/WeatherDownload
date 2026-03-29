from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .parser import (
    extract_recent_daily_station_date_ranges,
    extract_recent_daily_station_ids,
    parse_recent_daily_payload_json,
    parse_shmu_metadata_json,
)
from .registry import get_dataset_spec


def read_station_metadata_shmu(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    return discover_recent_daily_stations_shmu(source_url=source_url, timeout=timeout)


def discover_recent_daily_stations_shmu(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    """Discover a minimal SHMU station table from the recent/daily payload only.

    This helper is intentionally conservative. It derives only fields that are directly
    available from the current SHMU recent/daily source:

    - station_id from ``ind_kli``
    - begin_date / end_date from each station's own payload ``datum`` coverage

    The following normalized station metadata fields remain null until an authoritative
    SHMU station registry is identified:

    - gh_id
    - full_name
    - longitude
    - latitude
    - elevation_m
    """
    payload_text = _load_recent_daily_payload_text(source_url=source_url, timeout=timeout)
    _, table = parse_recent_daily_payload_json(payload_text)
    if table.empty:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    station_ranges = extract_recent_daily_station_date_ranges(table)
    rows: list[dict[str, object]] = []
    for row in station_ranges.itertuples(index=False):
        rows.append(
            {
                'station_id': row.station_id,
                'gh_id': pd.NA,
                'begin_date': row.begin_date,
                'end_date': row.end_date,
                'full_name': pd.NA,
                'longitude': pd.NA,
                'latitude': pd.NA,
                'elevation_m': pd.NA,
            }
        )
    return pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)


def read_station_observation_metadata_shmu(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    payload_text = _load_recent_daily_payload_text(source_url=source_url, timeout=timeout)
    metadata_text = _read_text(get_dataset_spec('recent', 'daily').metadata_url, timeout=timeout)
    _, table = parse_recent_daily_payload_json(payload_text)
    metadata_frame = parse_shmu_metadata_json(metadata_text)
    description_lookup = {
        row.element_raw: row.description
        for row in metadata_frame.itertuples(index=False)
    }
    unit_lookup = {
        row.element_raw: row.unit
        for row in metadata_frame.itertuples(index=False)
    }
    spec = get_dataset_spec('recent', 'daily')
    begin_date = ''
    end_date = ''
    if not table.empty:
        station_ranges = extract_recent_daily_station_date_ranges(table).set_index('station_id')
    else:
        station_ranges = pd.DataFrame(columns=['begin_date', 'end_date'])
    rows: list[dict[str, object]] = []
    for station_id in extract_recent_daily_station_ids(table):
        if station_id in station_ranges.index:
            begin_date = station_ranges.loc[station_id, 'begin_date']
            end_date = station_ranges.loc[station_id, 'end_date']
        for raw_code in spec.supported_elements:
            rows.append(
                {
                    'obs_type': 'RECENT_DAILY',
                    'station_id': station_id,
                    'begin_date': begin_date,
                    'end_date': end_date,
                    'element': raw_code,
                    'schedule': 'P1D monthly JSON files',
                    'name': raw_code,
                    'description': _combine_description(description_lookup.get(raw_code), unit_lookup.get(raw_code)),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def _load_recent_daily_payload_text(source_url: str | None, timeout: int) -> str:
    if source_url is not None:
        return _read_text(source_url, timeout=timeout)
    from .observations import resolve_latest_recent_daily_data_url

    latest_url = resolve_latest_recent_daily_data_url(timeout=timeout)
    return _read_text(latest_url, timeout=timeout)


def _read_text(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _combine_description(description: object, unit: object) -> object:
    description_text = '' if description is pd.NA or pd.isna(description) else str(description).strip()
    unit_text = '' if unit is pd.NA or pd.isna(unit) else str(unit).strip()
    if description_text and unit_text:
        return f'{description_text} [{unit_text}]'
    if description_text:
        return description_text
    if unit_text:
        return f'Unit: {unit_text}'
    return pd.NA

