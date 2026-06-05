from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import (
    normalize_lt_observation_metadata,
    normalize_lt_station_metadata,
    parse_lt_payload_json,
    read_text_from_source,
)
from .registry import LT_METEO_LT_PARAMETER_METADATA, get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='LT', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='LT',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_meteo_lt(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('meteo_lt', 'daily')
    source = source_url or spec.station_metadata_url
    station_records = _read_station_records(source, timeout=timeout)
    ranges_by_station = _read_station_ranges(source, timeout=timeout, station_ids=[record.get('code') for record in station_records])
    return normalize_lt_station_metadata(station_records, spec, ranges_by_station=ranges_by_station)


def read_station_observation_metadata_meteo_lt(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('meteo_lt', 'daily')
    source = source_url or spec.station_metadata_url
    station_records = _read_station_records(source, timeout=timeout)
    ranges_by_station = _read_station_ranges(source, timeout=timeout, station_ids=[record.get('code') for record in station_records])
    return normalize_lt_observation_metadata(
        station_records,
        spec,
        LT_METEO_LT_PARAMETER_METADATA,
        ranges_by_station=ranges_by_station,
    )


def _read_station_records(source: str, *, timeout: int) -> list[dict[str, object]]:
    local_path = Path(source)
    if local_path.exists() and local_path.is_dir():
        text = (local_path / 'stations.json').read_text(encoding='utf-8')
    else:
        text = read_text_from_source(source, timeout)
    payload = parse_lt_payload_json(text)
    if not isinstance(payload, list):
        raise ValueError('Meteo.lt station list response must be a JSON array.')
    return [record for record in payload if isinstance(record, dict)]


def _read_station_ranges(
    source: str,
    *,
    timeout: int,
    station_ids: list[object],
) -> dict[str, dict[str, object]]:
    spec = get_dataset_spec('meteo_lt', 'daily')
    local_path = Path(source)
    ranges_by_station: dict[str, dict[str, object]] = {}
    for station_id in station_ids:
        if not station_id:
            continue
        normalized_station_id = str(station_id).strip()
        if not normalized_station_id:
            continue
        if local_path.exists() and local_path.is_dir():
            range_path = local_path / f'{normalized_station_id}_observations_range.json'
            if not range_path.exists():
                continue
            text = range_path.read_text(encoding='utf-8')
        else:
            text = read_text_from_source(
                spec.station_observation_range_url_template.format(station_code=normalized_station_id),
                timeout,
            )
        payload = parse_lt_payload_json(text)
        if isinstance(payload, dict):
            ranges_by_station[normalized_station_id] = payload
    return ranges_by_station
