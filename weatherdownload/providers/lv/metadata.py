from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import (
    normalize_lvgmc_observation_metadata,
    normalize_lvgmc_parameter_metadata,
    normalize_lvgmc_station_metadata,
    parse_lvgmc_payload_json,
    read_text_from_source,
)
from .registry import get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='LV', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='LV',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_lvgmc(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('lvgmc', 'daily')
    station_records = _read_station_records(source_url, timeout=timeout)
    parameter_metadata = _read_parameter_metadata(source_url, timeout=timeout)
    return normalize_lvgmc_station_metadata(
        station_records,
        spec,
        parameter_metadata=parameter_metadata,
        active_only=True,
    )


def read_station_observation_metadata_lvgmc(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('lvgmc', 'daily')
    station_records = _read_station_records(source_url, timeout=timeout)
    parameter_metadata = _read_parameter_metadata(source_url, timeout=timeout)
    return normalize_lvgmc_observation_metadata(
        station_records,
        spec,
        parameter_metadata=parameter_metadata,
        active_only=True,
    )


def _read_station_records(source_url: str | None, *, timeout: int) -> list[dict[str, object]]:
    spec = get_dataset_spec('lvgmc', 'daily')
    local_path = Path(source_url) if source_url is not None else None
    if local_path is not None and local_path.exists() and local_path.is_dir():
        payload = parse_lvgmc_payload_json((local_path / 'stations.json').read_text(encoding='utf-8'))
    else:
        text = read_text_from_source(
            spec.metadata_api_url,
            timeout,
            params={'resource_id': spec.station_resource_id, 'limit': 10000},
        )
        payload = parse_lvgmc_payload_json(text)
    result = payload.get('result') if isinstance(payload, dict) else None
    records = result.get('records') if isinstance(result, dict) else None
    if not isinstance(records, list):
        raise ValueError('LVGMC station metadata response must contain result.records.')
    return [record for record in records if isinstance(record, dict)]


def _read_parameter_metadata(source_url: str | None, *, timeout: int) -> dict[str, dict[str, object]]:
    spec = get_dataset_spec('lvgmc', 'daily')
    local_path = Path(source_url) if source_url is not None else None
    if local_path is not None and local_path.exists() and local_path.is_dir():
        payload = parse_lvgmc_payload_json((local_path / 'parameters.json').read_text(encoding='utf-8'))
    else:
        text = read_text_from_source(
            spec.metadata_api_url,
            timeout,
            params={'resource_id': spec.parameter_resource_id, 'limit': 10000},
        )
        payload = parse_lvgmc_payload_json(text)
    result = payload.get('result') if isinstance(payload, dict) else None
    records = result.get('records') if isinstance(result, dict) else None
    if not isinstance(records, list):
        raise ValueError('LVGMC parameter metadata response must contain result.records.')
    return normalize_lvgmc_parameter_metadata([record for record in records if isinstance(record, dict)])
