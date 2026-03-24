from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from .se_parser import normalize_se_observation_metadata, normalize_se_station_metadata, parse_se_parameter_json, read_text_from_source
from .se_registry import SE_DAILY_PARAMETER_IDS, SMHI_METOBS_API_BASE, get_dataset_spec


def read_station_metadata_se(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    payloads = [_read_parameter_payload(parameter_id, source_url=source_url, timeout=timeout) for parameter_id in SE_DAILY_PARAMETER_IDS]
    return normalize_se_station_metadata(payloads)


def read_station_observation_metadata_se(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    payloads = [_read_parameter_payload(parameter_id, source_url=source_url, timeout=timeout) for parameter_id in SE_DAILY_PARAMETER_IDS]
    return normalize_se_observation_metadata(payloads, get_dataset_spec('historical', 'daily'))


def _read_parameter_payload(parameter_id: str, source_url: str | None, timeout: int) -> dict[str, object]:
    source = _resolve_parameter_source(source_url, parameter_id)
    payload_text = read_text_from_source(source, timeout, requests)
    return parse_se_parameter_json(payload_text)


def _resolve_parameter_source(source_url: str | None, parameter_id: str) -> str:
    if not source_url:
        return f'{SMHI_METOBS_API_BASE}/parameter/{parameter_id}.json'
    root = Path(source_url)
    if root.is_dir():
        for candidate in (
            root / f'parameter_{parameter_id}.json',
            root / f'parameter-{parameter_id}.json',
            root / f'{parameter_id}.json',
        ):
            if candidate.exists():
                return str(candidate)
        raise FileNotFoundError(f'No SMHI fixture file found for parameter {parameter_id} under {source_url!r}.')
    return source_url
