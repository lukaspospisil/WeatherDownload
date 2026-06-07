from __future__ import annotations

from pathlib import Path

from ..ghcnd.mixed import is_ghcnd_metadata_source
from ..ghcnd.wrappers import build_station_metadata_reader, build_station_observation_metadata_reader
from .parser import (
    normalize_vedur_observation_metadata,
    normalize_vedur_station_metadata,
    read_json_response,
)
from .registry import (
    IS_VEDUR_PARAMETER_METADATA_BY_SOURCE,
    IS_VEDUR_PARAMETERS_URL,
    get_dataset_spec,
)

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='IS', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='IS',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_vedur(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('vedur', 'daily')
    station_records = _read_station_records(source_url or spec.station_metadata_url, timeout=timeout)
    return normalize_vedur_station_metadata(station_records, spec)


def read_station_observation_metadata_vedur(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('vedur', 'daily')
    station_source = source_url or spec.station_metadata_url
    station_records = _read_station_records(station_source, timeout=timeout)
    parameter_metadata_by_source = _read_parameter_metadata(source_url, timeout=timeout)
    return normalize_vedur_observation_metadata(station_records, spec, parameter_metadata_by_source)


def _read_station_records(source: str, *, timeout: int) -> list[dict[str, object]]:
    local_path = Path(source)
    if local_path.exists() and local_path.is_dir():
        payload = read_json_response(str(local_path / 'stations.json'), timeout)
    else:
        payload = read_json_response(source, timeout)
    if not isinstance(payload, list):
        raise ValueError('Vedur station list response must be a JSON array.')
    return [record for record in payload if isinstance(record, dict)]


def _read_parameter_metadata(source_url: str | None, *, timeout: int) -> dict[str, dict[str, dict[str, str]]]:
    local_path = Path(source_url) if source_url and not is_ghcnd_metadata_source(source_url) else None
    if local_path is not None and local_path.exists() and local_path.is_dir():
        synop_payload = read_json_response(str(local_path / 'parameters_synop_day.json'), timeout)
        aws_payload = read_json_response(str(local_path / 'parameters_aws_day.json'), timeout)
    else:
        synop_payload = read_json_response(
            IS_VEDUR_PARAMETERS_URL,
            timeout,
            params={'url': '/observations/synop/day', 'locale': 'en'},
        )
        aws_payload = read_json_response(
            IS_VEDUR_PARAMETERS_URL,
            timeout,
            params={'url': '/observations/aws/day', 'locale': 'en'},
        )

    payloads = {'synop': synop_payload, 'aws': aws_payload}
    metadata_by_source: dict[str, dict[str, dict[str, str]]] = {}
    for source_kind, payload in payloads.items():
        if not isinstance(payload, dict):
            raise ValueError(f'Vedur parameter metadata response for {source_kind} must be a JSON object.')
        metadata_by_source[source_kind] = {
            raw_code: IS_VEDUR_PARAMETER_METADATA_BY_SOURCE[source_kind][raw_code]
            for raw_code in IS_VEDUR_PARAMETER_METADATA_BY_SOURCE[source_kind]
            if raw_code in payload
        }
    return metadata_by_source
