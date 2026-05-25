from __future__ import annotations

import os

import requests

from .parser import normalize_aemet_observation_metadata, parse_aemet_payload_json, parse_aemet_station_inventory_json, read_text_from_source
from .registry import AEMET_OPEN_DATA_BASE_URL, AEMET_STATION_INVENTORY_ENDPOINT, get_dataset_spec


def read_station_metadata_es(source_url: str | None = None, timeout: int = 60):
    if source_url is not None:
        inventory_text = read_text_from_source(source_url, timeout, requests)
        return parse_aemet_station_inventory_json(inventory_text)

    api_key = resolve_aemet_api_key()
    inventory_text = download_aemet_dataset_text(
        endpoint=f'{AEMET_OPEN_DATA_BASE_URL}{AEMET_STATION_INVENTORY_ENDPOINT}',
        timeout=timeout,
        api_key=api_key,
    )
    return parse_aemet_station_inventory_json(inventory_text)


def read_station_observation_metadata_es(source_url: str | None = None, timeout: int = 60):
    stations = read_station_metadata_es(source_url=source_url, timeout=timeout)
    return normalize_aemet_observation_metadata(stations, get_dataset_spec('aemet', 'daily'))


def resolve_aemet_api_key() -> str:
    for environment_name in ('WEATHERDOWNLOAD_AEMET_API_KEY', 'AEMET_API_KEY'):
        value = os.getenv(environment_name, '').strip()
        if value:
            return value
    raise ValueError(
        'AEMET OpenData API key is required for ES support. Set WEATHERDOWNLOAD_AEMET_API_KEY or AEMET_API_KEY before using country="ES".'
    )


def download_aemet_dataset_text(*, endpoint: str, timeout: int, api_key: str) -> str:
    response = requests.get(endpoint, headers={'api_key': api_key}, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    payload = parse_aemet_payload_json(response.text)
    if not isinstance(payload, dict):
        raise ValueError('AEMET metadata response must be a JSON object.')
    data_url = payload.get('datos')
    if not isinstance(data_url, str) or not data_url:
        raise ValueError('AEMET metadata response is missing the temporary datos URL.')
    data_response = requests.get(data_url, timeout=timeout)
    data_response.raise_for_status()
    data_response.encoding = 'utf-8'
    return data_response.text
