from __future__ import annotations

from pathlib import Path

import requests

from .errors import DownloadError
from .knmi_parser import normalize_knmi_observation_metadata, parse_knmi_api_listing_json, parse_knmi_station_metadata_csv
from .knmi_registry import (
    KNMI_OPEN_DATA_BASE_URL,
    KNMI_PARAMETER_METADATA,
    KNMI_STATION_DATASET_NAME,
    KNMI_STATION_DATASET_VERSION,
    get_dataset_spec,
)


def read_station_metadata_knmi(source_url: str | None = None, timeout: int = 60):
    if source_url is not None:
        return parse_knmi_station_metadata_csv(_read_text_from_source(source_url, timeout))
    api_key = resolve_knmi_api_key()
    filename = _latest_station_metadata_filename(timeout=timeout, api_key=api_key)
    csv_bytes = download_knmi_file_bytes(
        dataset_name=KNMI_STATION_DATASET_NAME,
        dataset_version=KNMI_STATION_DATASET_VERSION,
        filename=filename,
        timeout=timeout,
        api_key=api_key,
    )
    return parse_knmi_station_metadata_csv(csv_bytes.decode('utf-8'))


def read_station_observation_metadata_knmi(source_url: str | None = None, timeout: int = 60):
    stations = read_station_metadata_knmi(source_url=source_url, timeout=timeout)
    spec = get_dataset_spec('historical', 'daily')
    return normalize_knmi_observation_metadata(stations, spec, KNMI_PARAMETER_METADATA)


def resolve_knmi_api_key() -> str:
    import os

    for environment_name in ('WEATHERDOWNLOAD_KNMI_API_KEY', 'KNMI_API_KEY'):
        value = os.getenv(environment_name, '').strip()
        if value:
            return value
    raise ValueError(
        'KNMI Open Data API key is required for NL support. Set WEATHERDOWNLOAD_KNMI_API_KEY or KNMI_API_KEY before using country="NL".'
    )


def list_knmi_files(
    *,
    dataset_name: str,
    dataset_version: str,
    timeout: int,
    api_key: str,
    params: dict[str, str] | None = None,
) -> dict[str, object]:
    endpoint = f'{KNMI_OPEN_DATA_BASE_URL}/datasets/{dataset_name}/versions/{dataset_version}/files'
    response = requests.get(endpoint, headers={'Authorization': api_key}, params=params or {}, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return parse_knmi_api_listing_json(response.text)


def download_knmi_file_bytes(
    *,
    dataset_name: str,
    dataset_version: str,
    filename: str,
    timeout: int,
    api_key: str,
) -> bytes:
    endpoint = f'{KNMI_OPEN_DATA_BASE_URL}/datasets/{dataset_name}/versions/{dataset_version}/files/{filename}/url'
    response = requests.get(endpoint, headers={'Authorization': api_key}, timeout=timeout)
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise DownloadError(f'KNMI file-url response for {filename!r} is not valid JSON.') from exc
    download_url = payload.get('temporaryDownloadUrl')
    if not isinstance(download_url, str) or not download_url:
        raise DownloadError(f'KNMI file-url response for {filename!r} is missing temporaryDownloadUrl.')
    download_response = requests.get(download_url, timeout=timeout)
    download_response.raise_for_status()
    return download_response.content


def _latest_station_metadata_filename(timeout: int, api_key: str) -> str:
    payload = list_knmi_files(
        dataset_name=KNMI_STATION_DATASET_NAME,
        dataset_version=KNMI_STATION_DATASET_VERSION,
        timeout=timeout,
        api_key=api_key,
        params={'maxKeys': '1', 'sorting': 'desc', 'orderBy': 'filename'},
    )
    files = payload.get('files', [])
    if not files:
        raise DownloadError('KNMI station metadata dataset returned no files.')
    filename = files[0].get('filename')
    if not isinstance(filename, str) or not filename:
        raise DownloadError('KNMI station metadata dataset returned an invalid filename.')
    return filename


def _read_text_from_source(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text
