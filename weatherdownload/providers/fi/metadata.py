from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .registry import get_dataset_spec
from ...metadata import STATION_OBSERVATION_METADATA_COLUMNS
from .hourly_fmi import build_minimal_station_metadata_from_timevaluepair_fixture
from .fmi_station_metadata_parser import parse_fmi_station_feature_collection

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='FI', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='FI',
    get_dataset_spec=get_dataset_spec,
)

_FMI_STATIONS_STORED_QUERY = 'fmi::ef::stations'
_FMI_STATION_NETWORK_IDS = (121, 122)  # AWS + SYNOP
_FMI_STATION_PAGE_COUNT = 5000


def read_station_metadata_fmi(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    # Station discovery is intentionally not implemented yet. For tests and offline use,
    # allow passing a local timevaluepair fixture as the metadata source.
    if source_url is not None and Path(source_url).exists():
        xml_text = Path(source_url).read_text(encoding='utf-8')
        stations = parse_fmi_station_feature_collection(xml_text)
        if not stations.empty:
            stations.attrs['source_url'] = source_url
            _attach_fmi_station_elements_attrs(stations)
            return stations
        return build_minimal_station_metadata_from_timevaluepair_fixture(source_url)

    # If a URL is provided explicitly, treat it as an override.
    if source_url is not None:
        response = requests.get(source_url, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        stations = parse_fmi_station_feature_collection(response.text)
        stations.attrs['source_url'] = source_url
        _attach_fmi_station_elements_attrs(stations)
        return stations

    # Live station metadata: fetch a conservative subset of FMI networks used for weather observations.
    spec = get_dataset_spec('fmi', '1hour')
    payloads: list[str] = []
    for network_id in _FMI_STATION_NETWORK_IDS:
        payloads.extend(_download_station_pages(spec.wfs_url, network_id=network_id, timeout=timeout))

    combined = pd.concat([parse_fmi_station_feature_collection(text) for text in payloads], ignore_index=True)
    combined = combined.drop_duplicates(subset=['station_id']).sort_values('station_id', kind='stable').reset_index(drop=True)
    combined.attrs['source_url'] = spec.wfs_url
    _attach_fmi_station_elements_attrs(combined)
    return combined


def read_station_observation_metadata_fmi(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    # Not implemented yet for FMI.
    return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)


def _download_station_pages(wfs_url: str, *, network_id: int, timeout: int) -> list[str]:
    pages: list[str] = []
    start_index = 0
    for _ in range(100):
        params = {
            'service': 'WFS',
            'version': '2.0.0',
            'request': 'getFeature',
            'storedquery_id': _FMI_STATIONS_STORED_QUERY,
            'networkid': str(network_id),
            'count': str(_FMI_STATION_PAGE_COUNT),
            'startIndex': str(start_index),
        }
        response = requests.get(wfs_url, params=params, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        text = response.text
        pages.append(text)
        matched, returned = _feature_collection_counts(text)
        if returned <= 0:
            break
        start_index += returned
        if matched is not None and start_index >= matched:
            break
        if returned < _FMI_STATION_PAGE_COUNT:
            break
    return pages


def _feature_collection_counts(xml_text: str) -> tuple[int | None, int]:
    try:
        import xml.etree.ElementTree as ET

        root = ET.fromstring(xml_text)
    except Exception:
        return None, 0
    matched_text = root.attrib.get('numberMatched')
    returned_text = root.attrib.get('numberReturned')
    matched = int(matched_text) if isinstance(matched_text, str) and matched_text.isdigit() else None
    returned = int(returned_text) if isinstance(returned_text, str) and returned_text.isdigit() else 0
    return matched, returned


def _attach_fmi_station_elements_attrs(stations: pd.DataFrame) -> None:
    if stations.empty:
        return
    spec = get_dataset_spec('fmi', '1hour')
    stations.attrs.setdefault('station_provider_raw_elements_by_path', {})
    stations.attrs['station_provider_raw_elements_by_path'][('fmi', '1hour')] = {
        str(station_id): list(spec.supported_elements)
        for station_id in stations['station_id'].astype(str).tolist()
    }
