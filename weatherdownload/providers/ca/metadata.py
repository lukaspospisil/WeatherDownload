from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import pandas as pd

import requests

from ...errors import DownloadError
from .eccc_parser import (
    normalize_ca_eccc_observation_metadata,
    parse_ca_eccc_station_metadata_feature_collection,
    read_text_from_source,
)
from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .registry import get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='CA', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='CA',
    get_dataset_spec=get_dataset_spec,
)

_ECCC_STATION_PROPERTIES = (
    'CLIMATE_IDENTIFIER,STATION_NAME,DLY_FIRST_DATE,DLY_LAST_DATE,ELEVATION,HAS_HOURLY_DATA,HLY_FIRST_DATE,HLY_LAST_DATE'
)
_ECCC_STATION_PAGE_LIMIT = 5000


def read_station_metadata_eccc(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('eccc', 'daily')
    source = source_url or spec.station_metadata_url

    # Preserve the existing local-fixture behavior for tests and offline use.
    if source_url is not None and Path(source_url).exists():
        metadata_text = read_text_from_source(source, timeout, requests)
        stations = parse_ca_eccc_station_metadata_feature_collection(metadata_text)
        stations.attrs['source_url'] = source
        return stations

    base_url = _strip_query_params(source)
    params = {
        'f': 'json',
        'limit': str(_ECCC_STATION_PAGE_LIMIT),
        'properties': _ECCC_STATION_PROPERTIES,
    }
    features = _download_eccc_feature_collection_features(url=base_url, params=params, timeout=timeout)
    payload_text = json.dumps({'type': 'FeatureCollection', 'features': features})
    stations = parse_ca_eccc_station_metadata_feature_collection(payload_text)
    stations.attrs['source_url'] = source
    return stations


def read_station_observation_metadata_eccc(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_eccc(source_url=source_url, timeout=timeout)
    return normalize_ca_eccc_observation_metadata(stations)


def _strip_query_params(url: str) -> str:
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, '', ''))


def _extract_next_href(payload: dict[str, object], request_url: str) -> str | None:
    links = payload.get('links')
    if not isinstance(links, list):
        return None
    for link in links:
        if not isinstance(link, dict):
            continue
        if link.get('rel') != 'next':
            continue
        href = link.get('href')
        if isinstance(href, str) and href.strip():
            return urljoin(request_url, href.strip())
    return None


def _download_eccc_feature_collection_features(*, url: str, params: dict[str, str], timeout: int) -> list[dict[str, object]]:
    features: list[dict[str, object]] = []
    next_url: str | None = None
    seen_next: set[str] = set()

    for _ in range(1000):
        if next_url is None:
            response = requests.get(url, params=params, timeout=timeout)
        else:
            response = requests.get(next_url, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'

        try:
            payload = json.loads(response.text.lstrip('\ufeff'))
        except json.JSONDecodeError as exc:
            raise DownloadError('ECCC GeoMet station metadata response is not valid JSON.') from exc
        if not isinstance(payload, dict):
            break
        page_features = payload.get('features')
        if isinstance(page_features, list):
            for feature in page_features:
                if isinstance(feature, dict):
                    features.append(feature)

        next_candidate = _extract_next_href(payload, next_url or url)
        if not next_candidate or next_candidate in seen_next:
            break
        seen_next.add(next_candidate)
        next_url = next_candidate

    return features
