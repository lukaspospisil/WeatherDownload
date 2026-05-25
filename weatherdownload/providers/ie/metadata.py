from __future__ import annotations

from pathlib import Path

import requests

from .parser import (
    load_ie_audited_stations,
    normalize_ie_observation_metadata,
    normalize_ie_station_metadata,
    parse_ie_station_details_csv,
)
from .registry import IE_AUDITED_DAILY_STATIONS_PATH, IE_METEIREANN_STATION_DETAILS_URL, get_dataset_spec


def read_station_metadata_ie(source_url: str | None = None, timeout: int = 60):
    if source_url is None:
        return load_ie_audited_stations(IE_AUDITED_DAILY_STATIONS_PATH)
    source = source_url or IE_METEIREANN_STATION_DETAILS_URL
    local_path = Path(source)
    if local_path.exists():
        parsed = parse_ie_station_details_csv(local_path.read_text(encoding='utf-8'))
    else:
        response = requests.get(source, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        parsed = parse_ie_station_details_csv(response.text)
    return normalize_ie_station_metadata(parsed)


def read_station_observation_metadata_ie(source_url: str | None = None, timeout: int = 60):
    stations = read_station_metadata_ie(source_url=source_url, timeout=timeout)
    return normalize_ie_observation_metadata(stations, get_dataset_spec('meteireann', 'daily'))
