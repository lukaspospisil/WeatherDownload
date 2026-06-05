from __future__ import annotations

from datetime import timedelta

import pandas as pd
import requests

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import (
    normalize_knmi_observation_metadata,
    normalize_knmi_station_metadata,
    parse_knmi_daily_text,
    read_text_from_source,
)
from .registry import KNMI_DAILY_DATA_URL, get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='NL', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='NL',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_knmi(source_url: str | None = None, timeout: int = 60):
    if source_url is not None:
        return normalize_knmi_station_metadata(parse_knmi_daily_text(read_text_from_source(source_url, timeout, requests)))
    return normalize_knmi_station_metadata(parse_knmi_daily_text(_download_station_listing_text(timeout=timeout)))


def read_station_observation_metadata_knmi(source_url: str | None = None, timeout: int = 60):
    if source_url is not None:
        parsed = parse_knmi_daily_text(read_text_from_source(source_url, timeout, requests))
    else:
        parsed = parse_knmi_daily_text(_download_station_listing_text(timeout=timeout))
    return normalize_knmi_observation_metadata(parsed, get_dataset_spec('knmi', 'daily'))


def download_knmi_daily_text(
    *,
    station_ids: list[str] | None,
    raw_elements: list[str],
    start_date: str,
    end_date: str,
    timeout: int,
) -> str:
    payload = {
        'start': start_date.replace('-', ''),
        'end': end_date.replace('-', ''),
        'vars': ':'.join(raw_elements),
        'fmt': 'csv',
    }
    if station_ids:
        payload['stns'] = ':'.join(station_ids)
    response = requests.post(KNMI_DAILY_DATA_URL, data=payload, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def _download_station_listing_text(*, timeout: int) -> str:
    request_date = (pd.Timestamp.utcnow().normalize() - timedelta(days=14)).date().isoformat()
    return download_knmi_daily_text(
        station_ids=None,
        raw_elements=['TG'],
        start_date=request_date,
        end_date=request_date,
        timeout=timeout,
    )
