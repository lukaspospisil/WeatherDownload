from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .parser import (
    normalize_meteo_ad_observation_metadata,
    parse_meteo_ad_daily_variable_html,
    parse_meteo_ad_station_detail_html,
    parse_meteo_ad_station_inventory_html,
    read_text_from_source,
    related_daily_variables_source,
    related_station_detail_source,
    supported_daily_elements_from_fields,
)
from .registry import AD_DAILY_PARAMETER_METADATA, get_dataset_spec


def read_station_metadata_ad(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('meteo_ad', 'daily').station_metadata_url
    inventory_text = read_text_from_source(source, timeout, requests)
    stations = parse_meteo_ad_station_inventory_html(inventory_text)

    rows: list[dict[str, object]] = []
    for station_id, station_name in stations:
        detail_source = _station_detail_source(source, station_id)
        if detail_source is None:
            continue
        try:
            detail = parse_meteo_ad_station_detail_html(read_text_from_source(detail_source, timeout, requests))
        except Exception:
            continue
        if not detail.get('station_id'):
            detail['station_id'] = station_id
        if pd.isna(detail.get('full_name')) or not str(detail.get('full_name', '')).strip():
            detail['full_name'] = station_name or pd.NA
        rows.append(detail)

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.drop_duplicates(subset=['station_id']).sort_values('station_id').reset_index(drop=True)


def read_station_observation_metadata_ad(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    source = source_url or get_dataset_spec('meteo_ad', 'daily').station_metadata_url
    stations = read_station_metadata_ad(source_url=source, timeout=timeout)
    if stations.empty:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)

    station_supported: dict[str, list[str]] = {}
    for station_id in stations['station_id'].astype(str):
        fields_source = _daily_variables_source(source, station_id)
        if fields_source is None:
            continue
        try:
            fields = parse_meteo_ad_daily_variable_html(_read_daily_variables_html(fields_source, station_id, timeout))
        except Exception:
            continue
        supported = supported_daily_elements_from_fields(fields)
        if supported:
            station_supported[station_id] = supported

    if not station_supported:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)

    metadata = normalize_meteo_ad_observation_metadata(
        stations[stations['station_id'].isin(station_supported)],
        get_dataset_spec('meteo_ad', 'daily'),
        AD_DAILY_PARAMETER_METADATA,
        station_supported_elements=station_supported,
    )
    return metadata.sort_values(['station_id', 'element']).reset_index(drop=True)


def _station_detail_source(source: str, station_id: str) -> str | None:
    related_source = related_station_detail_source(source, station_id)
    if related_source is not None:
        return related_source
    if _is_local_source(source):
        return None
    spec = get_dataset_spec('meteo_ad', 'daily')
    return spec.station_detail_url_template.format(station_id=station_id)


def _daily_variables_source(source: str, station_id: str) -> str | None:
    related_source = related_daily_variables_source(source, station_id)
    if related_source is not None:
        return related_source
    if _is_local_source(source):
        return None
    return get_dataset_spec('meteo_ad', 'daily').daily_variables_url


def _read_daily_variables_html(source: str, station_id: str, timeout: int) -> str:
    if _is_local_source(source):
        return read_text_from_source(source, timeout, requests)
    response = requests.post(
        source,
        data={'id': 'mesuraEst', 'estacio': station_id, 'mesura': '0', 'idioma': 'ca'},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def _is_local_source(source: str) -> bool:
    return bool(source) and pd.notna(source) and Path(source).exists()
