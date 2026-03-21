from __future__ import annotations

import io
from dataclasses import dataclass

import pandas as pd
import requests

from .errors import DownloadError, UnsupportedQueryError
from .queries import ObservationQuery

BASE_DAILY_URL = 'https://opendata.chmi.cz/meteorology/climate/historical_csv/data/daily'
ELEMENT_GROUPS: dict[str, str] = {
    'T': 'temperature',
    'TMA': 'temperature',
    'TMI': 'temperature',
    'RH': 'humidity',
    'P': 'air_pressure',
    'SRA': 'precipitation',
    'SSV': 'sunshine',
    'HS': 'snow',
    'WSPD': 'wind',
    'WDIR': 'wind',
}
RAW_DAILY_COLUMNS = ['STATION', 'ELEMENT', 'TIMEFUNC', 'DT', 'VALUE', 'FLAG', 'QUALITY']
NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]


@dataclass(slots=True)
class DailyDownloadTarget:
    station_id: str
    element: str
    group: str
    url: str


def build_daily_download_targets(query: ObservationQuery) -> list[DailyDownloadTarget]:
    targets: list[DailyDownloadTarget] = []
    for station_id in query.station_ids:
        for element in query.elements or []:
            group = ELEMENT_GROUPS.get(element)
            if group is None:
                raise UnsupportedQueryError(f"Unsupported daily historical_csv element: {element}")
            targets.append(DailyDownloadTarget(
                station_id=station_id,
                element=element,
                group=group,
                url=f'{BASE_DAILY_URL}/{group}/dly-{station_id}-{element}.csv',
            ))
    return targets


def download_daily_csv(target: DailyDownloadTarget, timeout: int = 60) -> str:
    try:
        response = requests.get(target.url, timeout=timeout)
    except requests.RequestException as exc:
        raise DownloadError(f"Failed to download {target.url}") from exc
    if response.status_code == 404:
        raise FileNotFoundError(target.url)
    try:
        response.raise_for_status()
    except Exception as exc:
        raise DownloadError(f"Failed to download {target.url}") from exc
    response.encoding = 'utf-8'
    return response.text


def parse_daily_csv(csv_text: str) -> pd.DataFrame:
    table = pd.read_csv(io.StringIO(csv_text))
    missing_columns = [column for column in RAW_DAILY_COLUMNS if column not in table.columns]
    if missing_columns:
        raise ValueError(f"Downloaded file is missing expected columns: {missing_columns}")
    return table.loc[:, RAW_DAILY_COLUMNS]


def normalize_daily_observations(table: pd.DataFrame, query: ObservationQuery, station_metadata: pd.DataFrame | None = None) -> pd.DataFrame:
    normalized = table.rename(columns={
        'STATION': 'station_id',
        'ELEMENT': 'element',
        'TIMEFUNC': 'time_function',
        'VALUE': 'value',
        'FLAG': 'flag',
        'QUALITY': 'quality',
    }).copy()
    raw_dt = pd.to_datetime(table['DT'], utc=True)
    normalized['observation_date'] = raw_dt.dt.date
    normalized['value'] = pd.to_numeric(normalized['value'], errors='coerce')
    normalized['quality'] = pd.to_numeric(normalized['quality'], errors='coerce').astype('Int64')
    normalized['flag'] = normalized['flag'].astype('string').replace({'': pd.NA})
    normalized['dataset_scope'] = query.dataset_scope
    normalized['resolution'] = query.resolution
    if station_metadata is not None and not station_metadata.empty:
        mapping = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])
        normalized = normalized.merge(mapping, on='station_id', how='left')
    else:
        normalized['gh_id'] = pd.NA
    if query.start_date is not None:
        normalized = normalized[normalized['observation_date'] >= query.start_date]
    if query.end_date is not None:
        normalized = normalized[normalized['observation_date'] <= query.end_date]
    return normalized.loc[:, NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)
