from __future__ import annotations

from datetime import timedelta

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_meteo_lt
from .parser import LT_NORMALIZED_DAILY_COLUMNS, normalize_lt_daily_payload, parse_lt_payload_json
from .registry import get_dataset_spec
from ..ghcnd.wrappers import build_daily_observation_downloader

download_daily_observations_ghcnd = build_daily_observation_downloader(get_dataset_spec=get_dataset_spec)


def download_daily_observations_meteo_lt(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'meteo_lt' or query.resolution != 'daily':
        raise UnsupportedQueryError('The Lithuania daily downloader supports only meteo_lt/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The Meteo.lt Lithuania daily downloader requires at least one element.')
    if query.start_date is None or query.end_date is None:
        raise UnsupportedQueryError('The Meteo.lt Lithuania daily downloader requires start_date and end_date.')
    if query.all_history:
        raise UnsupportedQueryError(
            'The Meteo.lt Lithuania daily downloader does not support all_history because the source exposes per-day UTC observation slices only.'
        )

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_meteo_lt(timeout=timeout)
    station_id_lookup = {
        str(station_id).strip().upper(): str(station_id).strip()
        for station_id in metadata_table['station_id'].astype(str)
    }
    missing_station_ids = sorted(
        station_id
        for station_id in query.station_ids
        if station_id.strip().upper() not in station_id_lookup
    )
    if missing_station_ids:
        raise StationNotFoundError(f"No Meteo.lt station metadata found for station_id: {', '.join(missing_station_ids)}")

    frames: list[pd.DataFrame] = []
    for requested_station_id in query.station_ids:
        station_id = station_id_lookup[requested_station_id.strip().upper()]
        current_date = query.start_date
        while current_date <= query.end_date:
            payload = _download_station_day_payload(
                station_id=station_id,
                observation_date=current_date,
                timeout=timeout,
            )
            if payload is None:
                current_date += timedelta(days=1)
                continue
            normalized = normalize_lt_daily_payload(
                payload,
                query=query,
                station_id=station_id,
                observation_date=current_date,
            )
            if not normalized.empty:
                element_columns = canonicalize_element_series(pd.Series(normalized['element_raw']), query)
                normalized['element'] = element_columns['element']
                normalized['element_raw'] = element_columns['element_raw']
                frames.append(normalized.loc[:, LT_NORMALIZED_DAILY_COLUMNS])
            current_date += timedelta(days=1)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['station_id', 'element', 'observation_date']).reset_index(drop=True)
    return combined.loc[:, LT_NORMALIZED_DAILY_COLUMNS].sort_values(
        ['station_id', 'observation_date', 'element'],
        kind='stable',
    ).reset_index(drop=True)


def _download_station_day_payload(
    *,
    station_id: str,
    observation_date,
    timeout: int,
) -> dict[str, object] | None:
    spec = get_dataset_spec('meteo_lt', 'daily')
    url = spec.observation_url_template.format(
        station_code=station_id,
        observation_date=observation_date.isoformat(),
    )
    response = requests.get(url, timeout=timeout)
    if response.status_code == 404:
        return None
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise DownloadError(
            f'Failed to download Meteo.lt daily observations for station_id {station_id} and date {observation_date.isoformat()}: {exc}'
        ) from exc
    response.encoding = 'utf-8'
    payload = parse_lt_payload_json(response.text)
    if not isinstance(payload, dict):
        raise DownloadError(
            f'Meteo.lt daily observations for station_id {station_id} and date {observation_date.isoformat()} returned an unexpected payload shape.'
        )
    return payload
