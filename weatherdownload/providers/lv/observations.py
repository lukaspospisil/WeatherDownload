from __future__ import annotations

from datetime import timedelta

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_lvgmc
from .parser import LV_NORMALIZED_DAILY_COLUMNS, normalize_lvgmc_daily_records, parse_lvgmc_payload_json
from .registry import get_dataset_spec
from ..ghcnd.wrappers import build_daily_observation_downloader

download_daily_observations_ghcnd = build_daily_observation_downloader(get_dataset_spec=get_dataset_spec)


def download_daily_observations_lvgmc(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'lvgmc' or query.resolution != 'daily':
        raise UnsupportedQueryError('The Latvia daily downloader supports only lvgmc/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The LVGMC Latvia daily downloader requires at least one element.')
    if query.start_date is None or query.end_date is None:
        raise UnsupportedQueryError('The LVGMC Latvia daily downloader requires start_date and end_date.')
    if query.all_history:
        raise UnsupportedQueryError(
            'The LVGMC Latvia daily downloader does not support all_history because the recent meteorological archive covers only a limited recent window.'
        )

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_lvgmc(timeout=timeout)
    station_id_lookup = _station_id_lookup(metadata_table)
    missing_station_ids = sorted(
        station_id
        for station_id in query.station_ids
        if station_id.strip().upper() not in station_id_lookup
    )
    if missing_station_ids:
        raise StationNotFoundError(f"No LVGMC station metadata found for station_id: {', '.join(missing_station_ids)}")

    frames: list[pd.DataFrame] = []
    for requested_station_id in query.station_ids:
        station_id = station_id_lookup[requested_station_id.strip().upper()]
        current_date = query.start_date
        while current_date <= query.end_date:
            records = _download_station_day_records(
                station_id=station_id,
                raw_elements=query.elements,
                observation_date=current_date,
                timeout=timeout,
            )
            normalized = normalize_lvgmc_daily_records(
                records,
                query=query,
                station_id=station_id,
                observation_date=current_date,
            )
            if not normalized.empty:
                element_columns = canonicalize_element_series(pd.Series(normalized['element_raw']), query)
                normalized['element'] = element_columns['element']
                normalized['element_raw'] = element_columns['element_raw']
                frames.append(normalized.loc[:, LV_NORMALIZED_DAILY_COLUMNS])
            current_date += timedelta(days=1)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=['station_id', 'element', 'observation_date']).reset_index(drop=True)
    return combined.loc[:, LV_NORMALIZED_DAILY_COLUMNS].sort_values(
        ['station_id', 'observation_date', 'element'],
        kind='stable',
    ).reset_index(drop=True)


def _download_station_day_records(
    *,
    station_id: str,
    raw_elements: list[str],
    observation_date,
    timeout: int,
) -> list[dict[str, object]]:
    spec = get_dataset_spec('lvgmc', 'daily')
    sql = build_lvgmc_daily_sql(
        resource_id=spec.archive_hourly_resource_id,
        station_id=station_id,
        raw_elements=raw_elements,
        observation_date=observation_date,
    )
    response = requests.get(
        spec.observation_sql_api_url,
        params={'sql': sql},
        timeout=timeout,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise DownloadError(
            f'Failed to download LVGMC daily observations for station_id {station_id} and date {observation_date.isoformat()}: {exc}'
        ) from exc
    response.encoding = 'utf-8'
    payload = parse_lvgmc_payload_json(response.text)
    result = payload.get('result') if isinstance(payload, dict) else None
    records = result.get('records') if isinstance(result, dict) else None
    if not isinstance(records, list):
        raise DownloadError(
            f'LVGMC daily observations for station_id {station_id} and date {observation_date.isoformat()} returned an unexpected payload shape.'
        )
    return [record for record in records if isinstance(record, dict)]


def build_lvgmc_daily_sql(
    *,
    resource_id: str,
    station_id: str,
    raw_elements: list[str],
    observation_date,
) -> str:
    start_iso = f'{observation_date.isoformat()}T00:00:00Z'
    end_iso = f'{(observation_date + timedelta(days=1)).isoformat()}T00:00:00Z'
    station_literal = _sql_literal(station_id)
    abbreviations_sql = ', '.join(_sql_literal(raw_code) for raw_code in raw_elements)
    return (
        'SELECT STATION_ID, ABBREVIATION, DATETIME, VALUE '
        f'FROM "{resource_id}" '
        f"WHERE STATION_ID = {station_literal} "
        f'AND ABBREVIATION IN ({abbreviations_sql}) '
        f"AND DATETIME > {_sql_literal(start_iso)} "
        f"AND DATETIME <= {_sql_literal(end_iso)} "
        'ORDER BY DATETIME ASC, ABBREVIATION ASC'
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _station_id_lookup(station_metadata: pd.DataFrame) -> dict[str, str]:
    station_map = station_metadata.attrs.get('station_provider_raw_elements_by_path', {}).get(('lvgmc', 'daily'))
    if isinstance(station_map, dict) and station_map:
        return {
            str(station_id).strip().upper(): str(station_id).strip()
            for station_id in station_map
        }
    return {
        str(station_id).strip().upper(): str(station_id).strip()
        for station_id in station_metadata['station_id'].astype(str)
    }
