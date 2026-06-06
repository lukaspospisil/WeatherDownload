from __future__ import annotations

from urllib.parse import urlencode

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import read_station_metadata_ad
from .parser import (
    AD_NORMALIZED_DAILY_COLUMNS,
    parse_meteo_ad_daily_workbook,
    parse_meteo_ad_numeric,
    parse_meteo_ad_observation_date,
)
from .registry import get_dataset_spec


def download_daily_observations_ad(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'meteo_ad' or query.resolution != 'daily':
        raise UnsupportedQueryError('The Meteo.ad Andorra daily downloader only supports meteo_ad/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The Meteo.ad Andorra daily downloader requires at least one element.')
    if query.start_date is None or query.end_date is None or query.all_history:
        raise UnsupportedQueryError('The Meteo.ad Andorra daily downloader requires explicit start_date and end_date values.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_ad(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No Meteo.ad Andorra station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No Meteo.ad Andorra station metadata found for station_id: {', '.join(missing_station_ids)}")

    parsed_tables: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        payload = download_meteo_ad_daily_workbook(
            station_id=station_id,
            raw_elements=list(query.elements or []),
            start_date=query.start_date,
            end_date=query.end_date,
            timeout=timeout,
        )
        table = parse_meteo_ad_daily_workbook(payload)
        if table.empty:
            continue
        table['_station_id'] = station_id
        parsed_tables.append(table)

    if not parsed_tables:
        raise EmptyResultError('No observations found for the given query.')

    normalized = normalize_daily_observations_ad(pd.concat(parsed_tables, ignore_index=True), query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, AD_NORMALIZED_DAILY_COLUMNS]


def download_meteo_ad_daily_workbook(
    *,
    station_id: str,
    raw_elements: list[str],
    start_date,
    end_date,
    timeout: int,
) -> bytes:
    spec = get_dataset_spec('meteo_ad', 'daily')
    params = [
        ('estacio', station_id),
        ('dia_desde', str(start_date.day)),
        ('mes_desde', str(start_date.month)),
        ('any_desde', str(start_date.year)),
        ('mesura', '0'),
        ('dia_hasta', str(end_date.day)),
        ('mes_hasta', str(end_date.month)),
        ('any_hasta', str(end_date.year)),
    ]
    params.extend(('dades', raw_code) for raw_code in raw_elements)
    request_url = f'{spec.daily_export_url}?{urlencode(params, doseq=True)}'
    response = requests.get(request_url, timeout=timeout)
    response.raise_for_status()
    return response.content


def normalize_daily_observations_ad(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if table.empty:
        return pd.DataFrame(columns=AD_NORMALIZED_DAILY_COLUMNS)

    rows: list[pd.DataFrame] = []
    observation_dates = table['observation_date_raw'].map(parse_meteo_ad_observation_date)
    for raw_code in query.elements or []:
        if raw_code not in table.columns:
            continue
        element_columns = canonicalize_element_series(pd.Series([raw_code] * len(table.index), index=table.index), query)
        normalized = pd.DataFrame(
            {
                'station_id': table['_station_id'].astype('string'),
                'gh_id': pd.NA,
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'observation_date': observation_dates,
                'time_function': pd.NA,
                'value': table[raw_code].map(lambda value: parse_meteo_ad_numeric(raw_code, value)),
                'flag': pd.NA,
                'quality': pd.Series(pd.NA, index=table.index, dtype='Int64'),
                'provider': query.provider,
                'resolution': query.resolution,
            }
        )
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=AD_NORMALIZED_DAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['station_id'].isin(query.station_ids)]
    combined = combined[combined['observation_date'].notna()]
    combined = combined[(combined['observation_date'] >= query.start_date) & (combined['observation_date'] <= query.end_date)]
    return combined.sort_values(['station_id', 'observation_date', 'element']).reset_index(drop=True)
