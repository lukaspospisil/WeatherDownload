from __future__ import annotations

import pandas as pd

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .metadata import download_aemet_dataset_text, read_station_metadata_es, resolve_aemet_api_key
from .parser import ES_NORMALIZED_DAILY_COLUMNS, parse_aemet_daily_data_json, parse_aemet_numeric, parse_aemet_observation_date
from .registry import AEMET_DAILY_ENDPOINT_TEMPLATE, AEMET_OPEN_DATA_BASE_URL


def download_daily_observations_es(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'aemet' or query.resolution != 'daily':
        raise UnsupportedQueryError('The AEMET Spain downloader supports only aemet/daily.')
    if not query.elements:
        raise UnsupportedQueryError('The AEMET Spain daily downloader requires at least one element.')
    if query.all_history:
        raise NotImplementedError('AEMET Spain daily all_history mode is not implemented because station coverage dates are not available in the inventory metadata.')

    api_key = resolve_aemet_api_key()
    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_es(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No AEMET Spain station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No AEMET Spain station metadata found for station_id: {', '.join(missing_station_ids)}")

    endpoint = _build_daily_endpoint(query)
    payload_text = download_aemet_dataset_text(endpoint=endpoint, timeout=timeout, api_key=api_key)
    payload = parse_aemet_daily_data_json(payload_text)
    normalized = normalize_daily_observations_es(payload, query)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, ES_NORMALIZED_DAILY_COLUMNS]


def normalize_daily_observations_es(payload: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if payload.empty:
        return pd.DataFrame(columns=ES_NORMALIZED_DAILY_COLUMNS)

    rows: list[dict[str, object]] = []
    for record in payload.to_dict(orient='records'):
        station_id = str(record.get('indicativo', '')).strip().upper()
        if station_id not in query.station_ids:
            continue
        observation_date = parse_aemet_observation_date(record.get('fecha'))
        if observation_date is None:
            continue
        if query.start_date is not None and observation_date < query.start_date:
            continue
        if query.end_date is not None and observation_date > query.end_date:
            continue
        for raw_code in query.elements or []:
            element_columns = canonicalize_element_series(pd.Series([raw_code]), query)
            rows.append(
                {
                    'station_id': station_id,
                    'gh_id': pd.NA,
                    'element': element_columns.iloc[0]['element'],
                    'element_raw': element_columns.iloc[0]['element_raw'],
                    'observation_date': observation_date,
                    'time_function': pd.NA,
                    'value': parse_aemet_numeric(raw_code, record.get(raw_code)),
                    'flag': pd.NA,
                    'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                    'provider': query.provider,
                    'resolution': query.resolution,
                }
            )

    frame = pd.DataFrame.from_records(rows, columns=ES_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series(frame['quality'], dtype='Int64')
    return frame.sort_values(['station_id', 'observation_date', 'element']).reset_index(drop=True)


def _build_daily_endpoint(query: ObservationQuery) -> str:
    fecha_ini = f'{query.start_date.isoformat()}T00:00:00UTC'
    fecha_fin = f'{query.end_date.isoformat()}T23:59:59UTC'
    station_ids = ','.join(query.station_ids)
    path = AEMET_DAILY_ENDPOINT_TEMPLATE.format(fecha_ini=fecha_ini, fecha_fin=fecha_fin, station_ids=station_ids)
    return f'{AEMET_OPEN_DATA_BASE_URL}{path}'
