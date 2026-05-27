from __future__ import annotations

import pandas as pd
import requests

from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .parser import SI_ARSO_NORMALIZED_DAILY_COLUMNS, normalize_si_arso_daily_observation
from .registry import SI_ARSO_PID_BY_RAW, get_dataset_spec


def download_daily_observations_arso(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'arso' or query.resolution != 'daily':
        raise UnsupportedQueryError("The ARSO downloader supports only provider='arso' and resolution='daily'.")
    if not query.elements:
        raise UnsupportedQueryError('The ARSO daily downloader requires at least one element.')
    if query.start_date is None or query.end_date is None:
        raise UnsupportedQueryError('The ARSO daily downloader requires start_date and end_date.')

    available_station_ids = _available_station_ids(station_metadata)
    if available_station_ids:
        missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
        if missing_station_ids:
            raise StationNotFoundError(f"No ARSO station metadata found for station_id: {', '.join(missing_station_ids)}")

    spec = get_dataset_spec('arso', 'daily')
    var_ids = ','.join(SI_ARSO_PID_BY_RAW[element_raw] for element_raw in query.elements)
    frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        url = (
            f'{spec.observation_base_url}?type=daily&id={station_id}'
            f'&d1={query.start_date.isoformat()}&d2={query.end_date.isoformat()}&vars={var_ids}'
        )
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        frame = normalize_si_arso_daily_observation(
            response.text,
            station_id=station_id,
            provider=query.provider,
            resolution=query.resolution,
            start_date=query.start_date,
            end_date=query.end_date,
            allowed_raw_elements=set(query.elements),
        )
        if not frame.empty:
            frames.append(frame)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, SI_ARSO_NORMALIZED_DAILY_COLUMNS]


def _available_station_ids(station_metadata: pd.DataFrame | None) -> set[str]:
    if station_metadata is None or station_metadata.empty:
        return set()
    station_map = station_metadata.attrs.get('station_provider_raw_elements_by_path', {}).get(('arso', 'daily'))
    if isinstance(station_map, dict):
        return {str(station_id) for station_id in station_map}
    return set(station_metadata['station_id'].astype(str).tolist())
