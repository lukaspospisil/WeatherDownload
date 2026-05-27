from __future__ import annotations

import pandas as pd
import requests

from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from .parser import RO_ANM_NORMALIZED_DAILY_COLUMNS, normalize_ro_anm_daily_observation


def download_daily_observations_anm(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'anm' or query.resolution != 'daily':
        raise UnsupportedQueryError("The ANM downloader supports only provider='anm' and resolution='daily'.")
    if not query.elements:
        raise UnsupportedQueryError('The ANM daily downloader requires at least one element.')

    available_station_ids = _available_station_ids(station_metadata)
    if available_station_ids:
        missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
        if missing_station_ids:
            raise StationNotFoundError(f"No ANM station metadata found for station_id: {', '.join(missing_station_ids)}")

    frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        for raw_code in query.elements:
            url = (
                'https://inspire.meteoromania.ro/ids/'
                f'OM_Observation.EnvironmentalMonitoringFacility.{station_id}.{raw_code}.AllValuesWML20'
            )
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            frame = normalize_ro_anm_daily_observation(
                response.text,
                station_id=station_id,
                element_raw=raw_code,
                provider=query.provider,
                resolution=query.resolution,
                start_date=query.start_date,
                end_date=query.end_date,
            )
            if not frame.empty:
                frames.append(frame)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, RO_ANM_NORMALIZED_DAILY_COLUMNS]


def _available_station_ids(station_metadata: pd.DataFrame | None) -> set[str]:
    if station_metadata is None or station_metadata.empty:
        return set()
    station_map = station_metadata.attrs.get('station_provider_raw_elements_by_path', {}).get(('anm', 'daily'))
    if isinstance(station_map, dict):
        return {str(station_id) for station_id in station_map}
    return set(station_metadata['station_id'].astype(str).tolist())
