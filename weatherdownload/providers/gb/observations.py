from __future__ import annotations

from datetime import timedelta, timezone

import pandas as pd
import requests

from ...elements import canonicalize_element_series
from ...errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from ...queries import ObservationQuery
from ..ghcnd.mixed import build_mixed_observation_downloader
from ..ghcnd.wrappers import build_daily_observation_downloader, build_station_dly_url_builder
from .metadata import read_station_metadata_metoffice_datahub, resolve_metoffice_datahub_api_key
from .parser import (
    GB_METOFFICE_DATAHUB_NORMALIZED_SUBDAILY_COLUMNS,
    normalize_metoffice_datahub_hourly_observations,
    parse_metoffice_datahub_hourly_observations_json,
)
from .registry import get_dataset_spec

download_daily_observations_ghcnd = build_daily_observation_downloader(get_dataset_spec=get_dataset_spec)
build_station_dly_url = build_station_dly_url_builder(get_dataset_spec=get_dataset_spec)


def download_hourly_observations_metoffice_datahub(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'metoffice_datahub' or query.resolution != '1hour':
        raise UnsupportedQueryError(
            "The Met Office Weather DataHub GB downloader supports only provider='metoffice_datahub' and resolution='1hour'."
        )
    if query.all_history:
        raise UnsupportedQueryError(
            "The Met Office Weather DataHub GB downloader does not support all_history for provider='metoffice_datahub'."
        )
    if not query.elements:
        raise UnsupportedQueryError('The Met Office Weather DataHub GB hourly downloader requires at least one element.')
    if query.start is None or query.end is None:
        raise UnsupportedQueryError('The Met Office Weather DataHub GB hourly downloader requires start and end timestamps.')

    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_metoffice_datahub(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No Met Office Weather DataHub GB station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(
            f"No Met Office Weather DataHub GB station metadata found for station_id: {', '.join(missing_station_ids)}"
        )

    _validate_recent_window(query)
    api_key = resolve_metoffice_datahub_api_key()
    spec = get_dataset_spec('metoffice_datahub', '1hour')
    normalized_frames: list[pd.DataFrame] = []

    for station_id in query.station_ids:
        url = (spec.data_url or '').format(station_id=station_id)
        response = requests.get(url, headers={'apikey': api_key}, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        parsed = parse_metoffice_datahub_hourly_observations_json(response.text)
        normalized = normalize_metoffice_datahub_hourly_observations(
            parsed,
            station_ids=[station_id],
            raw_elements=query.elements or [],
            start=query.start.isoformat(),
            end=query.end.isoformat(),
            provider=query.provider,
            resolution=query.resolution,
        )
        if not normalized.empty:
            element_columns = canonicalize_element_series(normalized['element_raw'], query)
            normalized['element'] = element_columns['element']
            normalized['element_raw'] = element_columns['element_raw']
            normalized_frames.append(normalized.loc[:, GB_METOFFICE_DATAHUB_NORMALIZED_SUBDAILY_COLUMNS])

    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(normalized_frames, ignore_index=True)
    return combined.sort_values(['station_id', 'timestamp', 'element'], kind='stable').reset_index(drop=True)


def _validate_recent_window(query: ObservationQuery) -> None:
    now_utc = pd.Timestamp(_current_utc())
    if now_utc.tzinfo is None:
        now_utc = now_utc.tz_localize('UTC')
    else:
        now_utc = now_utc.tz_convert('UTC')
    earliest = now_utc - timedelta(hours=48)

    start_ts = pd.Timestamp(query.start)
    end_ts = pd.Timestamp(query.end)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize('UTC')
    else:
        start_ts = start_ts.tz_convert('UTC')
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize('UTC')
    else:
        end_ts = end_ts.tz_convert('UTC')

    if start_ts < earliest or end_ts > now_utc:
        raise UnsupportedQueryError(
            'Met Office Weather DataHub GB hourly observations only cover the last 48 hours. '
            f'Requested window {start_ts.isoformat()} to {end_ts.isoformat()} falls outside '
            f'{earliest.isoformat()} to {now_utc.isoformat()}.'
        )


def _current_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz=timezone.utc)


def _download_national_observations(*args, **kwargs):
    query = args[0] if args else kwargs.get('query')
    if getattr(query, 'provider', None) == 'metoffice_datahub' and getattr(query, 'resolution', None) == '1hour':
        return download_hourly_observations_metoffice_datahub(*args, **kwargs)
    raise NotImplementedError(
        'Great Britain national provider support currently implements only metoffice_datahub/1hour.'
    )


download_observations = build_mixed_observation_downloader(
    download_national_observations=_download_national_observations,
    download_ghcnd_observations=download_daily_observations_ghcnd,
)
