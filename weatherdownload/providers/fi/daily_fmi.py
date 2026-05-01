from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...errors import EmptyResultError, UnsupportedQueryError
from ...queries import ObservationQuery
from .fmi_parser import FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS, normalize_fmi_timevaluepair_daily_observations
from .registry import get_dataset_spec


def download_daily_observations_fmi(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'fmi' or query.resolution != 'daily':
        raise UnsupportedQueryError("FMI Finland downloader supports only provider='fmi' and resolution='daily'.")
    if query.all_history:
        raise UnsupportedQueryError("FMI Finland daily downloader does not support all_history for provider='fmi' yet.")
    if not query.elements:
        raise UnsupportedQueryError('FMI Finland daily downloader requires at least one element.')
    if query.start_date is None or query.end_date is None:
        raise UnsupportedQueryError('FMI Finland daily downloader requires start_date and end_date.')

    source = _resolve_local_fixture_source(station_metadata)
    if source is not None:
        xml_text = Path(source).read_text(encoding='utf-8')
        normalized = normalize_fmi_timevaluepair_daily_observations(
            xml_text,
            station_ids=query.station_ids,
            raw_elements=query.elements,
            provider=query.provider,
            resolution=query.resolution,
        )
        normalized = _filter_date_range(normalized, query)
        if normalized.empty:
            raise EmptyResultError('No observations found for the given query.')
        return normalized.loc[:, FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS]

    spec = get_dataset_spec(query.provider, query.resolution)
    params_template = {
        'service': 'WFS',
        'version': '2.0.0',
        'request': 'getFeature',
        'storedquery_id': spec.storedquery_id,
        'starttime': _format_fmi_date(query.start_date),
        'endtime': _format_fmi_date(query.end_date),
        'timestep': str(spec.timestep_minutes),
        'parameters': ','.join(query.elements),
    }

    frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        params = {**params_template, 'fmisid': str(station_id).strip()}
        response = requests.get(spec.wfs_url, params=params, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        normalized = normalize_fmi_timevaluepair_daily_observations(
            response.text,
            station_ids=[station_id],
            raw_elements=query.elements,
            provider=query.provider,
            resolution=query.resolution,
        )
        normalized = _filter_date_range(normalized, query)
        if not normalized.empty:
            frames.append(normalized)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'timestamp', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS]


def _resolve_local_fixture_source(station_metadata: pd.DataFrame | None) -> str | None:
    if station_metadata is None:
        return None
    source = station_metadata.attrs.get('source_url')
    if not isinstance(source, str):
        return None
    return source if Path(source).exists() else None


def _format_fmi_date(value: object) -> str:
    # Daily stored query accepts ISO dates without a time component.
    ts = pd.Timestamp(value)
    return ts.strftime('%Y-%m-%d')


def _filter_date_range(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if table.empty:
        return table
    start = pd.Timestamp(query.start_date).tz_localize('UTC')
    end = pd.Timestamp(query.end_date).tz_localize('UTC')
    filtered = table.copy()
    filtered['timestamp'] = pd.to_datetime(filtered['timestamp'], utc=True, errors='coerce')
    filtered = filtered[filtered['timestamp'].notna()]
    # Daily timevaluepair timestamps are at 00:00Z; interpret them as dates.
    filtered = filtered[(filtered['timestamp'] >= start) & (filtered['timestamp'] <= end)]
    return filtered

