from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from ...errors import EmptyResultError, UnsupportedQueryError
from ...queries import ObservationQuery
from ...metadata import STATION_METADATA_COLUMNS
from .fmi_parser import FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS, normalize_fmi_timevaluepair_hourly_observations
from .registry import get_dataset_spec


def download_hourly_observations_fmi(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.provider != 'fmi' or query.resolution != '1hour':
        raise UnsupportedQueryError("FMI Finland downloader supports only provider='fmi' and resolution='1hour'.")
    if query.all_history:
        raise UnsupportedQueryError("FMI Finland downloader does not support all_history for provider='fmi' yet.")
    if not query.elements:
        raise UnsupportedQueryError('FMI Finland hourly downloader requires at least one element.')
    if query.start is None or query.end is None:
        raise UnsupportedQueryError('FMI Finland hourly downloader requires start and end timestamps.')

    source = _resolve_local_fixture_source(station_metadata)
    if source is not None:
        xml_text = Path(source).read_text(encoding='utf-8')
        normalized = normalize_fmi_timevaluepair_hourly_observations(
            xml_text,
            station_ids=query.station_ids,
            raw_elements=query.elements,
            provider=query.provider,
            resolution=query.resolution,
        )
        normalized = _filter_time_range(normalized, query)
        if normalized.empty:
            raise EmptyResultError('No observations found for the given query.')
        return normalized.loc[:, FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS]

    spec = get_dataset_spec(query.provider, query.resolution)
    params_template = {
        'service': 'WFS',
        'version': '2.0.0',
        'request': 'getFeature',
        'storedquery_id': spec.storedquery_id,
        'starttime': _format_fmi_timestamp(query.start),
        'endtime': _format_fmi_timestamp(query.end),
        'timestep': str(spec.timestep_minutes),
        'parameters': ','.join(query.elements),
    }

    frames: list[pd.DataFrame] = []
    for station_id in query.station_ids:
        params = {**params_template, 'fmisid': str(station_id).strip()}
        response = requests.get(spec.wfs_url, params=params, timeout=timeout)
        response.raise_for_status()
        response.encoding = 'utf-8'
        normalized = normalize_fmi_timevaluepair_hourly_observations(
            response.text,
            station_ids=[station_id],
            raw_elements=query.elements,
            provider=query.provider,
            resolution=query.resolution,
        )
        normalized = _filter_time_range(normalized, query)
        if not normalized.empty:
            frames.append(normalized)

    if not frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'timestamp', 'element'], kind='stable').reset_index(drop=True)
    return combined.loc[:, FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS]


def build_minimal_station_metadata_from_timevaluepair_fixture(source_url: str) -> pd.DataFrame:
    xml_text = Path(source_url).read_text(encoding='utf-8')
    parsed = normalize_fmi_timevaluepair_hourly_observations(xml_text)
    station_ids = sorted({str(value) for value in parsed.attrs.get('station_id_candidates', []) if str(value).strip()})
    rows = [
        {
            'station_id': station_id,
            'gh_id': pd.NA,
            'begin_date': '',
            'end_date': '',
            'full_name': pd.NA,
            'longitude': pd.NA,
            'latitude': pd.NA,
            'elevation_m': pd.NA,
        }
        for station_id in station_ids
    ]
    table = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    table.attrs['source_url'] = source_url
    return table


def _resolve_local_fixture_source(station_metadata: pd.DataFrame | None) -> str | None:
    if station_metadata is None:
        return None
    source = station_metadata.attrs.get('source_url')
    if not isinstance(source, str):
        return None
    return source if Path(source).exists() else None


def _format_fmi_timestamp(value: object) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return ts.strftime('%Y-%m-%dT%H:%M:%SZ')


def _filter_time_range(table: pd.DataFrame, query: ObservationQuery) -> pd.DataFrame:
    if table.empty:
        return table
    start = pd.Timestamp(query.start)
    end = pd.Timestamp(query.end)
    if start.tzinfo is None:
        start = start.tz_localize('UTC')
    else:
        start = start.tz_convert('UTC')
    if end.tzinfo is None:
        end = end.tz_localize('UTC')
    else:
        end = end.tz_convert('UTC')
    filtered = table.copy()
    filtered['timestamp'] = pd.to_datetime(filtered['timestamp'], utc=True, errors='coerce')
    filtered = filtered[filtered['timestamp'].notna()]
    filtered = filtered[(filtered['timestamp'] >= start) & (filtered['timestamp'] <= end)]
    return filtered

