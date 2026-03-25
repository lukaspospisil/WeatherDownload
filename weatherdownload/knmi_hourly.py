from __future__ import annotations

import re

import pandas as pd

from .elements import canonicalize_element_series
from .errors import DownloadError, EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .knmi_metadata import download_knmi_file_bytes, list_knmi_files, read_station_metadata_knmi, resolve_knmi_api_key
from .knmi_parser import KNMI_NORMALIZED_SUBDAILY_COLUMNS, parse_knmi_hourly_netcdf_bytes
from .knmi_registry import KNMI_HOURLY_FILENAME_PREFIX, get_dataset_spec
from .queries import ObservationQuery

_KNMI_HOURLY_FILENAME_PATTERN = re.compile(r'(?P<prefix>hourly-observations-)(?P<date>\d{8})-(?P<hour>\d{2})\.nc$', re.IGNORECASE)


def download_hourly_observations_knmi(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution != '1hour':
        raise UnsupportedQueryError('The KNMI hourly downloader only supports historical/1hour.')
    if not query.elements:
        raise UnsupportedQueryError('The KNMI hourly downloader requires at least one element.')

    api_key = resolve_knmi_api_key()
    metadata_table = station_metadata if station_metadata is not None else read_station_metadata_knmi(timeout=timeout)
    if metadata_table.empty:
        raise EmptyResultError('No KNMI station metadata are available.')

    available_station_ids = set(metadata_table['station_id'].astype(str))
    missing_station_ids = sorted(set(query.station_ids) - available_station_ids)
    if missing_station_ids:
        raise StationNotFoundError(f"No KNMI station metadata found for station_id: {', '.join(missing_station_ids)}")

    request_start, request_end = _resolve_request_range(query, metadata_table)
    filenames = _list_hourly_filenames(request_start=request_start, request_end=request_end, timeout=timeout, api_key=api_key)
    if not filenames:
        raise EmptyResultError('No KNMI hourly files were found for the requested timestamp range.')

    spec = get_dataset_spec('historical', '1hour')
    parsed_payloads = []
    for filename in filenames:
        netcdf_bytes = download_knmi_file_bytes(
            dataset_name=spec.dataset_name,
            dataset_version=spec.dataset_version,
            filename=filename,
            timeout=timeout,
            api_key=api_key,
        )
        parsed_payloads.append(parse_knmi_hourly_netcdf_bytes(netcdf_bytes))

    normalized_frames = [
        normalize_hourly_observations_knmi(payload, query, station_metadata=metadata_table)
        for payload in parsed_payloads
    ]
    normalized_frames = [frame for frame in normalized_frames if not frame.empty]
    if not normalized_frames:
        raise EmptyResultError('No observations found for the given query.')
    combined = pd.concat(normalized_frames, ignore_index=True)
    combined = combined.sort_values(['station_id', 'timestamp', 'element']).reset_index(drop=True)
    return combined.loc[:, KNMI_NORMALIZED_SUBDAILY_COLUMNS]


def normalize_hourly_observations_knmi(
    payload: dict[str, object],
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    station_frame = payload.get('stations')
    if not isinstance(station_frame, pd.DataFrame) or station_frame.empty:
        return pd.DataFrame(columns=KNMI_NORMALIZED_SUBDAILY_COLUMNS)
    timestamp = payload.get('timestamp')
    if not isinstance(timestamp, pd.Timestamp):
        return pd.DataFrame(columns=KNMI_NORMALIZED_SUBDAILY_COLUMNS)
    variables = payload.get('variables')
    if not isinstance(variables, dict):
        return pd.DataFrame(columns=KNMI_NORMALIZED_SUBDAILY_COLUMNS)

    selected = station_frame.copy()
    selected['station_id'] = selected['station_id'].astype(str).str.strip()
    selected = selected[selected['station_id'].isin(query.station_ids)].reset_index(drop=True)
    if selected.empty:
        return pd.DataFrame(columns=KNMI_NORMALIZED_SUBDAILY_COLUMNS)

    metadata_lookup = None
    if station_metadata is not None and not station_metadata.empty:
        metadata_lookup = station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])

    source_station_ids = station_frame['station_id'].astype(str).str.strip().tolist()
    source_index = {station_id: index for index, station_id in enumerate(source_station_ids)}

    rows: list[pd.DataFrame] = []
    for raw_code in query.elements or []:
        values = variables.get(raw_code)
        if not isinstance(values, pd.Series):
            continue
        element_columns = canonicalize_element_series(pd.Series([raw_code] * len(selected.index), index=selected.index), query)
        selected_values = [values.iloc[source_index[station_id]] if station_id in source_index else pd.NA for station_id in selected['station_id']]
        normalized = pd.DataFrame(
            {
                'station_id': selected['station_id'],
                'element': element_columns['element'],
                'element_raw': element_columns['element_raw'],
                'timestamp': pd.Series([timestamp] * len(selected.index)),
                'value': pd.to_numeric(pd.Series(selected_values), errors='coerce'),
                'flag': pd.NA,
                'quality': pd.Series(pd.NA, index=selected.index, dtype='Int64'),
                'dataset_scope': query.dataset_scope,
                'resolution': query.resolution,
                'gh_id': pd.NA,
            }
        )
        if metadata_lookup is not None:
            normalized = normalized.drop(columns=['gh_id']).merge(metadata_lookup, on='station_id', how='left')
        rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=KNMI_NORMALIZED_SUBDAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    if query.start is not None:
        combined = combined[combined['timestamp'] >= pd.Timestamp(query.start)]
    if query.end is not None:
        combined = combined[combined['timestamp'] <= pd.Timestamp(query.end)]
    return combined.loc[:, KNMI_NORMALIZED_SUBDAILY_COLUMNS].reset_index(drop=True)


def _resolve_request_range(query: ObservationQuery, station_metadata: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    if not query.all_history:
        return pd.Timestamp(query.start), pd.Timestamp(query.end)
    selected = station_metadata[station_metadata['station_id'].isin(query.station_ids)].copy()
    begin = pd.to_datetime(selected['begin_date'], utc=True, errors='coerce').min()
    end = pd.to_datetime(selected['end_date'], utc=True, errors='coerce').max()
    if pd.isna(begin) or pd.isna(end):
        raise UnsupportedQueryError('KNMI all_history mode requires station coverage metadata.')
    return begin, end


def _list_hourly_filenames(*, request_start: pd.Timestamp, request_end: pd.Timestamp, timeout: int, api_key: str) -> list[str]:
    spec = get_dataset_spec('historical', '1hour')
    begin_filename = f'{KNMI_HOURLY_FILENAME_PREFIX}{request_start.strftime("%Y%m%d-%H")}.nc'
    next_page_token: str | None = None
    filenames: list[str] = []

    while True:
        params = {
            'maxKeys': '500',
            'orderBy': 'filename',
            'sorting': 'asc',
            'begin': begin_filename,
        }
        if next_page_token:
            params['nextPageToken'] = next_page_token
        payload = list_knmi_files(
            dataset_name=spec.dataset_name,
            dataset_version=spec.dataset_version,
            timeout=timeout,
            api_key=api_key,
            params=params,
        )
        files = payload.get('files', [])
        if not isinstance(files, list):
            raise DownloadError('KNMI hourly file listing returned an invalid files payload.')
        for file_info in files:
            filename = file_info.get('filename') if isinstance(file_info, dict) else None
            if not isinstance(filename, str):
                continue
            file_timestamp = _parse_knmi_hourly_filename_timestamp(filename)
            if file_timestamp is None:
                continue
            if file_timestamp > request_end:
                return filenames
            if file_timestamp < request_start:
                continue
            filenames.append(filename)
        next_page_token = payload.get('nextPageToken')
        if not next_page_token:
            break
    return filenames


def _parse_knmi_hourly_filename_timestamp(filename: str) -> pd.Timestamp | None:
    match = _KNMI_HOURLY_FILENAME_PATTERN.search(filename.strip())
    if match is None:
        return None
    return pd.Timestamp(f"{match.group('date')}T{match.group('hour')}:00:00Z")
