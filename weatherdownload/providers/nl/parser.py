from __future__ import annotations

import io
import json
import os
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

KNMI_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]
KNMI_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'provider', 'resolution',
]

_STATION_ID_ALIASES = {'wsi', 'stationid', 'station_id', 'wigosstationidentifier', 'wigosstationid', 'wigosid'}
_NAME_ALIASES = {'naam', 'name', 'stationname', 'station_name'}
_LATITUDE_ALIASES = {'lat', 'latitude', 'breedtegraad', 'y'}
_LONGITUDE_ALIASES = {'lon', 'longitude', 'lengtegraad', 'x'}
_ELEVATION_ALIASES = {'height', 'hoogte', 'elevation', 'altitude', 'stationheight'}
_BEGIN_DATE_ALIASES = {'begin_date', 'begindate', 'from', 'validfrom', 'startdate', 'operationeelvanaf'}
_END_DATE_ALIASES = {'end_date', 'enddate', 'until', 'validto', 'einddate', 'operationeeltot'}
_TIME_VARIABLE_ALIASES = ('time', 'valid_time', 'timestamp')
_STATION_VARIABLE_ALIASES = ('station', 'station_id', 'stationid', 'wigos_station_identifier', 'wigos_station_id', 'wsi')
_STATION_NAME_VARIABLE_ALIASES = ('station_name', 'name')
_LATITUDE_VARIABLE_ALIASES = ('lat', 'latitude')
_LONGITUDE_VARIABLE_ALIASES = ('lon', 'longitude')
_ELEVATION_VARIABLE_ALIASES = ('height', 'elevation', 'altitude', 'station_height')


def parse_knmi_api_listing_json(json_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(json_text.lstrip('\ufeff'))
    except json.JSONDecodeError as exc:
        raise ValueError('KNMI Open Data API response is not valid JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('KNMI Open Data API response must be a top-level JSON object.')
    files = payload.get('files')
    if files is None or not isinstance(files, list):
        raise ValueError('KNMI Open Data API response is missing a files list.')
    return payload


def parse_knmi_station_metadata_csv(csv_text: str) -> pd.DataFrame:
    table = pd.read_csv(io.StringIO(csv_text.lstrip('\ufeff')), dtype=str)
    if table.empty:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    normalized_columns = {_normalize_column_name(column): column for column in table.columns}
    station_column = _resolve_column(normalized_columns, _STATION_ID_ALIASES)
    if station_column is None:
        raise ValueError('KNMI station metadata CSV is missing a station identifier column.')
    name_column = _resolve_column(normalized_columns, _NAME_ALIASES)
    latitude_column = _resolve_column(normalized_columns, _LATITUDE_ALIASES)
    longitude_column = _resolve_column(normalized_columns, _LONGITUDE_ALIASES)
    elevation_column = _resolve_column(normalized_columns, _ELEVATION_ALIASES)
    begin_column = _resolve_column(normalized_columns, _BEGIN_DATE_ALIASES)
    end_column = _resolve_column(normalized_columns, _END_DATE_ALIASES)

    rows: list[dict[str, object]] = []
    for row in table.to_dict(orient='records'):
        station_id = _clean_string(row.get(station_column))
        if not station_id:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': normalize_knmi_metadata_datetime(row.get(begin_column)),
                'end_date': normalize_knmi_metadata_datetime(row.get(end_column)),
                'full_name': _clean_string(row.get(name_column)) or pd.NA,
                'longitude': _parse_float(row.get(longitude_column)),
                'latitude': _parse_float(row.get(latitude_column)),
                'elevation_m': _parse_float(row.get(elevation_column)),
            }
        )
    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(['station_id']).drop_duplicates(subset=['station_id'], keep='first').reset_index(drop=True)


def normalize_knmi_metadata_datetime(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    timestamp = pd.Timestamp(cleaned)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    else:
        timestamp = timestamp.tz_convert('UTC')
    return timestamp.strftime('%Y-%m-%dT%H:%MZ')


def normalize_knmi_observation_metadata(stations: pd.DataFrame, spec: Any, parameter_metadata: dict[str, dict[str, str]]) -> pd.DataFrame:
    if spec.resolution == 'daily':
        obs_type = 'HISTORICAL_DAILY'
        schedule = 'P1D KNMI Open Data API'
    elif spec.resolution == '1hour':
        obs_type = 'HISTORICAL_HOURLY'
        schedule = 'PT1H KNMI Open Data API'
    elif spec.resolution == '10min':
        obs_type = 'HISTORICAL_10MIN'
        schedule = 'PT10M KNMI Open Data API'
    else:
        raise ValueError(f'Unsupported KNMI resolution for metadata normalization: {spec.resolution}')

    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in spec.supported_elements:
            if raw_code == 'RH':
                metadata_key = 'RH_DAILY' if spec.resolution == 'daily' else 'RH_HOUR'
            elif raw_code == 'SQ':
                metadata_key = 'SQ_DAILY' if spec.resolution == 'daily' else 'SQ_HOUR'
            elif spec.resolution == '10min':
                metadata_key = f'{raw_code}_10MIN'
            else:
                metadata_key = raw_code
            metadata = parameter_metadata.get(metadata_key, parameter_metadata.get(raw_code, {}))
            rows.append(
                {
                    'obs_type': obs_type,
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': schedule,
                    'name': metadata.get('name', raw_code),
                    'description': metadata.get('description', pd.NA),
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_knmi_daily_netcdf_bytes(netcdf_bytes: bytes) -> dict[str, object]:
    payload = _parse_knmi_netcdf_bytes(netcdf_bytes)
    return {
        'observation_date': (payload['timestamp'] - timedelta(days=1)).date(),
        'stations': payload['stations'],
        'variables': payload['variables'],
    }


def parse_knmi_hourly_netcdf_bytes(netcdf_bytes: bytes) -> dict[str, object]:
    payload = _parse_knmi_netcdf_bytes(netcdf_bytes)
    return {
        'timestamp': payload['timestamp'],
        'stations': payload['stations'],
        'variables': payload['variables'],
    }

def parse_knmi_tenmin_netcdf_bytes(netcdf_bytes: bytes) -> dict[str, object]:
    payload = _parse_knmi_netcdf_bytes(netcdf_bytes)
    return {
        'timestamp': payload['timestamp'],
        'stations': payload['stations'],
        'variables': payload['variables'],
    }


def _parse_knmi_netcdf_bytes(netcdf_bytes: bytes) -> dict[str, object]:
    try:
        import netCDF4
    except ImportError as exc:
        raise RuntimeError('KNMI NetCDF support requires the optional dependency netCDF4.') from exc

    temp_path: str | None = None
    dataset = None
    try:
        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as handle:
            handle.write(netcdf_bytes)
            temp_path = handle.name
        dataset = netCDF4.Dataset(temp_path, mode='r')
        station_ids = _extract_string_variable(dataset, _STATION_VARIABLE_ALIASES)
        if station_ids.empty:
            raise ValueError('KNMI NetCDF is missing station identifiers.')
        station_frame = pd.DataFrame(
            {
                'station_id': station_ids,
                'full_name': _extract_string_variable(dataset, _STATION_NAME_VARIABLE_ALIASES, expected_length=len(station_ids)),
                'latitude': _extract_numeric_variable(dataset, _LATITUDE_VARIABLE_ALIASES, expected_length=len(station_ids)),
                'longitude': _extract_numeric_variable(dataset, _LONGITUDE_VARIABLE_ALIASES, expected_length=len(station_ids)),
                'elevation_m': _extract_numeric_variable(dataset, _ELEVATION_VARIABLE_ALIASES, expected_length=len(station_ids)),
            }
        )
        variables: dict[str, pd.Series] = {}
        for variable_name in dataset.variables:
            upper_name = variable_name.strip().upper()
            if upper_name in {'TIME', 'LAT', 'LON', 'LATITUDE', 'LONGITUDE', 'HEIGHT', 'ELEVATION', 'ALTITUDE'}:
                continue
            if upper_name in {'STATION', 'STATION_ID', 'STATIONID', 'WIGOS_STATION_IDENTIFIER', 'WIGOS_STATION_ID', 'WSI', 'STATION_NAME', 'NAME'}:
                continue
            values = _coerce_data_vector(dataset.variables[variable_name][:], expected_length=len(station_ids))
            if values is None:
                continue
            variables[variable_name.strip()] = values
        timestamp = _extract_timestamp(dataset, netCDF4)
        return {
            'timestamp': timestamp,
            'stations': station_frame,
            'variables': variables,
        }
    finally:
        if dataset is not None:
            dataset.close()
        if temp_path is not None and os.path.exists(temp_path):
            Path(temp_path).unlink(missing_ok=True)


def _extract_timestamp(dataset: Any, netcdf4_module: Any) -> pd.Timestamp:
    time_variable_name = _first_present_variable_name(dataset, _TIME_VARIABLE_ALIASES)
    if time_variable_name is None:
        raise ValueError('KNMI NetCDF is missing a time variable.')
    variable = dataset.variables[time_variable_name]
    raw_values = variable[:]
    if hasattr(raw_values, 'shape') and len(getattr(raw_values, 'shape', ())) > 0:
        flattened = pd.Series(raw_values.reshape(-1))
        if flattened.empty:
            raise ValueError('KNMI NetCDF time variable is empty.')
        raw_value = flattened.iloc[0]
    else:
        raw_value = raw_values
    if hasattr(variable, 'units') and not isinstance(raw_value, str):
        converted = netcdf4_module.num2date(raw_value, units=variable.units)
        timestamp = pd.Timestamp(str(converted))
    else:
        timestamp = pd.Timestamp(str(raw_value))
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    else:
        timestamp = timestamp.tz_convert('UTC')
    return timestamp


def _extract_string_variable(dataset: Any, aliases: tuple[str, ...], expected_length: int | None = None) -> pd.Series:
    variable_name = _first_present_variable_name(dataset, aliases)
    if variable_name is None:
        if expected_length is None:
            return pd.Series(dtype='string')
        return pd.Series([pd.NA] * expected_length, dtype='string')
    series = _coerce_string_vector(dataset.variables[variable_name][:])
    if expected_length is not None and len(series) != expected_length:
        raise ValueError(f'KNMI NetCDF variable {variable_name!r} does not match the station dimension.')
    return series


def _extract_numeric_variable(dataset: Any, aliases: tuple[str, ...], expected_length: int) -> pd.Series:
    variable_name = _first_present_variable_name(dataset, aliases)
    if variable_name is None:
        return pd.Series([pd.NA] * expected_length, dtype='Float64')
    values = _coerce_data_vector(dataset.variables[variable_name][:], expected_length=expected_length)
    if values is None:
        return pd.Series([pd.NA] * expected_length, dtype='Float64')
    return pd.to_numeric(values, errors='coerce').astype('Float64')


def _coerce_string_vector(values: Any) -> pd.Series:
    if getattr(values, 'ndim', 1) == 2:
        rows = []
        for row in values:
            rows.append(''.join(_decode_char(item) for item in row).strip())
        return pd.Series(rows, dtype='string')
    flattened = pd.Series(values.reshape(-1) if hasattr(values, 'reshape') else list(values))
    return flattened.map(_clean_string).astype('string')


def _coerce_data_vector(values: Any, expected_length: int) -> pd.Series | None:
    ndim = getattr(values, 'ndim', 1)
    if ndim == 0:
        return pd.Series([values.item()] * expected_length)
    if ndim == 1:
        series = pd.Series(values)
    elif ndim == 2:
        if values.shape[0] == 1:
            series = pd.Series(values[0])
        elif values.shape[1] == 1:
            series = pd.Series(values[:, 0])
        else:
            return None
    else:
        return None
    if len(series) != expected_length:
        raise ValueError('KNMI NetCDF variable shape does not match the station dimension.')
    return pd.Series(series.array.to_numpy(dtype=object, na_value=pd.NA)) if hasattr(series, 'array') else series


def _first_present_variable_name(dataset: Any, aliases: tuple[str, ...]) -> str | None:
    lookup = {_normalize_column_name(name): name for name in dataset.variables}
    for alias in aliases:
        resolved = lookup.get(_normalize_column_name(alias))
        if resolved is not None:
            return resolved
    return None


def _resolve_column(columns: dict[str, str], aliases: set[str]) -> str | None:
    for alias in aliases:
        resolved = columns.get(alias)
        if resolved is not None:
            return resolved
    return None


def _normalize_column_name(value: object) -> str:
    return ''.join(character for character in str(value).strip().casefold() if character.isalnum())


def _decode_char(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='ignore')
    return str(value)


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    return float(cleaned)



