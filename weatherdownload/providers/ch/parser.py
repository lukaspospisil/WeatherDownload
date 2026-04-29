from __future__ import annotations

import csv
import io
import json
import re
from pathlib import Path

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

CH_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]
CH_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'provider', 'resolution',
]

_CH_MISSING_SENTINELS = {'', '-', '--', '-999', '-999.0'}
_CH_HISTORICAL_ASSET_PATTERN = re.compile(r'_(?P<start>\d{4})-(?P<end>\d{4})\.csv$')


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return _decode_meteoswiss_text(local_path.read_bytes())
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    return _decode_meteoswiss_text(response.content)



def parse_ch_station_metadata_csv(csv_text: str) -> pd.DataFrame:
    reader = csv.DictReader(io.StringIO(csv_text.lstrip('\ufeff')), delimiter=';')
    rows: list[dict[str, object]] = []
    for row in reader:
        station_id = normalize_ch_station_id(row.get('station_abbr'))
        if not station_id:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': _clean_string(row.get('station_wigos_id')) or pd.NA,
                'begin_date': normalize_ch_metadata_date(row.get('station_data_since')),
                'end_date': '',
                'full_name': _clean_string(row.get('station_name')) or pd.NA,
                'longitude': _parse_float(row.get('station_coordinates_wgs84_lon')),
                'latitude': _parse_float(row.get('station_coordinates_wgs84_lat')),
                'elevation_m': _parse_float(row.get('station_height_masl')),
            }
        )
    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(['station_id', 'begin_date']).reset_index(drop=True)



def parse_ch_parameter_metadata_csv(csv_text: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(csv_text.lstrip('\ufeff')), sep=';', dtype=str).fillna('')



def parse_ch_data_inventory_csv(csv_text: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(csv_text.lstrip('\ufeff')), sep=';', dtype=str).fillna('')



def normalize_ch_observation_metadata(
    stations: pd.DataFrame,
    specs_and_metadata: list[tuple[object, dict[str, dict[str, str]]]],
    parameter_table: pd.DataFrame | None = None,
    inventory_table: pd.DataFrame | None = None,
) -> pd.DataFrame:
    station_lookup = stations.set_index('station_id') if not stations.empty else pd.DataFrame()
    parameter_lookup = {}
    if parameter_table is not None and not parameter_table.empty:
        parameter_lookup = {
            _clean_string(row.parameter_shortname): {
                'name': _clean_string(getattr(row, 'parameter_description_en', '')),
                'description': _clean_string(getattr(row, 'parameter_description_en', '')),
            }
            for row in parameter_table.itertuples(index=False)
        }

    rows: list[dict[str, object]] = []
    if inventory_table is not None and not inventory_table.empty and not station_lookup.empty:
        for spec, parameter_metadata in specs_and_metadata:
            supported = set(getattr(spec, 'supported_elements', ()))
            inventory_rows = inventory_table[
                inventory_table['station_abbr'].astype('string').str.upper().isin(station_lookup.index)
                & inventory_table['parameter_shortname'].isin(supported)
            ]
            for row in inventory_rows.itertuples(index=False):
                raw_code = _clean_string(row.parameter_shortname)
                metadata = parameter_metadata.get(raw_code, {})
                parameter_info = parameter_lookup.get(raw_code, {})
                rows.append(
                    {
                        'obs_type': metadata.get('obs_type', _obs_type_for_resolution(spec.resolution)),
                        'station_id': normalize_ch_station_id(row.station_abbr),
                        'begin_date': normalize_ch_inventory_datetime(row.data_since),
                        'end_date': normalize_ch_inventory_datetime(row.data_till),
                        'element': raw_code,
                        'schedule': metadata.get('schedule', _schedule_for_resolution(spec.resolution)),
                        'name': metadata.get('name') or parameter_info.get('name') or raw_code,
                        'description': metadata.get('description') or parameter_info.get('description') or pd.NA,
                        'height': pd.NA,
                    }
                )
        if rows:
            return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS).sort_values(
                ['station_id', 'obs_type', 'element', 'begin_date']
            ).reset_index(drop=True)

    for station in stations.itertuples(index=False):
        for spec, parameter_metadata in specs_and_metadata:
            for raw_code in getattr(spec, 'supported_elements', ()):  # pragma: no branch
                metadata = parameter_metadata.get(raw_code, {})
                parameter_info = parameter_lookup.get(raw_code, {})
                rows.append(
                    {
                        'obs_type': metadata.get('obs_type', _obs_type_for_resolution(spec.resolution)),
                        'station_id': station.station_id,
                        'begin_date': station.begin_date,
                        'end_date': station.end_date,
                        'element': raw_code,
                        'schedule': metadata.get('schedule', _schedule_for_resolution(spec.resolution)),
                        'name': metadata.get('name') or parameter_info.get('name') or raw_code,
                        'description': metadata.get('description') or parameter_info.get('description') or pd.NA,
                        'height': pd.NA,
                    }
                )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)



def parse_ch_station_item_json(json_text: str) -> dict[str, str]:
    payload = json.loads(json_text)
    assets = payload.get('assets', {})
    return {
        asset_name: asset_payload['href']
        for asset_name, asset_payload in assets.items()
        if isinstance(asset_payload, dict) and asset_payload.get('href')
    }



def parse_ch_observation_csv(csv_text: str) -> pd.DataFrame:
    table = pd.read_csv(io.StringIO(csv_text.lstrip('\ufeff')), sep=';', dtype=str)
    table.columns = [_clean_string(column) for column in table.columns]
    return table.fillna('')



def normalize_ch_station_id(value: object) -> str:
    return _clean_string(value).upper()



def normalize_ch_metadata_date(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    parsed = pd.to_datetime(cleaned, format='%d.%m.%Y', errors='coerce', utc=True)
    if pd.isna(parsed):
        return ''
    return parsed.strftime('%Y-%m-%dT%H:%MZ')



def normalize_ch_inventory_datetime(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    parsed = pd.to_datetime(cleaned, format='%d.%m.%Y %H:%M', errors='coerce', utc=True)
    if pd.isna(parsed):
        return ''
    return parsed.strftime('%Y-%m-%dT%H:%MZ')



def normalize_ch_observation_date(value: object) -> object:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, format='%d.%m.%Y %H:%M', errors='coerce', utc=True)
    if pd.isna(parsed):
        return None
    return parsed.date()



def normalize_ch_observation_timestamp(value: object) -> pd.Timestamp | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, format='%d.%m.%Y %H:%M', errors='coerce', utc=True)
    if pd.isna(parsed):
        return None
    return parsed



def normalize_ch_query_timestamp(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize('UTC')
    return timestamp.tz_convert('UTC')



def to_numeric_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype('string').str.strip()
    cleaned = cleaned.where(~cleaned.isin(_CH_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned, errors='coerce')



def empty_flag_series(index: pd.Index) -> pd.Series:
    return pd.Series(pd.NA, index=index, dtype='string')



def historical_asset_year_range(asset_name: str) -> tuple[int, int] | None:
    match = _CH_HISTORICAL_ASSET_PATTERN.search(asset_name)
    if match is None:
        return None
    return int(match.group('start')), int(match.group('end'))



def current_year_utc() -> int:
    return pd.Timestamp.now(tz='UTC').year



def _decode_meteoswiss_text(payload: bytes) -> str:
    for encoding in ('utf-8-sig', 'cp1252', 'latin-1'):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode('utf-8', errors='replace')



def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()



def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    return float(cleaned)



def _obs_type_for_resolution(resolution: str) -> str:
    if resolution == '10min':
        return 'HISTORICAL_10MIN'
    if resolution == '1hour':
        return 'HISTORICAL_HOURLY'
    return 'HISTORICAL_DAILY'



def _schedule_for_resolution(resolution: str) -> str:
    if resolution == '10min':
        return 'PT10M MeteoSwiss A1'
    if resolution == '1hour':
        return 'PT1H MeteoSwiss A1'
    return 'P1D MeteoSwiss A1'

