from __future__ import annotations

import json
from calendar import monthrange
from datetime import date

import pandas as pd

from ...elements import canonicalize_element_series
from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS


GHCND_NORMALIZED_DAILY_COLUMNS = [
    'station_id',
    'gh_id',
    'element',
    'element_raw',
    'observation_date',
    'time_function',
    'value',
    'flag',
    'quality',
    'dataset_scope',
    'resolution',
]

GHCND_DAILY_SCALE_FACTORS = {
    'EVAP': 10.0,
    'PRCP': 10.0,
    'TAVG': 10.0,
    'TMAX': 10.0,
    'TMIN': 10.0,
}

GHCND_STATION_ELEMENTS_ATTR = 'station_provider_raw_elements_by_path'


def parse_ghcnd_stations_text(stations_text: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for line in stations_text.splitlines():
        if not line.strip():
            continue
        station_id = line[0:11].strip()
        if not station_id:
            continue
        rows.append(
            {
                'station_id': station_id,
                'country': station_id[:2],
                'latitude': _parse_float(line[12:20]),
                'longitude': _parse_float(line[21:30]),
                'elevation_m': _parse_elevation(line[31:37]),
                'state': _parse_string(line[38:40]),
                'full_name': _parse_string(line[41:71]),
                'gsn_flag': _parse_string(line[72:75]),
                'hcn_crn_flag': _parse_string(line[76:79]),
                'wmo_id': _parse_string(line[80:85]),
            }
        )
    return pd.DataFrame.from_records(
        rows,
        columns=['station_id', 'country', 'latitude', 'longitude', 'elevation_m', 'state', 'full_name', 'gsn_flag', 'hcn_crn_flag', 'wmo_id'],
    )


def parse_ghcnd_inventory_text(inventory_text: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for line in inventory_text.splitlines():
        if not line.strip():
            continue
        station_id = line[0:11].strip()
        if not station_id:
            continue
        rows.append(
            {
                'station_id': station_id,
                'latitude': _parse_float(line[12:20]),
                'longitude': _parse_float(line[21:30]),
                'element_raw': _parse_string(line[31:35]),
                'begin_year': _parse_int(line[36:40]),
                'end_year': _parse_int(line[41:45]),
            }
        )
    return pd.DataFrame.from_records(
        rows,
        columns=['station_id', 'latitude', 'longitude', 'element_raw', 'begin_year', 'end_year'],
    )


def normalize_ghcnd_station_metadata(
    stations_table: pd.DataFrame,
    inventory_table: pd.DataFrame,
    *,
    country_prefix: str = 'US',
    supported_elements: tuple[str, ...] = ('EVAP',),
) -> pd.DataFrame:
    if stations_table.empty or inventory_table.empty:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)

    station_elements = build_station_supported_raw_elements(
        inventory_table,
        country_prefix=country_prefix,
        supported_elements=supported_elements,
    )
    if not station_elements:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)

    supported = tuple(str(element).strip() for element in supported_elements if str(element).strip())
    inventory = inventory_table[
        inventory_table['station_id'].str.startswith(country_prefix)
        & inventory_table['element_raw'].isin(supported)
        & inventory_table['station_id'].isin(station_elements)
    ].copy()
    ranges = (
        inventory.groupby('station_id', as_index=False)
        .agg(begin_year=('begin_year', 'min'), end_year=('end_year', 'max'))
        .sort_values('station_id', ignore_index=True)
    )
    merged = ranges.merge(stations_table, on='station_id', how='left')
    normalized_rows: list[dict[str, object]] = []
    for row in merged.itertuples(index=False):
        normalized_rows.append(
            {
                'station_id': row.station_id,
                'gh_id': pd.NA,
                'begin_date': f'{int(row.begin_year):04d}-01-01T00:00Z' if pd.notna(row.begin_year) else pd.NA,
                'end_date': f'{int(row.end_year):04d}-12-31T00:00Z' if pd.notna(row.end_year) else pd.NA,
                'full_name': row.full_name if pd.notna(row.full_name) else pd.NA,
                'longitude': row.longitude if pd.notna(row.longitude) else pd.NA,
                'latitude': row.latitude if pd.notna(row.latitude) else pd.NA,
                'elevation_m': row.elevation_m if pd.notna(row.elevation_m) else pd.NA,
            }
        )
    result = pd.DataFrame.from_records(normalized_rows, columns=STATION_METADATA_COLUMNS)
    result.attrs[GHCND_STATION_ELEMENTS_ATTR] = {
        ('ghcnd', 'daily'): station_elements,
    }
    return result


def normalize_ghcnd_observation_metadata(
    inventory_table: pd.DataFrame,
    *,
    country_prefix: str = 'US',
    supported_elements: tuple[str, ...] = ('EVAP',),
) -> pd.DataFrame:
    station_elements = build_station_supported_raw_elements(
        inventory_table,
        country_prefix=country_prefix,
        supported_elements=supported_elements,
    )
    if not station_elements:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)

    supported = tuple(str(element).strip() for element in supported_elements if str(element).strip())
    inventory = inventory_table[
        inventory_table['station_id'].str.startswith(country_prefix)
        & inventory_table['element_raw'].isin(supported)
        & inventory_table['station_id'].isin(station_elements)
    ].copy()
    rows: list[dict[str, object]] = []
    grouped = (
        inventory.groupby(['station_id', 'element_raw'], as_index=False)
        .agg(begin_year=('begin_year', 'min'), end_year=('end_year', 'max'))
        .sort_values(['station_id', 'element_raw'], ignore_index=True)
    )
    for row in grouped.itertuples(index=False):
        rows.append(
            {
                'obs_type': 'GHCND_DAILY',
                'station_id': row.station_id,
                'begin_date': f'{int(row.begin_year):04d}-01-01T00:00Z' if pd.notna(row.begin_year) else pd.NA,
                'end_date': f'{int(row.end_year):04d}-12-31T00:00Z' if pd.notna(row.end_year) else pd.NA,
                'element': row.element_raw,
                'schedule': 'P1D .dly monthly records',
                'name': row.element_raw,
                'description': _observation_description(row.element_raw),
                'height': pd.NA,
            }
        )
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def build_station_supported_raw_elements(
    inventory_table: pd.DataFrame,
    *,
    country_prefix: str = 'US',
    supported_elements: tuple[str, ...] = ('EVAP',),
) -> dict[str, list[str]]:
    supported = tuple(str(element).strip() for element in supported_elements if str(element).strip())
    if inventory_table.empty or not supported:
        return {}
    inventory = inventory_table[
        inventory_table['station_id'].str.startswith(country_prefix)
        & inventory_table['element_raw'].isin(supported)
    ].copy()
    if inventory.empty:
        return {}
    station_elements: dict[str, list[str]] = {}
    for station_id, group in inventory.groupby('station_id', sort=True):
        available = {str(value).strip() for value in group['element_raw'].dropna().tolist()}
        ordered = [element for element in supported if element in available]
        if ordered:
            station_elements[str(station_id)] = ordered
    return station_elements


def parse_ghcnd_dly_text(
    dly_text: str,
    *,
    supported_elements: tuple[str, ...] = ('TAVG', 'TMAX', 'TMIN', 'PRCP', 'SNWD', 'EVAP'),
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    supported = set(supported_elements)
    for line in dly_text.splitlines():
        if not line.strip():
            continue
        station_id = line[0:11].strip()
        year = int(line[11:15])
        month = int(line[15:17])
        element_raw = line[17:21].strip()
        if element_raw not in supported:
            continue
        days_in_month = monthrange(year, month)[1]
        for day in range(1, 32):
            if day > days_in_month:
                continue
            offset = 21 + (day - 1) * 8
            raw_value = int(line[offset:offset + 5])
            mflag = _parse_string(line[offset + 5:offset + 6])
            qflag = _parse_string(line[offset + 6:offset + 7])
            sflag = _parse_string(line[offset + 7:offset + 8])
            observation_date = date(year, month, day)
            rows.append(
                {
                    'station_id': station_id,
                    'year': year,
                    'month': month,
                    'day': day,
                    'days_in_month': days_in_month,
                    'element_raw': element_raw,
                    'observation_date': observation_date,
                    'value_raw': raw_value,
                    'mflag': mflag,
                    'qflag': qflag,
                    'sflag': sflag,
                }
            )
    return pd.DataFrame.from_records(
        rows,
        columns=['station_id', 'year', 'month', 'day', 'days_in_month', 'element_raw', 'observation_date', 'value_raw', 'mflag', 'qflag', 'sflag'],
    )


def normalize_daily_observations_ghcnd(
    raw_table: pd.DataFrame,
    *,
    query,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if raw_table.empty:
        return pd.DataFrame(columns=GHCND_NORMALIZED_DAILY_COLUMNS)

    normalized = raw_table.copy()
    normalized = normalized[normalized['station_id'].isin(query.station_ids)]
    if query.elements:
        normalized = normalized[normalized['element_raw'].isin(query.elements)]
    if query.start_date is not None and query.end_date is not None:
        normalized = normalized[normalized['observation_date'].between(query.start_date, query.end_date)]
    normalized = normalized[normalized['day'] <= normalized['days_in_month']].copy()
    if normalized.empty:
        return pd.DataFrame(columns=GHCND_NORMALIZED_DAILY_COLUMNS)

    normalized['value'] = normalized['value_raw'].replace(-9999, pd.NA).astype('Float64')
    normalized['value'] = normalized.apply(_scale_daily_value, axis=1).astype('Float64')
    normalized = normalized[normalized['value'].notna()].copy()
    if normalized.empty:
        return pd.DataFrame(columns=GHCND_NORMALIZED_DAILY_COLUMNS)

    element_frame = canonicalize_element_series(normalized['element_raw'], query)
    normalized['element'] = element_frame['element']
    normalized['element_raw'] = element_frame['element_raw']
    normalized['gh_id'] = pd.NA
    normalized['time_function'] = pd.NA
    normalized['flag'] = normalized.apply(_compose_flag, axis=1)
    normalized['quality'] = normalized['qflag'].astype('object')
    normalized.loc[normalized['quality'].isna(), 'quality'] = pd.NA
    normalized['dataset_scope'] = query.dataset_scope
    normalized['resolution'] = query.resolution

    if station_metadata is not None and not station_metadata.empty and 'gh_id' in station_metadata.columns:
        gh_lookup = station_metadata.set_index('station_id')['gh_id']
        normalized['gh_id'] = normalized['station_id'].map(gh_lookup).astype('object')

    return normalized.loc[:, GHCND_NORMALIZED_DAILY_COLUMNS].reset_index(drop=True)


def _compose_flag(row: pd.Series) -> object:
    payload = {}
    if pd.notna(row.get('mflag')) and str(row.get('mflag')).strip():
        payload['measurement_flag'] = str(row.get('mflag')).strip()
    if pd.notna(row.get('sflag')) and str(row.get('sflag')).strip():
        payload['source_flag'] = str(row.get('sflag')).strip()
    if not payload:
        return pd.NA
    return json.dumps(payload, sort_keys=True)


def _scale_daily_value(row: pd.Series) -> object:
    value = row.get('value')
    element_raw = str(row.get('element_raw') or '').strip()
    if pd.isna(value):
        return pd.NA
    scale = GHCND_DAILY_SCALE_FACTORS.get(element_raw)
    if scale is None:
        return value
    return float(value) / scale


def _observation_description(element_raw: str) -> str:
    descriptions = {
        'EVAP': 'Evaporation of water from evaporation pan [raw unit: tenths of mm; WeatherDownload output: mm]',
        'PRCP': 'Daily precipitation total [raw unit: tenths of mm; WeatherDownload output: mm]',
        'SNWD': 'Daily snow depth [raw unit: mm; WeatherDownload output: mm]',
        'TAVG': 'Daily average temperature [raw unit: tenths of degrees C; WeatherDownload output: degrees C]',
        'TMAX': 'Daily maximum temperature [raw unit: tenths of degrees C; WeatherDownload output: degrees C]',
        'TMIN': 'Daily minimum temperature [raw unit: tenths of degrees C; WeatherDownload output: degrees C]',
    }
    return descriptions.get(element_raw, f'{element_raw} [raw NOAA GHCN-Daily units]')


def _parse_float(value: str) -> float | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    return float(cleaned)


def _parse_int(value: str) -> int | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    return int(cleaned)


def _parse_string(value: str) -> str | None:
    cleaned = value.strip()
    return cleaned or None


def _parse_elevation(value: str) -> float | None:
    parsed = _parse_float(value)
    if parsed is None or parsed <= -999.0:
        return None
    return parsed
