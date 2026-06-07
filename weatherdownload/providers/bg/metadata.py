from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import (
    build_source_url,
    parse_bg_month_links,
    parse_bg_station_table,
    read_text_from_source,
    related_open_data_source,
)
from .registry import BG_NIMH_PARAMETER_METADATA, get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='BG', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='BG',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_nimh(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('nimh', 'daily')
    rain_source = source_url or spec.rain_page_url
    snow_source = related_open_data_source(rain_source, target='snow') or spec.snow_page_url

    rain_links = parse_bg_month_links(read_text_from_source(rain_source, timeout), slug='prec')
    snow_links = parse_bg_month_links(read_text_from_source(snow_source, timeout), slug='snow')
    if not rain_links and not snow_links:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)

    rain_table = _read_latest_station_table(rain_source, rain_links, timeout)
    snow_table = _read_latest_station_table(snow_source, snow_links, timeout)
    combined = _normalize_bg_station_metadata(rain_table, snow_table, rain_links, snow_links)
    combined.attrs['station_provider_raw_elements_by_path'] = _build_station_attrs(rain_table, snow_table)
    return combined


def read_station_observation_metadata_nimh(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    stations = read_station_metadata_nimh(source_url=source_url, timeout=timeout)
    if stations.empty:
        return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)

    station_attrs = stations.attrs.get('station_provider_raw_elements_by_path', {})
    raw_elements_by_station = station_attrs.get(('nimh', 'daily'), {})
    records: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for raw_code in raw_elements_by_station.get(str(station.station_id), []):
            parameter = BG_NIMH_PARAMETER_METADATA[raw_code]
            records.append(
                {
                    'obs_type': parameter['obs_type'],
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': parameter['schedule'],
                    'name': parameter['name'],
                    'description': parameter['description'],
                    'height': pd.NA,
                }
            )
    return pd.DataFrame.from_records(records, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def _read_latest_station_table(
    page_source: str,
    links: dict[tuple[int, int], str],
    timeout: int,
) -> pd.DataFrame:
    if not links:
        return pd.DataFrame(columns=['station_id', 'full_name'])
    year, month = max(links)
    csv_source = build_source_url(page_source, links[(year, month)])
    return parse_bg_station_table(read_text_from_source(csv_source, timeout))


def _normalize_bg_station_metadata(
    rain_table: pd.DataFrame,
    snow_table: pd.DataFrame,
    rain_links: dict[tuple[int, int], str],
    snow_links: dict[tuple[int, int], str],
) -> pd.DataFrame:
    combined = pd.concat([rain_table, snow_table], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)

    combined = combined.drop_duplicates(subset=['station_id'], keep='first').copy()
    begin_year, begin_month = min([*rain_links, *snow_links])
    end_year, end_month = max([*rain_links, *snow_links])
    combined['gh_id'] = pd.NA
    combined['begin_date'] = _iso_utc(datetime(begin_year, begin_month, 1, tzinfo=timezone.utc))
    combined['end_date'] = _iso_utc(_month_end_utc(end_year, end_month))
    combined['longitude'] = pd.NA
    combined['latitude'] = pd.NA
    combined['elevation_m'] = pd.NA
    combined = combined.rename(columns={'full_name': 'full_name'})
    return combined.loc[:, STATION_METADATA_COLUMNS].sort_values('station_id', kind='stable').reset_index(drop=True)


def _build_station_attrs(rain_table: pd.DataFrame, snow_table: pd.DataFrame) -> dict[tuple[str, str], dict[str, list[str]]]:
    raw_elements_by_station: dict[str, list[str]] = {}
    for station_id in rain_table.get('station_id', pd.Series(dtype='object')).astype(str):
        raw_elements_by_station.setdefault(station_id, [])
        if 'precipitation' not in raw_elements_by_station[station_id]:
            raw_elements_by_station[station_id].append('precipitation')
    for station_id in snow_table.get('station_id', pd.Series(dtype='object')).astype(str):
        raw_elements_by_station.setdefault(station_id, [])
        if 'snow_cover_depth' not in raw_elements_by_station[station_id]:
            raw_elements_by_station[station_id].append('snow_cover_depth')
    return {('nimh', 'daily'): raw_elements_by_station}


def _month_end_utc(year: int, month: int) -> datetime:
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return next_month.replace(hour=0, minute=0) - pd.Timedelta(minutes=1)


def _iso_utc(value: datetime) -> str:
    return value.isoformat(timespec='minutes').replace('+00:00', 'Z')
