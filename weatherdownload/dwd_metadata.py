from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd
import requests

from .dwd_registry import DwdDatasetSpec, list_dataset_specs
from .metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS


def read_station_metadata_dwd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    specs = [_single_source_spec(source_url)] if source_url is not None else list_dataset_specs()
    station_tables = [_load_station_description(spec.metadata_url, timeout=timeout) for spec in specs]
    if not station_tables:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    combined = pd.concat(station_tables, ignore_index=True)
    return _aggregate_station_metadata(combined)


def read_station_observation_metadata_dwd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    specs = [_single_source_spec(source_url)] if source_url is not None else list_dataset_specs()
    rows: list[dict[str, object]] = []
    for spec in specs:
        stations = _load_station_description(spec.metadata_url, timeout=timeout)
        rows.extend(_station_observation_rows_from_spec(stations, spec))
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def _single_source_spec(source_url: str) -> DwdDatasetSpec:
    return DwdDatasetSpec(
        dataset_scope='historical',
        resolution='daily',
        source_id='custom',
        label='Custom DWD source',
        metadata_url=source_url,
        supported_elements=(),
        time_semantics='date',
        implemented=False,
    )


def _load_station_description(source_url: str, timeout: int) -> pd.DataFrame:
    response = requests.get(source_url, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return _parse_station_description_text(response.text)


def _parse_station_description_text(text: str) -> pd.DataFrame:
    lines = [line.rstrip('\n') for line in text.splitlines() if line.strip()]
    if len(lines) < 3:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    header_line = lines[0]
    separator_line = lines[1]
    spans = [(match.start(), match.end()) for match in re.finditer(r'-+', separator_line)]
    column_names = [header_line[start:end].strip() for start, end in spans]

    records: list[dict[str, object]] = []
    for line in lines[2:]:
        row = {
            column_name: line[start:end].strip()
            for column_name, (start, end) in zip(column_names, spans)
        }
        station_id = _normalize_station_id(row.get('Stations_id'))
        if not station_id:
            continue
        records.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': _normalize_dwd_date(row.get('von_datum')),
                'end_date': _normalize_dwd_date(row.get('bis_datum')),
                'full_name': _clean_string(row.get('Stationsname')),
                'longitude': _parse_float(row.get('geoLaenge')),
                'latitude': _parse_float(row.get('geoBreite')),
                'elevation_m': _parse_float(row.get('Stationshoehe')),
            }
        )
    return pd.DataFrame.from_records(records, columns=STATION_METADATA_COLUMNS)


def _aggregate_station_metadata(table: pd.DataFrame) -> pd.DataFrame:
    if table.empty:
        return table.copy()
    grouped_rows: list[dict[str, object]] = []
    for station_id, group in table.groupby('station_id', sort=True):
        grouped_rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': _min_date(group['begin_date']),
                'end_date': _max_date(group['end_date']),
                'full_name': _first_present(group['full_name']),
                'longitude': _first_present(group['longitude']),
                'latitude': _first_present(group['latitude']),
                'elevation_m': _first_present(group['elevation_m']),
            }
        )
    return pd.DataFrame.from_records(grouped_rows, columns=STATION_METADATA_COLUMNS)


def _station_observation_rows_from_spec(stations: pd.DataFrame, spec: DwdDatasetSpec) -> Iterable[dict[str, object]]:
    for station in stations.itertuples(index=False):
        for element in spec.supported_elements:
            yield {
                'obs_type': spec.resolution.upper(),
                'station_id': station.station_id,
                'begin_date': station.begin_date,
                'end_date': station.end_date,
                'element': element,
                'schedule': pd.NA,
                'name': spec.label,
                'description': f'{spec.label} station metadata from DWD CDC',
                'height': pd.NA,
            }


def _normalize_station_id(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    return cleaned.zfill(5)


def _normalize_dwd_date(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    if len(cleaned) != 8 or not cleaned.isdigit():
        return cleaned
    return f'{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]}T00:00Z'


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    return float(cleaned)


def _first_present(series: pd.Series) -> object:
    for value in series.tolist():
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value:
            continue
        return value
    return pd.NA


def _min_date(series: pd.Series) -> str:
    values = [value for value in series.tolist() if isinstance(value, str) and value]
    return min(values) if values else ''


def _max_date(series: pd.Series) -> str:
    values = [value for value in series.tolist() if isinstance(value, str) and value]
    return max(values) if values else ''


