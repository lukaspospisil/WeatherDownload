from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd
import requests

from .registry import DwdDatasetSpec, list_dataset_specs
from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

_DWD_STATION_LINE_PATTERN = re.compile(
    r'^\s*(?P<station_id>\d+)\s+'
    r'(?P<begin_date>\d{8})\s+'
    r'(?P<end_date>\d{8})\s+'
    r'(?P<elevation>-?\d+(?:\.\d+)?)\s+'
    r'(?P<latitude>-?\d+(?:\.\d+)?)\s+'
    r'(?P<longitude>-?\d+(?:\.\d+)?)\s+'
    r'(?P<name_block>.+?)\s*$'
)


def read_station_metadata_dwd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    specs = [_single_source_spec(source_url)] if source_url is not None else _list_national_dataset_specs()
    station_tables = [_load_station_description(spec.metadata_url, timeout=timeout) for spec in specs]
    if not station_tables:
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    combined = pd.concat(station_tables, ignore_index=True)
    return _aggregate_station_metadata(combined)


def read_station_observation_metadata_dwd(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    specs = [_single_source_spec(source_url)] if source_url is not None else _list_national_dataset_specs()
    rows: list[dict[str, object]] = []
    for spec in specs:
        stations = _load_station_description(spec.metadata_url, timeout=timeout)
        rows.extend(_station_observation_rows_from_spec(stations, spec))
    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def _single_source_spec(source_url: str) -> DwdDatasetSpec:
    return DwdDatasetSpec(
        provider='historical',
        resolution='daily',
        source_id='custom',
        label='Custom DWD source',
        metadata_url=source_url,
        supported_elements=(),
        time_semantics='date',
        implemented=False,
    )


def _list_national_dataset_specs() -> list[DwdDatasetSpec]:
    return [spec for spec in list_dataset_specs() if isinstance(spec, DwdDatasetSpec)]


def _load_station_description(source_url: str, timeout: int) -> pd.DataFrame:
    response = requests.get(source_url, timeout=timeout)
    response.raise_for_status()
    text = _decode_dwd_text_response(response)
    return _parse_station_description_text(text)


def _decode_dwd_text_response(response: requests.Response) -> str:
    content = getattr(response, 'content', None)
    if content is not None:
        return content.decode('latin-1')
    text = getattr(response, 'text', '')
    return text.encode('latin-1', errors='replace').decode('latin-1')


def _parse_station_description_text(text: str) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for line in text.splitlines():
        match = _DWD_STATION_LINE_PATTERN.match(line)
        if not match:
            continue
        records.append(
            {
                'station_id': _normalize_station_id(match.group('station_id')),
                'gh_id': pd.NA,
                'begin_date': _normalize_dwd_date(match.group('begin_date')),
                'end_date': _normalize_dwd_date(match.group('end_date')),
                'full_name': _normalize_name_block(match.group('name_block')),
                'longitude': _parse_float(match.group('longitude')),
                'latitude': _parse_float(match.group('latitude')),
                'elevation_m': _parse_float(match.group('elevation')),
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


def _normalize_name_block(value: object) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return ''
    return re.sub(r'\s{2,}', ' | ', cleaned)


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

