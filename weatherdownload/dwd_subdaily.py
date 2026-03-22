from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from .dwd_registry import DwdDatasetSpec, list_dataset_specs
from .elements import canonicalize_element_series
from .errors import EmptyResultError, StationNotFoundError, UnsupportedQueryError
from .queries import ObservationQuery

NORMALIZED_DWD_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp', 'value', 'flag', 'quality', 'dataset_scope', 'resolution'
]
_DWD_MISSING_SENTINELS = {'-999', '-999.0', '-9999', '-9999.0'}
_BERLIN_TZ = ZoneInfo('Europe/Berlin')
_UTC_TZ = ZoneInfo('UTC')


@dataclass(frozen=True)
class _DwdSubdailySourceConfig:
    source_id: str
    directory_url: str
    archive_pattern: re.Pattern[str]
    product_pattern: re.Pattern[str]
    timestamp_format: str


@dataclass(frozen=True)
class DwdSubdailyDownloadTarget:
    station_id: str
    source_spec: DwdDatasetSpec
    archive_url: str


_SOURCE_CONFIGS: dict[str, _DwdSubdailySourceConfig] = {
    'hourly_air_temperature': _DwdSubdailySourceConfig(
        source_id='hourly_air_temperature',
        directory_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/',
        archive_pattern=re.compile(r'stundenwerte_TU_(?P<station_id>\d{5})_(?P<begin>\d{8})_(?P<end>\d{8})_hist\.zip'),
        product_pattern=re.compile(r'produkt_tu_stunde_\d{8}_\d{8}_(?P<station_id>\d{5})\.txt'),
        timestamp_format='%Y%m%d%H',
    ),
    'hourly_wind': _DwdSubdailySourceConfig(
        source_id='hourly_wind',
        directory_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/',
        archive_pattern=re.compile(r'stundenwerte_FF_(?P<station_id>\d{5})_(?P<begin>\d{8})_(?P<end>\d{8})_hist\.zip'),
        product_pattern=re.compile(r'produkt_ff_stunde_\d{8}_\d{8}_(?P<station_id>\d{5})\.txt'),
        timestamp_format='%Y%m%d%H',
    ),
    'tenmin_air_temperature': _DwdSubdailySourceConfig(
        source_id='tenmin_air_temperature',
        directory_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/',
        archive_pattern=re.compile(r'10minutenwerte_TU_(?P<station_id>\d{5})_(?P<begin>\d{8})_(?P<end>\d{8})_hist\.zip'),
        product_pattern=re.compile(r'produkt_zehn_min_tu_\d{8}_\d{8}_(?P<station_id>\d{5})\.txt'),
        timestamp_format='%Y%m%d%H%M',
    ),
    'tenmin_wind': _DwdSubdailySourceConfig(
        source_id='tenmin_wind',
        directory_url='https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/historical/',
        archive_pattern=re.compile(r'10minutenwerte_wind_(?P<station_id>\d{5})_(?P<begin>\d{8})_(?P<end>\d{8})_hist\.zip'),
        product_pattern=re.compile(r'produkt_zehn_min_ff_\d{8}_\d{8}_(?P<station_id>\d{5})\.txt'),
        timestamp_format='%Y%m%d%H%M',
    ),
}


def download_subdaily_observations_dwd(
    query: ObservationQuery,
    timeout: int = 60,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if query.dataset_scope != 'historical' or query.resolution not in {'1hour', '10min'}:
        raise UnsupportedQueryError('The DWD subdaily downloader only supports historical/1hour and historical/10min.')
    if not query.elements:
        raise UnsupportedQueryError('The DWD subdaily downloader requires at least one element.')
    if query.start is None or query.end is None:
        raise UnsupportedQueryError('The DWD subdaily downloader requires start and end.')

    source_specs = _implemented_source_specs_for_query(query)
    if not source_specs:
        raise UnsupportedQueryError(f"No implemented DWD downloader is available for {query.dataset_scope}/{query.resolution}.")

    targets = _build_subdaily_download_targets(query, source_specs, timeout=timeout)
    if not targets:
        station_list = ', '.join(sorted(query.station_ids))
        raise StationNotFoundError(f'No DWD {query.resolution} historical data found for station_id: {station_list}')

    parsed_tables: list[pd.DataFrame] = []
    found_station_ids: set[str] = set()
    for target in targets:
        archive_bytes = _download_archive_bytes(target.archive_url, timeout=timeout)
        parsed_tables.append(_parse_subdaily_archive(archive_bytes, target))
        found_station_ids.add(target.station_id)

    if not parsed_tables:
        raise EmptyResultError('No observations found for the given query.')

    merged = pd.concat(parsed_tables, ignore_index=True)
    normalized = normalize_subdaily_observations_dwd(merged, query, station_metadata=station_metadata)
    if normalized.empty:
        raise EmptyResultError('No observations found for the given query.')
    return normalized.loc[:, NORMALIZED_DWD_SUBDAILY_COLUMNS]


def _implemented_source_specs_for_query(query: ObservationQuery) -> list[DwdDatasetSpec]:
    raw_elements = set(query.elements or [])
    matches = [
        spec
        for spec in list_dataset_specs()
        if spec.dataset_scope == query.dataset_scope and spec.resolution == query.resolution and spec.implemented
    ]
    return [spec for spec in matches if raw_elements.intersection(spec.supported_elements)]


def _build_subdaily_download_targets(query: ObservationQuery, source_specs: list[DwdDatasetSpec], timeout: int) -> list[DwdSubdailyDownloadTarget]:
    targets: list[DwdSubdailyDownloadTarget] = []
    requested_start = pd.Timestamp(query.start).date()
    requested_end = pd.Timestamp(query.end).date()
    for spec in source_specs:
        config = _SOURCE_CONFIGS[spec.source_id]
        archives = _fetch_archive_urls(config, timeout=timeout)
        for station_id in query.station_ids:
            for begin_date, end_date, archive_url in archives.get(station_id, []):
                if end_date < requested_start or begin_date > requested_end:
                    continue
                targets.append(DwdSubdailyDownloadTarget(station_id=station_id, source_spec=spec, archive_url=archive_url))
    return targets


def _fetch_archive_urls(config: _DwdSubdailySourceConfig, timeout: int) -> dict[str, list[tuple[datetime.date, datetime.date, str]]]:
    response = requests.get(config.directory_url, timeout=timeout)
    response.raise_for_status()
    archives: dict[str, list[tuple[datetime.date, datetime.date, str]]] = {}
    for match in config.archive_pattern.finditer(response.text):
        station_id = match.group('station_id')
        begin_date = datetime.strptime(match.group('begin'), '%Y%m%d').date()
        end_date = datetime.strptime(match.group('end'), '%Y%m%d').date()
        archive_url = config.directory_url + match.group(0)
        archives.setdefault(station_id, []).append((begin_date, end_date, archive_url))
    return archives


def _download_archive_bytes(archive_url: str, timeout: int) -> bytes:
    response = requests.get(archive_url, timeout=timeout)
    response.raise_for_status()
    return response.content


def _parse_subdaily_archive(archive_bytes: bytes, target: DwdSubdailyDownloadTarget) -> pd.DataFrame:
    config = _SOURCE_CONFIGS[target.source_spec.source_id]
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        product_names = [
            name for name in archive.namelist()
            if config.product_pattern.fullmatch(name.split('/')[-1])
        ]
        if not product_names:
            raise ValueError(f'No DWD subdaily product file found in archive for station_id {target.station_id}.')
        product_name = product_names[0]
        csv_text = archive.read(product_name).decode('latin-1')
    table = pd.read_csv(io.StringIO(csv_text), sep=';', dtype=str)
    table.columns = [column.strip() for column in table.columns]
    table['__source_id__'] = target.source_spec.source_id
    return table


def normalize_subdaily_observations_dwd(
    table: pd.DataFrame,
    query: ObservationQuery,
    station_metadata: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    metadata_lookup = _build_gh_id_lookup(station_metadata)
    for source_id, source_table in table.groupby('__source_id__', sort=False):
        source_columns = set(source_table.columns)
        requested_elements = [element for element in (query.elements or []) if element in source_columns]
        if not requested_elements:
            continue
        quality_series = _extract_quality_series(source_table)
        timestamps = _parse_dwd_subdaily_timestamps(source_table['MESS_DATUM'], source_id)
        for element in requested_elements:
            element_columns = canonicalize_element_series(pd.Series([element] * len(source_table.index), index=source_table.index), query)
            normalized = pd.DataFrame(
                {
                    'station_id': source_table['STATIONS_ID'].astype(str).str.strip().str.zfill(5),
                    'element': element_columns['element'],
                    'element_raw': element_columns['element_raw'],
                    'timestamp': timestamps,
                    'value': _to_numeric_with_missing(source_table[element]),
                    'flag': pd.NA,
                    'quality': quality_series,
                    'dataset_scope': query.dataset_scope,
                    'resolution': query.resolution,
                }
            )
            if metadata_lookup is not None:
                normalized = normalized.merge(metadata_lookup, on='station_id', how='left')
            else:
                normalized['gh_id'] = pd.NA
            rows.append(normalized)

    if not rows:
        return pd.DataFrame(columns=NORMALIZED_DWD_SUBDAILY_COLUMNS)

    combined = pd.concat(rows, ignore_index=True)
    combined = combined[(combined['timestamp'] >= pd.Timestamp(query.start)) & (combined['timestamp'] <= pd.Timestamp(query.end))]
    return combined.loc[:, NORMALIZED_DWD_SUBDAILY_COLUMNS].reset_index(drop=True)


def _parse_dwd_subdaily_timestamps(series: pd.Series, source_id: str) -> pd.Series:
    config = _SOURCE_CONFIGS[source_id]
    naive = pd.to_datetime(series.astype(str).str.strip(), format=config.timestamp_format)
    before_2000 = naive.dt.year < 2000
    result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns, UTC]')
    if before_2000.any():
        localized = naive[before_2000].dt.tz_localize(_BERLIN_TZ, ambiguous='infer', nonexistent='shift_forward').dt.tz_convert(_UTC_TZ)
        result.loc[before_2000] = localized
    if (~before_2000).any():
        utc_values = naive[~before_2000].dt.tz_localize(_UTC_TZ)
        result.loc[~before_2000] = utc_values
    return result


def _extract_quality_series(table: pd.DataFrame) -> pd.Series:
    quality_columns = [column for column in table.columns if column.upper().startswith('QN')]
    if not quality_columns:
        return pd.Series(pd.NA, index=table.index, dtype='Int64')
    return _to_quality_with_missing(table[quality_columns[0]])


def _build_gh_id_lookup(station_metadata: pd.DataFrame | None) -> pd.DataFrame | None:
    if station_metadata is None or station_metadata.empty or 'gh_id' not in station_metadata.columns:
        return None
    return station_metadata.loc[:, ['station_id', 'gh_id']].drop_duplicates(subset=['station_id'])


def _to_numeric_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.where(~cleaned.isin(_DWD_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned, errors='coerce')


def _to_quality_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.where(~cleaned.isin(_DWD_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned, errors='coerce').astype('Int64')
