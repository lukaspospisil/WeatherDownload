from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    export_table,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.elements import raw_to_canonical_map_for_spec
from weatherdownload.providers import get_provider, normalize_country_code

FINAL_SERIES_COLUMNS = [
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'vapour_pressure',
    'sunshine_duration',
]
FAO_CANONICAL_ELEMENTS = tuple(FINAL_SERIES_COLUMNS)
CZ_TIMEFUNC_BY_CANONICAL = {
    'tas_mean': 'AVG',
    'wind_speed': 'AVG',
    'vapour_pressure': 'AVG',
    'tas_max': '20:00',
    'tas_min': '20:00',
    'sunshine_duration': '00:00',
}


@dataclass(frozen=True)
class FaoCountryConfig:
    country: str
    dataset_scope: str
    resolution: str
    obs_types: tuple[str, ...]
    canonical_to_raw: dict[str, tuple[str, ...]]
    raw_to_canonical: dict[str, str]
    time_function_by_canonical: dict[str, str]
    dataset_type: str
    source: str

    @property
    def canonical_elements(self) -> list[str]:
        return list(self.canonical_to_raw.keys())


class CacheMissingError(FileNotFoundError):
    pass


class ProgressReporter:
    def __init__(self, *, silent: bool) -> None:
        self.silent = silent

    def info(self, message: str) -> None:
        if not self.silent:
            print(message)

    def essential(self, message: str) -> None:
        print(message)


class CacheStats:
    def __init__(self) -> None:
        self.downloaded = 0
        self.reused = 0
        self.missing = 0
        self.failed = 0
        self.candidate_stations = 0
        self.cache_ready_stations = 0
        self.metadata_downloaded = 0
        self.metadata_reused = 0

    def add_file_status(self, status: str) -> None:
        if status == 'downloaded':
            self.downloaded += 1
        elif status == 'reused':
            self.reused += 1
        elif status == 'missing':
            self.missing += 1
        elif status == 'failed':
            self.failed += 1

    def add_metadata_status(self, status: str) -> None:
        if status == 'downloaded':
            self.metadata_downloaded += 1
        elif status == 'reused':
            self.metadata_reused += 1
        self.add_file_status(status)


class StationCacheResult:
    def __init__(self, station_id: str) -> None:
        self.station_id = station_id
        self.downloaded = 0
        self.reused = 0
        self.missing = 0
        self.failed = 0

    @property
    def available(self) -> bool:
        return self.missing == 0 and self.failed == 0

    def add_status(self, status: str) -> None:
        if status == 'downloaded':
            self.downloaded += 1
        elif status == 'reused':
            self.reused += 1
        elif status == 'missing':
            self.missing += 1
        elif status == 'failed':
            self.failed += 1

    def summary(self) -> str:
        parts: list[str] = []
        if self.reused:
            parts.append(f'reused {self.reused}')
        if self.downloaded:
            parts.append(f'downloaded {self.downloaded}')
        if self.missing:
            parts.append(f'missing {self.missing}')
        if self.failed:
            parts.append(f'failed {self.failed}')
        if not parts:
            parts.append('no files')
        return ', '.join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        reporter = ProgressReporter(silent=args.silent)
        stats = CacheStats()

        # Pick the country-specific FAO-prep mapping and cache location first.
        config = get_fao_country_config(args.country)
        country_cache_dir = resolve_country_cache_dir(args.cache_dir, config.country)

        reporter.info(f'Using cache directory: {country_cache_dir}')
        meta1 = load_station_metadata_with_cache(
            country_cache_dir,
            country=config.country,
            mode=args.mode,
            timeout=args.timeout,
            reporter=reporter,
            stats=stats,
        )
        meta2 = load_station_observation_metadata_with_cache(
            country_cache_dir,
            country=config.country,
            mode=args.mode,
            timeout=args.timeout,
            reporter=reporter,
            stats=stats,
        )

        if args.station_ids:
            requested = {station_id.strip().upper() for station_id in args.station_ids}
            meta1 = meta1[meta1['station_id'].str.upper().isin(requested)].reset_index(drop=True)
            meta2 = meta2[meta2['station_id'].str.upper().isin(requested)].reset_index(drop=True)

        # Screen stations using observation metadata before touching daily observations.
        candidates = screen_candidate_stations(meta1, meta2, config=config, min_complete_days=args.min_complete_days)
        stats.candidate_stations = len(candidates)
        reporter.essential(f'Meta2-screened candidate stations: {len(candidates)}')

        available_station_count, missing_station_ids, failed_station_ids = cache_candidate_daily_inputs(
            candidates,
            cache_dir=country_cache_dir,
            config=config,
            mode=args.mode,
            timeout=args.timeout,
            reporter=reporter,
            stats=stats,
        )
        stats.cache_ready_stations = available_station_count

        if args.mode == 'download':
            reporter.essential(f'Cached FAO inputs for {available_station_count} station(s) under {country_cache_dir}.')
            print_final_summary(reporter, stats)
            if missing_station_ids:
                reporter.essential(f'Stations with missing required daily inputs: {", ".join(missing_station_ids)}')
            if failed_station_ids:
                reporter.essential(f'Stations with failed downloads: {", ".join(failed_station_ids)}')
            return 0

        if args.mode == 'build' and (missing_station_ids or failed_station_ids):
            print_final_summary(reporter, stats)
            problem_ids = missing_station_ids + failed_station_ids
            raise CacheMissingError(
                'Build mode requires a complete local cache. Missing or failed stations: '
                + ', '.join(problem_ids)
            )

        unavailable_station_ids = set(missing_station_ids) | set(failed_station_ids)
        retained_series: list[dict[str, Any]] = []
        station_rows: list[dict[str, Any]] = []

        # Build the cleaned daily dataset from cached normalized observations only.
        build_candidates = candidates[~candidates['station_id'].isin(unavailable_station_ids)].reset_index(drop=True)
        total_build_candidates = len(build_candidates)
        reporter.info(f'Building cleaned dataset from cache for {total_build_candidates} station(s).')

        for index, station in enumerate(build_candidates.itertuples(index=False), start=1):
            reporter.info(f'[{index}/{total_build_candidates}] Processing {station.station_id} ({station.full_name}) from cache')
            daily_table = read_cached_daily_observations(station.station_id, cache_dir=country_cache_dir)

            complete = prepare_complete_station_series(daily_table, config=config)
            if complete.empty:
                reporter.info('  No complete FAO-prep days after country-specific selection, skipping.')
                continue
            if len(complete) < args.min_complete_days:
                reporter.info(f'  Only {len(complete)} complete days, below threshold {args.min_complete_days}.')
                continue

            # Keep a station-level summary row and a per-station canonical time series.
            retained_series.append(
                build_series_record(
                    complete,
                    station_id=station.station_id,
                    full_name=station.full_name,
                    latitude=station.latitude,
                    longitude=station.longitude,
                    elevation=station.elevation_m,
                )
            )
            station_rows.append(
                {
                    'WSI': station.station_id,
                    'FULL_NAME': station.full_name,
                    'Latitude': station.latitude,
                    'Longitude': station.longitude,
                    'Elevation': station.elevation_m,
                    'NumCompleteDays_E': int(len(complete)),
                    'FirstCompleteDate_E': complete['Date'].min().isoformat(),
                    'LastCompleteDate_E': complete['Date'].max().isoformat(),
                }
            )

        data_info = {
            'CreatedAt': pd.Timestamp.utcnow().isoformat(),
            'DatasetType': config.dataset_type,
            'Source': config.source,
            'Country': config.country,
            'Elements': FINAL_SERIES_COLUMNS,
            'ProviderElementMapping': build_provider_element_mapping(config),
            'MinCompleteDays': int(args.min_complete_days),
            'NumStations': int(len(retained_series)),
        }
        exported_targets: list[str] = []
        # Export one or both bundle formats without mixing download logic into the exporters.
        if args.export_format in {'mat', 'both'}:
            export_mat_bundle(args.output, data_info=data_info, stations=station_rows, series=retained_series)
            exported_targets.append(str(args.output))
        if args.export_format in {'parquet', 'both'}:
            export_parquet_bundle(args.output_dir, data_info=data_info, stations=station_rows, series=retained_series)
            exported_targets.append(str(args.output_dir))
        reporter.essential(f"Exported FAO-prep output to: {', '.join(exported_targets)}")
        print_final_summary(reporter, stats)
        return 0
    except Exception as exc:
        import sys

        print(f'Error: {exc}', file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Prepare a country-aware daily dataset for later FAO processing.')
    parser.add_argument(
        '--country',
        default='CZ',
        help='ISO 3166-1 alpha-2 country code. Defaults to CZ.',
    )
    parser.add_argument(
        '--mode',
        choices=['full', 'download', 'build'],
        default='full',
        help='Run the full pipeline, cache inputs only, or build only from cached inputs.',
    )
    parser.add_argument(
        '--cache-dir',
        type=Path,
        default=Path('outputs/fao_cache'),
        help='Base cache directory for metadata and daily inputs.',
    )
    parser.add_argument('--output', type=Path, default=Path('outputs/fao_daily.mat'), help='MATLAB .mat output path.')
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('outputs/fao_daily_bundle'),
        help='Portable Parquet bundle output directory.',
    )
    parser.add_argument(
        '--export-format',
        choices=['mat', 'parquet', 'both'],
        default='mat',
        help='Output format to produce in full or build mode.',
    )
    parser.add_argument('--station-id', action='append', dest='station_ids', help='Optional canonical station_id filter. Can be provided multiple times.')
    parser.add_argument('--min-complete-days', type=int, default=3650, help='Minimum number of complete E-based days required per station.')
    parser.add_argument('--timeout', type=int, default=60, help='HTTP timeout in seconds.')
    parser.add_argument('--silent', action='store_true', help='Suppress non-essential progress output.')
    return parser


def get_fao_country_config(country: str | None) -> FaoCountryConfig:
    normalized_country = normalize_country_code(country)
    try:
        # Reuse the provider registry so the example stays aligned with supported daily mappings.
        provider = get_provider(normalized_country)
        daily_spec = provider.get_dataset_spec(
            'historical_csv' if normalized_country == 'CZ' else 'historical',
            'daily',
        )
    except Exception as exc:
        raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.') from exc

    canonical_to_raw = daily_spec.canonical_elements or {}
    raw_to_provider_canonical = raw_to_canonical_map_for_spec(daily_spec)
    selected_canonical_to_raw: dict[str, tuple[str, ...]] = {}
    raw_to_canonical: dict[str, str] = {}
    for canonical_name in FAO_CANONICAL_ELEMENTS:
        raw_codes = canonical_to_raw.get(canonical_name, ())
        if not raw_codes:
            raise ValueError(
                f'FAO preparation example requires daily canonical element {canonical_name!r} for country {normalized_country}.'
            )
        selected_canonical_to_raw[canonical_name] = tuple(raw_codes)
    for raw_code, canonical_name in raw_to_provider_canonical.items():
        if canonical_name in selected_canonical_to_raw:
            raw_to_canonical[raw_code.upper()] = canonical_name

    if normalized_country == 'CZ':
        return FaoCountryConfig(
            country='CZ',
            dataset_scope='historical_csv',
            resolution='daily',
            obs_types=('DLY',),
            canonical_to_raw=selected_canonical_to_raw,
            raw_to_canonical=raw_to_canonical,
            time_function_by_canonical=dict(CZ_TIMEFUNC_BY_CANONICAL),
            dataset_type='CHMI daily meteorological dataset prepared for later FAO Penman-Monteith processing',
            source='CHMI OpenData historical_csv metadata and daily observations',
        )
    if normalized_country == 'DE':
        return FaoCountryConfig(
            country='DE',
            dataset_scope='historical',
            resolution='daily',
            obs_types=('DAILY',),
            canonical_to_raw=selected_canonical_to_raw,
            raw_to_canonical=raw_to_canonical,
            time_function_by_canonical={},
            dataset_type='DWD daily meteorological dataset prepared for later FAO Penman-Monteith processing',
            source='DWD CDC historical daily metadata and observations',
        )
    raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.')


def resolve_country_cache_dir(cache_dir: Path, country: str) -> Path:
    return cache_dir / normalize_country_code(country)


def load_station_metadata_with_cache(
    cache_dir: Path,
    *,
    country: str,
    mode: str,
    timeout: int,
    reporter: ProgressReporter,
    stats: CacheStats,
) -> pd.DataFrame:
    table, status = load_cached_dataframe(
        cache_path=cache_dir / 'meta1.csv',
        mode=mode,
        loader=lambda: read_station_metadata(country=country, timeout=timeout),
        description='meta1.csv',
    )
    stats.add_metadata_status(status)
    reporter.info(f'meta1.csv: {format_file_status(status)}')
    return table


def load_station_observation_metadata_with_cache(
    cache_dir: Path,
    *,
    country: str,
    mode: str,
    timeout: int,
    reporter: ProgressReporter,
    stats: CacheStats,
) -> pd.DataFrame:
    table, status = load_cached_dataframe(
        cache_path=cache_dir / 'meta2.csv',
        mode=mode,
        loader=lambda: read_station_observation_metadata(country=country, timeout=timeout),
        description='meta2.csv',
    )
    stats.add_metadata_status(status)
    reporter.info(f'meta2.csv: {format_file_status(status)}')
    return table


def load_cached_dataframe(
    *,
    cache_path: Path,
    mode: str,
    loader,
    description: str,
) -> tuple[pd.DataFrame, str]:
    if cache_path.exists():
        return pd.read_csv(cache_path), 'reused'
    if mode == 'build':
        raise CacheMissingError(f'Missing cached {description}: {cache_path}')
    table = loader()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(cache_path, index=False)
    return table, 'downloaded'


def screen_candidate_stations(
    meta1: pd.DataFrame,
    meta2: pd.DataFrame,
    *,
    config: FaoCountryConfig,
    min_complete_days: int,
) -> pd.DataFrame:
    # Keep only the daily observation metadata rows relevant for the FAO-prep variables.
    relevant = meta2[
        meta2['obs_type'].astype(str).str.upper().isin(config.obs_types)
        & meta2['element'].astype(str).str.upper().isin(config.raw_to_canonical)
    ].copy()
    relevant['canonical_element'] = relevant['element'].astype(str).str.upper().map(config.raw_to_canonical)
    supported = (
        relevant.groupby('station_id')['canonical_element']
        .agg(lambda values: set(values))
        .reset_index(name='canonical_elements')
    )
    supported['has_required_elements'] = supported['canonical_elements'].apply(lambda values: set(FINAL_SERIES_COLUMNS).issubset(values))

    overlap = estimate_station_overlap_days(relevant)
    screened = supported.merge(overlap, on='station_id', how='left')
    screened['overlap_days_estimate'] = screened['overlap_days_estimate'].fillna(0).astype(int)
    candidate_ids = screened[
        screened['has_required_elements'] & (screened['overlap_days_estimate'] >= min_complete_days)
    ]['station_id']
    candidate_rows = meta1[meta1['station_id'].isin(candidate_ids)].copy()
    return deduplicate_candidate_stations(candidate_rows)


def estimate_station_overlap_days(meta2: pd.DataFrame) -> pd.DataFrame:
    if meta2.empty:
        return pd.DataFrame(columns=['station_id', 'overlap_days_estimate'])
    dated = meta2.copy()
    dated['begin_ts'] = pd.to_datetime(dated['begin_date'], utc=True)
    dated['end_ts'] = pd.to_datetime(dated['end_date'], utc=True)
    element_spans = (
        dated.groupby(['station_id', 'canonical_element'])
        .agg(begin_ts=('begin_ts', 'min'), end_ts=('end_ts', 'max'))
        .reset_index()
    )
    station_spans = (
        element_spans.groupby('station_id')
        .agg(overlap_begin=('begin_ts', 'max'), overlap_end=('end_ts', 'min'))
        .reset_index()
    )
    station_spans['overlap_days_estimate'] = (
        (station_spans['overlap_end'] - station_spans['overlap_begin']).dt.days + 1
    ).clip(lower=0)
    return station_spans[['station_id', 'overlap_days_estimate']]


def deduplicate_candidate_stations(meta1_rows: pd.DataFrame) -> pd.DataFrame:
    if meta1_rows.empty:
        return meta1_rows.reset_index(drop=True)
    return meta1_rows.drop_duplicates(subset=['station_id'], keep='first').reset_index(drop=True)


def cache_candidate_daily_inputs(
    candidates: pd.DataFrame,
    *,
    cache_dir: Path,
    config: FaoCountryConfig,
    mode: str,
    timeout: int,
    reporter: ProgressReporter,
    stats: CacheStats,
) -> tuple[int, list[str], list[str]]:
    # Cache normalized daily observations per station so rebuilds can run offline.
    available_station_count = 0
    missing_station_ids: list[str] = []
    failed_station_ids: list[str] = []
    total_candidates = len(candidates)
    reporter.info(f'Checking daily input cache for {total_candidates} station(s).')
    for index, station in enumerate(candidates.itertuples(index=False), start=1):
        result = ensure_daily_observations_cached(
            station.station_id,
            cache_dir=cache_dir,
            config=config,
            mode=mode,
            timeout=timeout,
            stats=stats,
        )
        if result.available:
            available_station_count += 1
        if result.missing:
            missing_station_ids.append(station.station_id)
        if result.failed:
            failed_station_ids.append(station.station_id)
        reporter.info(
            f'[{index}/{total_candidates}] {station.station_id} ({station.full_name}): {result.summary()}'
        )
    return available_station_count, missing_station_ids, failed_station_ids


def ensure_daily_observations_cached(
    station_id: str,
    *,
    cache_dir: Path,
    config: FaoCountryConfig,
    mode: str,
    timeout: int,
    stats: CacheStats,
) -> StationCacheResult:
    result = StationCacheResult(station_id)
    cache_path = cached_daily_observations_path(cache_dir, station_id)
    if cache_path.exists():
        result.add_status('reused')
        stats.add_file_status('reused')
        return result
    if mode == 'build':
        result.add_status('missing')
        stats.add_file_status('missing')
        return result
    try:
        # Download the full normalized daily history for the required canonical variables.
        query = ObservationQuery(
            country=config.country,
            dataset_scope=config.dataset_scope,
            resolution=config.resolution,
            station_ids=[station_id],
            all_history=True,
            elements=config.canonical_elements,
        )
        observations = download_observations(query, timeout=timeout, country=config.country)
    except Exception:
        result.add_status('failed')
        stats.add_file_status('failed')
        return result
    if observations.empty:
        result.add_status('missing')
        stats.add_file_status('missing')
        return result
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    observations.to_csv(cache_path, index=False)
    result.add_status('downloaded')
    stats.add_file_status('downloaded')
    return result


def read_cached_daily_observations(station_id: str, *, cache_dir: Path) -> pd.DataFrame:
    cache_path = cached_daily_observations_path(cache_dir, station_id)
    if not cache_path.exists():
        raise CacheMissingError(f'Missing cached daily observations for station {station_id}: {cache_path}')
    table = pd.read_csv(cache_path)
    table['observation_date'] = pd.to_datetime(table['observation_date']).dt.date
    return table


def cached_daily_observations_path(cache_dir: Path, station_id: str) -> Path:
    return cache_dir / 'daily' / station_id / f'daily-{station_id}.csv'


def prepare_complete_station_series(daily_table: pd.DataFrame, *, config: FaoCountryConfig) -> pd.DataFrame:
    # Pivot the long-form daily observations into one complete-day table per station.
    selected_tables: list[pd.DataFrame] = []
    for canonical_name in FINAL_SERIES_COLUMNS:
        selected = select_daily_variable_rows(daily_table, canonical_name=canonical_name, config=config)
        if selected.empty:
            return pd.DataFrame(columns=['Date', *FINAL_SERIES_COLUMNS])
        selected_tables.append(selected)

    merged = selected_tables[0]
    for table in selected_tables[1:]:
        merged = merged.merge(table, on='Date', how='inner')

    complete = merged.dropna(subset=FINAL_SERIES_COLUMNS).sort_values('Date').reset_index(drop=True)
    return complete


def select_daily_variable_rows(
    daily_table: pd.DataFrame,
    *,
    canonical_name: str,
    config: FaoCountryConfig,
) -> pd.DataFrame:
    # Apply the country-specific daily selection rule, if one exists, before merging by date.
    filtered = daily_table[daily_table['element'].astype(str) == canonical_name].copy()
    required_time_function = config.time_function_by_canonical.get(canonical_name)
    if required_time_function is not None:
        filtered = filtered[filtered['time_function'].astype(str).str.strip() == required_time_function]
    if filtered.empty:
        return pd.DataFrame(columns=['Date', canonical_name])
    filtered['Date'] = pd.to_datetime(filtered['observation_date']).dt.date
    filtered[canonical_name] = pd.to_numeric(filtered['value'], errors='coerce')
    filtered = filtered[['Date', canonical_name]].dropna(subset=[canonical_name]).drop_duplicates(subset=['Date'], keep='last')
    return filtered.reset_index(drop=True)


def build_series_record(
    complete: pd.DataFrame,
    *,
    station_id: str,
    full_name: str,
    latitude: float | None,
    longitude: float | None,
    elevation: float | None,
) -> dict[str, Any]:
    return {
        'WSI': station_id,
        'FULL_NAME': full_name,
        'Latitude': latitude,
        'Longitude': longitude,
        'Elevation': elevation,
        'Date': [value.isoformat() for value in complete['Date'].tolist()],
        'tas_mean': complete['tas_mean'].astype(float).tolist(),
        'tas_max': complete['tas_max'].astype(float).tolist(),
        'tas_min': complete['tas_min'].astype(float).tolist(),
        'wind_speed': complete['wind_speed'].astype(float).tolist(),
        'vapour_pressure': complete['vapour_pressure'].astype(float).tolist(),
        'sunshine_duration': complete['sunshine_duration'].astype(float).tolist(),
    }


def export_mat_bundle(output_path: Path, *, data_info: dict[str, Any], stations: list[dict[str, Any]], series: list[dict[str, Any]]) -> None:
    from scipy.io import savemat

    # Export a MATLAB-oriented nested bundle for downstream workflows.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'dataInfo': _to_mat_struct(data_info),
        'stations': _station_rows_to_struct(stations),
        'series': np.array([_to_mat_struct(item) for item in series], dtype=object),
    }
    savemat(output_path, payload)


def export_parquet_bundle(
    output_dir: Path,
    *,
    data_info: dict[str, Any],
    stations: list[dict[str, Any]],
    series: list[dict[str, Any]],
) -> None:
    # Export a portable bundle directory for R, Python, or later MATLAB import.
    output_dir.mkdir(parents=True, exist_ok=True)
    data_info_path = output_dir / 'data_info.json'
    stations_path = output_dir / 'stations.parquet'
    series_path = output_dir / 'series.parquet'

    data_info_path.write_text(json.dumps(data_info, indent=2), encoding='utf-8')
    export_table(build_station_table(stations), stations_path, format='parquet')
    export_table(build_series_table(series), series_path, format='parquet')


def build_provider_element_mapping(config: FaoCountryConfig) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for canonical_name in FINAL_SERIES_COLUMNS:
        mapping[canonical_name] = {
            'raw_codes': list(config.canonical_to_raw[canonical_name]),
            'selection_rule': config.time_function_by_canonical.get(canonical_name),
        }
    return mapping


def build_station_table(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        'WSI',
        'FULL_NAME',
        'Latitude',
        'Longitude',
        'Elevation',
        'NumCompleteDays_E',
        'FirstCompleteDate_E',
        'LastCompleteDate_E',
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame.from_records(rows, columns=columns)


def build_series_table(series: list[dict[str, Any]]) -> pd.DataFrame:
    columns = ['WSI', 'FULL_NAME', 'Latitude', 'Longitude', 'Elevation', 'Date', *FINAL_SERIES_COLUMNS]
    records: list[dict[str, Any]] = []
    for item in series:
        dates = item['Date']
        num_rows = len(dates)
        for index in range(num_rows):
            records.append(
                {
                    'WSI': item['WSI'],
                    'FULL_NAME': item['FULL_NAME'],
                    'Latitude': item['Latitude'],
                    'Longitude': item['Longitude'],
                    'Elevation': item['Elevation'],
                    'Date': dates[index],
                    'tas_mean': item['tas_mean'][index],
                    'tas_max': item['tas_max'][index],
                    'tas_min': item['tas_min'][index],
                    'wind_speed': item['wind_speed'][index],
                    'vapour_pressure': item['vapour_pressure'][index],
                    'sunshine_duration': item['sunshine_duration'][index],
                }
            )
    if not records:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame.from_records(records, columns=columns)


def print_final_summary(reporter: ProgressReporter, stats: CacheStats) -> None:
    reporter.essential(
        'Cache summary: '
        + f"downloaded={stats.downloaded}, reused={stats.reused}, missing={stats.missing}, failed={stats.failed}, "
        + f"candidate_stations={stats.candidate_stations}, cache_ready_stations={stats.cache_ready_stations}"
    )


def format_file_status(status: str) -> str:
    if status == 'downloaded':
        return 'downloaded'
    if status == 'reused':
        return 'reused from cache'
    if status == 'missing':
        return 'missing'
    if status == 'failed':
        return 'failed'
    return status


def _station_rows_to_struct(rows: list[dict[str, Any]]) -> dict[str, np.ndarray]:
    if not rows:
        return {
            'WSI': np.array([], dtype=object),
            'FULL_NAME': np.array([], dtype=object),
            'Latitude': np.array([], dtype=np.float64),
            'Longitude': np.array([], dtype=np.float64),
            'Elevation': np.array([], dtype=np.float64),
            'NumCompleteDays_E': np.array([], dtype=np.float64),
            'FirstCompleteDate_E': np.array([], dtype=object),
            'LastCompleteDate_E': np.array([], dtype=object),
        }
    columns = rows[0].keys()
    return {
        column: _to_mat_value([row[column] for row in rows])
        for column in columns
    }


def _to_mat_struct(mapping: dict[str, Any]) -> dict[str, Any]:
    return {
        key: _to_mat_value(value)
        for key, value in mapping.items()
    }


def _to_mat_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _to_mat_struct(value)
    if isinstance(value, list):
        if not value:
            return np.array([], dtype=object)
        if all(isinstance(item, dict) for item in value):
            return np.array([_to_mat_struct(item) for item in value], dtype=object)
        if all(isinstance(item, str) for item in value):
            return np.array(value, dtype=object)
        if all(isinstance(item, (int, float, np.floating, np.integer)) or pd.isna(item) for item in value):
            return np.array([np.nan if pd.isna(item) else float(item) for item in value], dtype=np.float64)
        return np.array([_to_mat_value(item) for item in value], dtype=object)
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, np.floating, np.integer)):
        return float(value)
    if value is None or pd.isna(value):
        return np.nan
    return value


if __name__ == '__main__':
    raise SystemExit(main())
