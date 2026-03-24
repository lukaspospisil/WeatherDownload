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
AT_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'sunshine_duration',
)
BE_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'sunshine_duration',
)
DK_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'sunshine_duration',
)
NL_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'sunshine_duration',
)
SE_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
)
AT_ASSUMPTIONS = {
    'wind_height_handling': (
        'wind_speed uses GeoSphere daily vv_mittel as delivered. This shared workflow does not convert wind speed to FAO 2 m wind speed, '
        'because the dataset metadata used here do not provide a normalized measurement height contract for that conversion.'
    ),
    'pressure_usage': (
        'GeoSphere daily pressure is available in the provider, but it is intentionally not included in this shared FAO-preparation bundle. '
        'The current cross-country workflow exports only the common daily core inputs used downstream in the existing sample workflow.'
    ),
    'relative_humidity_interpretation': (
        'GeoSphere daily rf_mittel exists in the provider, but this shared workflow does not use it to derive new variables. ' 
        'vapour_pressure therefore remains empty for Austria in the shared bundle.'
    ),
    'sunshine_duration_to_radiation': (
        'sunshine_duration uses GeoSphere daily so_h as observed sunshine duration in hours. '
        'This shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
}
AT_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['tl_mittel'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['tlmax'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['tlmin'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['vv_mittel'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Austria daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
    'sunshine_duration': {'raw_codes': ['so_h'], 'selection_rule': None, 'status': 'observed'},
}
BE_ASSUMPTIONS = {
    'observed_inputs_only': (
        'The Belgium branch packages only source-backed daily observations from the RMI/KMI provider. '
        'The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.'
    ),
    'provider_daily_aggregation': (
        'Belgium daily values come from the official provider-side aws_1day aggregation. '
        'This shared workflow does not recompute daily values from 10-minute data.'
    ),
    'vapour_pressure_availability': (
        'The current Belgium daily provider path does not expose observed vapour_pressure for this shared workflow. '
        'The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.'
    ),
    'pressure_usage': (
        'Belgium daily pressure is available in the provider, but this shared workflow does not use it to derive any new variables and does not export pressure in the FAO-oriented bundle.'
    ),
    'sunshine_duration_to_radiation': (
        'sunshine_duration uses observed Belgium daily sunshine duration only. '
        'The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
}
BE_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['temp_avg'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['temp_max'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['temp_min'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['wind_speed_10m'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Belgium daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
    'sunshine_duration': {'raw_codes': ['sun_duration'], 'selection_rule': None, 'status': 'observed'},
}
DK_ASSUMPTIONS = {
    'observed_inputs_only': (
        'The Denmark branch packages only source-backed daily observations from the DMI provider. '
        'The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.'
    ),
    'denmark_only_scope': (
        'This shared workflow uses the current Denmark-only DMI daily provider slice. '
        'Greenland and Faroe Islands differences are out of scope in this pass.'
    ),
    'vapour_pressure_availability': (
        'The current Denmark daily provider path does not expose observed vapour_pressure for this shared workflow. '
        'The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.'
    ),
    'pressure_usage': (
        'Denmark daily pressure is available in the provider, but this shared workflow does not use it to derive any new variables and does not export pressure in the FAO-oriented bundle.'
    ),
    'sunshine_duration_to_radiation': (
        'sunshine_duration uses observed Denmark daily sunshine duration only. '
        'The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
}
DK_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['mean_temp'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['mean_daily_max_temp'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['mean_daily_min_temp'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['mean_wind_speed'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Denmark daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
    'sunshine_duration': {'raw_codes': ['bright_sunshine'], 'selection_rule': None, 'status': 'observed'},
}
NL_ASSUMPTIONS = {
    'observed_inputs_only': (
        'The Netherlands branch packages only source-backed daily observations from the KNMI provider. '
        'The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.'
    ),
    'vapour_pressure_availability': (
        'KNMI NL historical daily support in this pass does not expose observed vapour_pressure in the provider path used here. '
        'The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.'
    ),
    'pressure_usage': (
        'KNMI daily pressure is available in the provider, but this shared workflow does not use it to derive any new variables and does not export pressure in the FAO-oriented bundle.'
    ),
    'sunshine_duration_to_radiation': (
        'sunshine_duration uses observed KNMI daily sunshine duration only. '
        'The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
}
NL_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['TG'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['TX'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['TN'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['FG'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Netherlands daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
    'sunshine_duration': {'raw_codes': ['SQ'], 'selection_rule': None, 'status': 'observed'},
}
SE_ASSUMPTIONS = {
    'observed_inputs_only': (
        'The Sweden branch packages only source-backed daily observations from the SMHI provider. '
        'The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables.'
    ),
    'corrected_archive_limit': (
        'The current Sweden daily provider path uses the official SMHI corrected-archive daily CSV path. '
        'That source excludes the latest three months while quality control is still in progress.'
    ),
    'wind_speed_availability': (
        'The current Sweden daily provider path used by this shared workflow does not expose observed daily wind_speed. '
        'The shared workflow leaves wind_speed empty instead of deriving it or substituting hourly data.'
    ),
    'vapour_pressure_availability': (
        'The current Sweden daily provider path does not expose observed vapour_pressure for this shared workflow. '
        'The shared workflow leaves vapour_pressure empty instead of deriving it from humidity or temperature.'
    ),
    'sunshine_duration_availability': (
        'The current Sweden daily provider path used by this shared workflow does not expose observed daily sunshine_duration. '
        'The shared workflow leaves sunshine_duration empty instead of estimating it from other fields.'
    ),
}
SE_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['2'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['20'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['19'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Sweden daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
    'vapour_pressure': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Sweden daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
    'sunshine_duration': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Sweden daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
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
    required_complete_elements: tuple[str, ...]
    query_elements: tuple[str, ...]
    provider_element_mapping: dict[str, dict[str, Any]]
    assumptions: dict[str, str]
    dataset_type: str
    source: str


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

        config = get_fao_country_config(args.country)
        country_cache_dir = resolve_country_cache_dir(args.cache_dir, config.country)
        mat_output_path = resolve_mat_output_path(args.output, country=config.country)
        parquet_output_dir = resolve_parquet_output_dir(args.output_dir, country=config.country)

        reporter.info(f'Using cache directory: {country_cache_dir}')
        meta1 = load_station_metadata_with_cache(country_cache_dir, country=config.country, mode=args.mode, timeout=args.timeout, reporter=reporter, stats=stats)
        meta2 = load_station_observation_metadata_with_cache(country_cache_dir, country=config.country, mode=args.mode, timeout=args.timeout, reporter=reporter, stats=stats)

        if args.station_ids:
            requested = {station_id.strip().upper() for station_id in args.station_ids}
            meta1 = meta1[meta1['station_id'].str.upper().isin(requested)].reset_index(drop=True)
            meta2 = meta2[meta2['station_id'].str.upper().isin(requested)].reset_index(drop=True)

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
            raise CacheMissingError('Build mode requires a complete local cache. Missing or failed stations: ' + ', '.join(problem_ids))

        unavailable_station_ids = set(missing_station_ids) | set(failed_station_ids)
        retained_series: list[dict[str, Any]] = []
        station_rows: list[dict[str, Any]] = []

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

            retained_series.append(build_series_record(complete, station_id=station.station_id, full_name=station.full_name, latitude=station.latitude, longitude=station.longitude, elevation=station.elevation_m))
            station_rows.append({
                'station_id': station.station_id,
                'full_name': station.full_name,
                'latitude': station.latitude,
                'longitude': station.longitude,
                'elevation_m': station.elevation_m,
                'num_complete_days': int(len(complete)),
                'first_complete_date': complete['date'].min().isoformat(),
                'last_complete_date': complete['date'].max().isoformat(),
            })

        data_info = build_data_info(config, station_rows, min_complete_days=args.min_complete_days)
        exported_targets: list[str] = []
        if args.export_format in {'mat', 'both'}:
            export_mat_bundle(mat_output_path, data_info=data_info, stations=station_rows, series=retained_series)
            exported_targets.append(str(mat_output_path))
        if args.export_format in {'parquet', 'both'}:
            export_parquet_bundle(parquet_output_dir, data_info=data_info, stations=station_rows, series=retained_series)
            exported_targets.append(str(parquet_output_dir))
        reporter.essential(f"Exported FAO-prep output to: {', '.join(exported_targets)}")
        print_final_summary(reporter, stats)
        return 0
    except Exception as exc:
        import sys

        print(f'Error: {exc}', file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Download and package a country-aware daily meteorological input bundle for later FAO workflow use.')
    parser.add_argument('--country', default='CZ', help='ISO 3166-1 alpha-2 country code. Defaults to CZ.')
    parser.add_argument('--mode', choices=['full', 'download', 'build'], default='full', help='Run the full pipeline, cache inputs only, or build only from cached inputs.')
    parser.add_argument('--cache-dir', type=Path, default=Path('outputs/fao_cache'), help='Base cache directory for metadata and daily inputs.')
    parser.add_argument('--output', type=Path, default=None, help='MATLAB .mat output path.')
    parser.add_argument('--output-dir', type=Path, default=None, help='Portable Parquet bundle output directory.')
    parser.add_argument('--export-format', choices=['mat', 'parquet', 'both'], default='mat', help='Output format to produce in full or build mode.')
    parser.add_argument('--station-id', action='append', dest='station_ids', help='Optional canonical station_id filter. Can be provided multiple times.')
    parser.add_argument('--min-complete-days', type=int, default=3650, help='Minimum number of complete observed-input days required per station.')
    parser.add_argument('--timeout', type=int, default=60, help='HTTP timeout in seconds.')
    parser.add_argument('--silent', action='store_true', help='Suppress non-essential progress output.')
    return parser


def get_fao_country_config(country: str | None) -> FaoCountryConfig:
    normalized_country = normalize_country_code(country)
    try:
        provider = get_provider(normalized_country)
        daily_spec = provider.get_dataset_spec('historical_csv' if normalized_country == 'CZ' else 'historical', 'daily')
    except Exception as exc:
        raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.') from exc

    canonical_to_raw = daily_spec.canonical_elements or {}
    raw_to_provider_canonical = raw_to_canonical_map_for_spec(daily_spec)
    if normalized_country == 'AT':
        query_elements = AT_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'BE':
        query_elements = BE_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'DK':
        query_elements = DK_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'NL':
        query_elements = NL_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'SE':
        query_elements = SE_REQUIRED_OBSERVED_ELEMENTS
    else:
        query_elements = FAO_CANONICAL_ELEMENTS

    selected_canonical_to_raw: dict[str, tuple[str, ...]] = {}
    raw_to_canonical: dict[str, str] = {}
    for canonical_name in query_elements:
        raw_codes = canonical_to_raw.get(canonical_name, ())
        if not raw_codes:
            raise ValueError(f'FAO preparation example requires daily canonical element {canonical_name!r} for country {normalized_country}.')
        selected_canonical_to_raw[canonical_name] = tuple(raw_codes)
    for raw_code, canonical_name in raw_to_provider_canonical.items():
        if canonical_name in selected_canonical_to_raw:
            raw_to_canonical[raw_code.upper()] = canonical_name

    if normalized_country == 'CZ':
        return FaoCountryConfig('CZ', 'historical_csv', 'daily', ('DLY',), selected_canonical_to_raw, raw_to_canonical, dict(CZ_TIMEFUNC_BY_CANONICAL), FAO_CANONICAL_ELEMENTS, FAO_CANONICAL_ELEMENTS, build_observed_provider_element_mapping(selected_canonical_to_raw, dict(CZ_TIMEFUNC_BY_CANONICAL)), {}, 'CHMI observed daily input bundle prepared for later FAO workflow packaging', 'CHMI OpenData historical_csv metadata and daily observations')
    if normalized_country == 'DE':
        return FaoCountryConfig('DE', 'historical', 'daily', ('DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, FAO_CANONICAL_ELEMENTS, FAO_CANONICAL_ELEMENTS, build_observed_provider_element_mapping(selected_canonical_to_raw, {}), {}, 'DWD observed daily input bundle prepared for later FAO workflow packaging', 'DWD CDC historical daily metadata and observations')
    if normalized_country == 'AT':
        return FaoCountryConfig('AT', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, AT_REQUIRED_OBSERVED_ELEMENTS, AT_REQUIRED_OBSERVED_ELEMENTS, dict(AT_PROVIDER_ELEMENT_MAPPING), dict(AT_ASSUMPTIONS), 'GeoSphere Austria observed daily input bundle prepared for later FAO workflow packaging', 'GeoSphere Austria Dataset API station historical daily klima-v2-1d')
    if normalized_country == 'BE':
        return FaoCountryConfig('BE', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, BE_REQUIRED_OBSERVED_ELEMENTS, BE_REQUIRED_OBSERVED_ELEMENTS, dict(BE_PROVIDER_ELEMENT_MAPPING), dict(BE_ASSUMPTIONS), 'RMI/KMI Belgium observed daily input bundle prepared for later FAO workflow packaging', 'RMI/KMI open-data platform aws_1day daily station observations')
    if normalized_country == 'DK':
        return FaoCountryConfig('DK', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, DK_REQUIRED_OBSERVED_ELEMENTS, DK_REQUIRED_OBSERVED_ELEMENTS, dict(DK_PROVIDER_ELEMENT_MAPPING), dict(DK_ASSUMPTIONS), 'DMI Denmark observed daily input bundle prepared for later FAO workflow packaging', 'DMI Climate Data station and stationValue daily station observations for Denmark')
    if normalized_country == 'NL':
        return FaoCountryConfig('NL', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, NL_REQUIRED_OBSERVED_ELEMENTS, NL_REQUIRED_OBSERVED_ELEMENTS, dict(NL_PROVIDER_ELEMENT_MAPPING), dict(NL_ASSUMPTIONS), 'KNMI observed daily input bundle prepared for later FAO workflow packaging', 'KNMI Open Data API validated daily in-situ meteorological observations')
    if normalized_country == 'SE':
        return FaoCountryConfig('SE', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, SE_REQUIRED_OBSERVED_ELEMENTS, SE_REQUIRED_OBSERVED_ELEMENTS, dict(SE_PROVIDER_ELEMENT_MAPPING), dict(SE_ASSUMPTIONS), 'SMHI Sweden observed daily input bundle prepared for later FAO workflow packaging', 'SMHI Meteorological Observations corrected-archive daily station observations')
    raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.')


def build_observed_provider_element_mapping(canonical_to_raw: dict[str, tuple[str, ...]], time_function_by_canonical: dict[str, str]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for canonical_name in FINAL_SERIES_COLUMNS:
        mapping[canonical_name] = {'raw_codes': list(canonical_to_raw[canonical_name]), 'selection_rule': time_function_by_canonical.get(canonical_name), 'status': 'observed'}
    return mapping


def build_data_info(config: FaoCountryConfig, station_rows: list[dict[str, Any]], *, min_complete_days: int) -> dict[str, Any]:
    data_info = {
        'created_at': pd.Timestamp.now('UTC').isoformat(),
        'dataset_type': config.dataset_type,
        'source': config.source,
        'country': config.country,
        'elements': FINAL_SERIES_COLUMNS,
        'provider_element_mapping': build_provider_element_mapping(config),
        'min_complete_days': int(min_complete_days),
        'num_stations': int(len(station_rows)),
    }
    if config.assumptions:
        data_info['assumptions'] = dict(config.assumptions)
    return data_info

def resolve_country_cache_dir(cache_dir: Path, country: str) -> Path:
    return cache_dir / normalize_country_code(country)


def resolve_mat_output_path(output: Path | None, *, country: str) -> Path:
    if output is not None:
        return output
    return Path('outputs') / f"fao_daily.{normalize_country_code(country).lower()}.mat"


def resolve_parquet_output_dir(output_dir: Path | None, *, country: str) -> Path:
    if output_dir is not None:
        return output_dir
    return Path('outputs') / f"fao_daily.{normalize_country_code(country).lower()}"


def load_station_metadata_with_cache(cache_dir: Path, *, country: str, mode: str, timeout: int, reporter: ProgressReporter, stats: CacheStats) -> pd.DataFrame:
    table, status = load_cached_dataframe(cache_path=cache_dir / 'meta1.csv', mode=mode, loader=lambda: read_station_metadata(country=country, timeout=timeout), description='meta1.csv')
    stats.add_metadata_status(status)
    reporter.info(f'meta1.csv: {format_file_status(status)}')
    return table


def load_station_observation_metadata_with_cache(cache_dir: Path, *, country: str, mode: str, timeout: int, reporter: ProgressReporter, stats: CacheStats) -> pd.DataFrame:
    table, status = load_cached_dataframe(cache_path=cache_dir / 'meta2.csv', mode=mode, loader=lambda: read_station_observation_metadata(country=country, timeout=timeout), description='meta2.csv')
    stats.add_metadata_status(status)
    reporter.info(f'meta2.csv: {format_file_status(status)}')
    return table


def load_cached_dataframe(*, cache_path: Path, mode: str, loader, description: str) -> tuple[pd.DataFrame, str]:
    if cache_path.exists():
        return pd.read_csv(cache_path), 'reused'
    if mode == 'build':
        raise CacheMissingError(f'Missing cached {description}: {cache_path}')
    table = loader()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(cache_path, index=False)
    return table, 'downloaded'


def screen_candidate_stations(meta1: pd.DataFrame, meta2: pd.DataFrame, *, config: FaoCountryConfig, min_complete_days: int) -> pd.DataFrame:
    relevant = meta2[
        meta2['obs_type'].astype(str).str.upper().isin(config.obs_types)
        & meta2['element'].astype(str).str.upper().isin(config.raw_to_canonical)
    ].copy()
    relevant['canonical_element'] = relevant['element'].astype(str).str.upper().map(config.raw_to_canonical)
    supported = relevant.groupby('station_id')['canonical_element'].agg(lambda values: set(values)).reset_index(name='canonical_elements')
    supported['has_required_elements'] = supported['canonical_elements'].apply(lambda values: set(config.required_complete_elements).issubset(values))

    overlap = estimate_station_overlap_days(relevant)
    screened = supported.merge(overlap, on='station_id', how='left')
    screened['overlap_days_estimate'] = screened['overlap_days_estimate'].fillna(0).astype(int)
    candidate_ids = screened[screened['has_required_elements'] & (screened['overlap_days_estimate'] >= min_complete_days)]['station_id']
    candidate_rows = meta1[meta1['station_id'].isin(candidate_ids)].copy()
    return deduplicate_candidate_stations(candidate_rows)


def estimate_station_overlap_days(meta2: pd.DataFrame) -> pd.DataFrame:
    if meta2.empty:
        return pd.DataFrame(columns=['station_id', 'overlap_days_estimate'])
    dated = meta2.copy()
    dated['begin_ts'] = pd.to_datetime(dated['begin_date'], utc=True)
    dated['end_ts'] = pd.to_datetime(dated['end_date'], utc=True)
    element_spans = dated.groupby(['station_id', 'canonical_element']).agg(begin_ts=('begin_ts', 'min'), end_ts=('end_ts', 'max')).reset_index()
    station_spans = element_spans.groupby('station_id').agg(overlap_begin=('begin_ts', 'max'), overlap_end=('end_ts', 'min')).reset_index()
    station_spans['overlap_days_estimate'] = ((station_spans['overlap_end'] - station_spans['overlap_begin']).dt.days + 1).clip(lower=0)
    return station_spans[['station_id', 'overlap_days_estimate']]


def deduplicate_candidate_stations(meta1_rows: pd.DataFrame) -> pd.DataFrame:
    if meta1_rows.empty:
        return meta1_rows.reset_index(drop=True)
    return meta1_rows.drop_duplicates(subset=['station_id'], keep='first').reset_index(drop=True)


def cache_candidate_daily_inputs(candidates: pd.DataFrame, *, cache_dir: Path, config: FaoCountryConfig, mode: str, timeout: int, reporter: ProgressReporter, stats: CacheStats) -> tuple[int, list[str], list[str]]:
    available_station_count = 0
    missing_station_ids: list[str] = []
    failed_station_ids: list[str] = []
    total_candidates = len(candidates)
    reporter.info(f'Checking daily input cache for {total_candidates} station(s).')
    for index, station in enumerate(candidates.itertuples(index=False), start=1):
        result = ensure_daily_observations_cached(station.station_id, cache_dir=cache_dir, config=config, mode=mode, timeout=timeout, stats=stats)
        if result.available:
            available_station_count += 1
        if result.missing:
            missing_station_ids.append(station.station_id)
        if result.failed:
            failed_station_ids.append(station.station_id)
        reporter.info(f'[{index}/{total_candidates}] {station.station_id} ({station.full_name}): {result.summary()}')
    return available_station_count, missing_station_ids, failed_station_ids


def ensure_daily_observations_cached(station_id: str, *, cache_dir: Path, config: FaoCountryConfig, mode: str, timeout: int, stats: CacheStats) -> StationCacheResult:
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
        query = ObservationQuery(
            country=config.country,
            dataset_scope=config.dataset_scope,
            resolution=config.resolution,
            station_ids=[station_id],
            all_history=True,
            elements=list(config.query_elements),
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
    table = pd.read_csv(cache_path, dtype={'flag': 'string'})
    table['observation_date'] = pd.to_datetime(table['observation_date']).dt.date
    return table


def cached_daily_observations_path(cache_dir: Path, station_id: str) -> Path:
    return cache_dir / 'daily' / station_id / f'daily-{station_id}.csv'


def prepare_complete_station_series(daily_table: pd.DataFrame, *, config: FaoCountryConfig) -> pd.DataFrame:
    selected_tables: list[pd.DataFrame] = []
    for canonical_name in config.required_complete_elements:
        selected = select_daily_variable_rows(daily_table, canonical_name=canonical_name, config=config)
        if selected.empty:
            return pd.DataFrame(columns=['date', *FINAL_SERIES_COLUMNS])
        selected_tables.append(selected)

    merged = selected_tables[0]
    for table in selected_tables[1:]:
        merged = merged.merge(table, on='date', how='inner')

    for canonical_name in FINAL_SERIES_COLUMNS:
        if canonical_name not in merged.columns:
            merged[canonical_name] = pd.NA

    complete = merged.dropna(subset=list(config.required_complete_elements)).sort_values('date').reset_index(drop=True)
    return complete.loc[:, ['date', *FINAL_SERIES_COLUMNS]]


def select_daily_variable_rows(daily_table: pd.DataFrame, *, canonical_name: str, config: FaoCountryConfig) -> pd.DataFrame:
    filtered = daily_table[daily_table['element'].astype(str) == canonical_name].copy()
    required_time_function = config.time_function_by_canonical.get(canonical_name)
    if required_time_function is not None:
        filtered = filtered[filtered['time_function'].astype(str).str.strip() == required_time_function]
    if filtered.empty:
        return pd.DataFrame(columns=['date', canonical_name])
    filtered['date'] = pd.to_datetime(filtered['observation_date']).dt.date
    filtered[canonical_name] = pd.to_numeric(filtered['value'], errors='coerce')
    filtered = filtered[['date', canonical_name]].dropna(subset=[canonical_name]).drop_duplicates(subset=['date'], keep='last')
    return filtered.reset_index(drop=True)


def build_series_record(complete: pd.DataFrame, *, station_id: str, full_name: str, latitude: float | None, longitude: float | None, elevation: float | None) -> dict[str, Any]:
    return {
        'station_id': station_id,
        'full_name': full_name,
        'latitude': latitude,
        'longitude': longitude,
        'elevation_m': elevation,
        'date': [value.isoformat() for value in complete['date'].tolist()],
        'tas_mean': pd.to_numeric(complete['tas_mean'], errors='coerce').tolist(),
        'tas_max': pd.to_numeric(complete['tas_max'], errors='coerce').tolist(),
        'tas_min': pd.to_numeric(complete['tas_min'], errors='coerce').tolist(),
        'wind_speed': pd.to_numeric(complete['wind_speed'], errors='coerce').tolist(),
        'vapour_pressure': pd.to_numeric(complete['vapour_pressure'], errors='coerce').tolist(),
        'sunshine_duration': pd.to_numeric(complete['sunshine_duration'], errors='coerce').tolist(),
    }


def export_mat_bundle(output_path: Path, *, data_info: dict[str, Any], stations: list[dict[str, Any]], series: list[dict[str, Any]]) -> None:
    from scipy.io import savemat

    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'data_info': _to_mat_struct(data_info),
        'stations': _station_rows_to_struct(stations),
        'series': np.array([_to_mat_struct(item) for item in series], dtype=object),
    }
    savemat(output_path, payload)


def export_parquet_bundle(output_dir: Path, *, data_info: dict[str, Any], stations: list[dict[str, Any]], series: list[dict[str, Any]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    data_info_path = output_dir / 'data_info.json'
    stations_path = output_dir / 'stations.parquet'
    series_path = output_dir / 'series.parquet'

    data_info_path.write_text(json.dumps(data_info, indent=2), encoding='utf-8')
    export_table(build_station_table(stations), stations_path, format='parquet')
    export_table(build_series_table(series), series_path, format='parquet')


def build_provider_element_mapping(config: FaoCountryConfig) -> dict[str, dict[str, Any]]:
    return dict(config.provider_element_mapping)


def build_station_table(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = ['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'num_complete_days', 'first_complete_date', 'last_complete_date']
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame.from_records(rows, columns=columns)


def build_series_table(series: list[dict[str, Any]]) -> pd.DataFrame:
    columns = ['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'date', *FINAL_SERIES_COLUMNS]
    records: list[dict[str, Any]] = []
    for item in series:
        dates = item['date']
        num_rows = len(dates)
        for index in range(num_rows):
            records.append(
                {
                    'station_id': item['station_id'],
                    'full_name': item['full_name'],
                    'latitude': item['latitude'],
                    'longitude': item['longitude'],
                    'elevation_m': item['elevation_m'],
                    'date': dates[index],
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
            'station_id': np.array([], dtype=object),
            'full_name': np.array([], dtype=object),
            'latitude': np.array([], dtype=np.float64),
            'longitude': np.array([], dtype=np.float64),
            'elevation_m': np.array([], dtype=np.float64),
            'num_complete_days': np.array([], dtype=np.float64),
            'first_complete_date': np.array([], dtype=object),
            'last_complete_date': np.array([], dtype=object),
        }
    columns = rows[0].keys()
    return {column: _to_mat_value([row[column] for row in rows]) for column in columns}


def _to_mat_struct(mapping: dict[str, Any]) -> dict[str, Any]:
    return {key: _to_mat_value(value) for key, value in mapping.items()}


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








