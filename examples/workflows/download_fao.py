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
from weatherdownload import fao as shared_fao
from weatherdownload import fao_config as shared_fao_config
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
FILL_MISSING_CHOICES = ('none', 'allow-derived', 'allow-hourly-aggregate')
DERIVED_VAPOUR_PRESSURE_RULE_NAME = 'vapour_pressure_from_tas_mean_and_relative_humidity'
DERIVED_VAPOUR_PRESSURE_RULE_DESCRIPTION = (
    'Derived vapour_pressure from observed daily tas_mean and relative_humidity '
    'using the Magnus saturation-vapour-pressure formula in hPa.'
)
PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS = 18
PL_HOURLY_WIND_SPEED_RULE_DESCRIPTION = (
    'Filled daily wind_speed from official IMGW historical/1hour wind_speed by arithmetic mean over the UTC calendar day '
    f'when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly observations were available.'
)
PL_HOURLY_VAPOUR_PRESSURE_RULE_DESCRIPTION = (
    'Filled daily vapour_pressure from official IMGW historical/1hour vapour_pressure by arithmetic mean over the UTC calendar day '
    f'when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly observations were available.'
)
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
CH_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'sunshine_duration',
)
HU_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'sunshine_duration',
)
PL_REQUIRED_OBSERVED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
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
CH_ASSUMPTIONS = {
    'observed_inputs_only': (
        'The Switzerland branch packages source-backed daily observations from the MeteoSwiss A1 provider slice for later FAO-oriented processing. '
        'It does not compute FAO-56 ET0.'
    ),
    'vapour_pressure_availability': (
        'Observed daily vapour_pressure is available from the implemented MeteoSwiss A1 daily provider path and is used directly when present.'
    ),
    'fallback_policy': (
        'If the shared allow-derived policy is enabled and observed vapour_pressure is missing on some rows, the existing shared fallback from '
        'observed tas_mean plus observed relative_humidity may be used. No CH-specific derivation logic is introduced.'
    ),
    'sunshine_duration_to_radiation': (
        'Observed daily sunshine_duration is exported as input only. The workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
    'daily_precipitation_window': (
        'MeteoSwiss daily precipitation semantics remain provider-defined. This FAO-prep workflow does not reinterpret or recompute them.'
    ),
}
CH_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['tre200d0'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['tre200dx'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['tre200dn'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['fkl010d0'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {'raw_codes': ['pva200d0'], 'selection_rule': None, 'status': 'observed'},
    'sunshine_duration': {'raw_codes': ['sre000d0'], 'selection_rule': None, 'status': 'observed'},
}
HU_ASSUMPTIONS = {
    'observed_inputs_only': (
        'The Hungary branch packages only source-backed daily observations from the HungaroMet provider. '
        'The shared workflow does not compute FAO-56 ET0 or derive any meteorological variables by default.'
    ),
    'vapour_pressure_availability': (
        'The current Hungary daily provider path does not expose observed vapour_pressure for this shared workflow. '
        'The shared workflow leaves vapour_pressure empty unless the optional shared allow-derived fill policy is enabled.'
    ),
    'relative_humidity_helper': (
        'Hungary daily relative_humidity is available from the current provider slice and may be used only by the existing shared '
        'allow-derived fallback rule for vapour_pressure. No provider-specific derivation logic is added.'
    ),
    'sunshine_duration_to_radiation': (
        'sunshine_duration uses observed HungaroMet daily sunshine duration only. '
        'The shared workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
}
HU_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['t'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['tx'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['tn'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {'raw_codes': ['fs'], 'selection_rule': None, 'status': 'observed'},
    'vapour_pressure': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': (
            'Not directly available from the current Hungary daily provider path. '
            'The shared workflow leaves this field empty by default and may fill it only through the existing opt-in shared fallback rule.'
        ),
    },
    'sunshine_duration': {'raw_codes': ['f'], 'selection_rule': None, 'status': 'observed'},
}
PL_ASSUMPTIONS = {
    'observed_inputs_only': (
        'The Poland branch packages only source-backed daily observations from the IMGW-PIB synop provider slice for later FAO-oriented processing. '
        'It does not compute FAO-56 ET0.'
    ),
    'wind_speed_availability': (
        'The current Poland synop daily provider path does not expose observed wind_speed for this shared workflow. '
        'The shared workflow leaves wind_speed empty instead of substituting another IMGW product or recomputing it from other resolutions.'
    ),
    'vapour_pressure_availability': (
        'The current Poland synop daily provider path does not expose observed vapour_pressure for this shared workflow. '
        'The shared workflow leaves vapour_pressure empty, and the existing allow-derived fallback cannot be used because this provider slice does not expose observed daily relative_humidity here.'
    ),
    'sunshine_duration_to_radiation': (
        'Observed IMGW daily sunshine_duration is exported as input only. '
        'This workflow does not derive solar radiation, net radiation, or extraterrestrial radiation.'
    ),
    'station_metadata_limits': (
        'The implemented official IMGW station list provides the canonical station identifiers and names used here, but not clean source-backed coordinates or elevation for this provider slice. '
        'Those metadata fields therefore remain missing in the normalized station table and exported FAO-prep bundle.'
    ),
}
PL_PROVIDER_ELEMENT_MAPPING = {
    'tas_mean': {'raw_codes': ['STD'], 'selection_rule': None, 'status': 'observed'},
    'tas_max': {'raw_codes': ['TMAX'], 'selection_rule': None, 'status': 'observed'},
    'tas_min': {'raw_codes': ['TMIN'], 'selection_rule': None, 'status': 'observed'},
    'wind_speed': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': 'Not directly available from the current Poland synop daily provider path. The shared workflow leaves this field empty instead of deriving it.',
    },
    'vapour_pressure': {
        'raw_codes': [],
        'selection_rule': None,
        'status': 'unavailable',
        'notes': (
            'Not directly available from the current Poland synop daily provider path. '
            'The shared workflow leaves this field empty, and the existing shared fallback cannot be used because relative_humidity is unavailable in this slice.'
        ),
    },
    'sunshine_duration': {'raw_codes': ['USL'], 'selection_rule': None, 'status': 'observed'},
}
PL_HOURLY_PROVIDER_ELEMENT_MAPPING = {
    'wind_speed': {
        'raw_codes': ['FWR'],
        'selection_rule': f'UTC daily arithmetic mean from hourly observations when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly values are available',
        'status': 'aggregated_hourly_opt_in',
        'notes': 'Opt-in only. Filled from official IMGW historical/1hour wind_speed observations; never used by default.',
    },
    'vapour_pressure': {
        'raw_codes': ['CPW'],
        'selection_rule': f'UTC daily arithmetic mean from hourly observations when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly values are available',
        'status': 'aggregated_hourly_opt_in',
        'notes': 'Opt-in only. Filled from official IMGW historical/1hour vapour_pressure observations; never used by default.',
    },
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
    hourly_dataset_scope: str | None = None
    hourly_resolution: str | None = None
    hourly_query_elements: tuple[str, ...] = ()


@dataclass(frozen=True)
class FieldFillSummary:
    field: str
    status: str
    rule: str
    observed_count: int
    aggregated_count: int
    derived_count: int
    missing_count: int


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

        config = shared_fao_config.get_fao_country_config(args.country, fill_missing=args.fill_missing)
        country_cache_dir = resolve_country_cache_dir(args.cache_dir, config.country)
        mat_output_path = resolve_mat_output_path(args.output, country=config.country)
        parquet_output_dir = resolve_parquet_output_dir(args.output_dir, country=config.country)
        export_timestamp = pd.Timestamp.now('UTC').isoformat()

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
        hourly_missing_station_ids: list[str] = []
        hourly_failed_station_ids: list[str] = []
        if fill_policy_uses_hourly_aggregate(args.fill_missing) and config.hourly_query_elements:
            hourly_available_station_count, hourly_missing_station_ids, hourly_failed_station_ids = cache_candidate_hourly_inputs(
                candidates[~candidates['station_id'].isin(set(missing_station_ids) | set(failed_station_ids))].reset_index(drop=True),
                cache_dir=country_cache_dir,
                config=config,
                mode=args.mode,
                timeout=args.timeout,
                reporter=reporter,
                stats=stats,
            )
            reporter.info(f'Hourly supplement cache ready for {hourly_available_station_count} station(s).')

        if args.mode == 'download':
            reporter.essential(f'Cached FAO inputs for {available_station_count} station(s) under {country_cache_dir}.')
            print_final_summary(reporter, stats)
            if missing_station_ids:
                reporter.essential(f'Stations with missing required daily inputs: {", ".join(missing_station_ids)}')
            if failed_station_ids:
                reporter.essential(f'Stations with failed downloads: {", ".join(failed_station_ids)}')
            if hourly_missing_station_ids:
                reporter.essential(f'Stations with missing optional hourly supplement inputs: {", ".join(hourly_missing_station_ids)}')
            if hourly_failed_station_ids:
                reporter.essential(f'Stations with failed optional hourly supplement downloads: {", ".join(hourly_failed_station_ids)}')
            return 0

        if args.mode == 'build' and (missing_station_ids or failed_station_ids or hourly_missing_station_ids or hourly_failed_station_ids):
            print_final_summary(reporter, stats)
            problem_ids = missing_station_ids + failed_station_ids + hourly_missing_station_ids + hourly_failed_station_ids
            raise CacheMissingError('Build mode requires a complete local cache. Missing or failed stations: ' + ', '.join(problem_ids))

        unavailable_station_ids = set(missing_station_ids) | set(failed_station_ids)
        retained_series: list[dict[str, Any]] = []
        station_rows: list[dict[str, Any]] = []
        provenance_tables: list[pd.DataFrame] = []
        applied_rules_by_field: dict[str, set[str]] = {field: set() for field in FINAL_SERIES_COLUMNS}

        build_candidates = candidates[~candidates['station_id'].isin(unavailable_station_ids)].reset_index(drop=True)
        total_build_candidates = len(build_candidates)
        reporter.info(f'Building cleaned dataset from cache for {total_build_candidates} station(s).')

        for index, station in enumerate(build_candidates.itertuples(index=False), start=1):
            reporter.info(f'[{index}/{total_build_candidates}] Processing {station.station_id} ({station.full_name}) from cache')
            daily_table = read_cached_daily_observations(station.station_id, cache_dir=country_cache_dir)
            hourly_table = (
                read_cached_hourly_observations(station.station_id, cache_dir=country_cache_dir)
                if fill_policy_uses_hourly_aggregate(args.fill_missing) and config.hourly_query_elements and station.station_id not in set(hourly_missing_station_ids) | set(hourly_failed_station_ids)
                else pd.DataFrame()
            )
            complete, provenance, applied_rules = prepare_complete_station_series_with_provenance(
                daily_table,
                hourly_table=hourly_table,
                config=config,
                fill_missing=args.fill_missing,
            )
            if complete.empty:
                reporter.info('  No complete FAO-prep days after country-specific selection, skipping.')
                continue
            if len(complete) < args.min_complete_days:
                reporter.info(f'  Only {len(complete)} complete days, below threshold {args.min_complete_days}.')
                continue

            retained_series.append(build_series_record(complete, station_id=station.station_id, full_name=station.full_name, latitude=station.latitude, longitude=station.longitude, elevation=station.elevation_m))
            provenance_tables.append(provenance)
            for field_name, rule in applied_rules.items():
                if rule:
                    applied_rules_by_field[field_name].add(rule)
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

        data_info = build_data_info(config, station_rows, min_complete_days=args.min_complete_days, fill_missing=args.fill_missing)
        field_summaries = summarize_field_fill_status(
            provenance_tables,
            fill_missing=args.fill_missing,
            applied_rules_by_field=applied_rules_by_field,
        )
        exported_targets: list[str] = []
        if args.export_format in {'mat', 'both'}:
            export_mat_bundle(mat_output_path, data_info=data_info, stations=station_rows, series=retained_series)
            write_info_sidecar(
                mat_output_path,
                country=config.country,
                export_timestamp=export_timestamp,
                fill_missing=args.fill_missing,
                field_summaries=field_summaries,
                requested_fields=FINAL_SERIES_COLUMNS,
                bundle_sections=('data_info', 'stations', 'series'),
            )
            exported_targets.append(str(mat_output_path))
        if args.export_format in {'parquet', 'both'}:
            export_parquet_bundle(parquet_output_dir, data_info=data_info, stations=station_rows, series=retained_series)
            write_info_sidecar(
                parquet_output_dir,
                country=config.country,
                export_timestamp=export_timestamp,
                fill_missing=args.fill_missing,
                field_summaries=field_summaries,
                requested_fields=FINAL_SERIES_COLUMNS,
                bundle_sections=('data_info.json', 'stations.parquet', 'series.parquet'),
            )
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
    parser.add_argument('--fill-missing', choices=shared_fao_config.FILL_MISSING_CHOICES, default='none', help='Keep missing FAO-oriented inputs empty, or explicitly allow the example layer to apply the documented opt-in fallback rules.')
    parser.add_argument('--timeout', type=int, default=60, help='HTTP timeout in seconds.')
    parser.add_argument('--silent', action='store_true', help='Suppress non-essential progress output.')
    return parser


def fill_policy_uses_derived(fill_missing: str) -> bool:
    return fill_missing == 'allow-derived'


def fill_policy_uses_hourly_aggregate(fill_missing: str) -> bool:
    return fill_missing == 'allow-hourly-aggregate'


def get_fao_country_config(country: str | None, *, fill_missing: str = 'none') -> FaoCountryConfig:
    normalized_country = normalize_country_code(country)
    try:
        provider = get_provider(normalized_country)
        daily_spec = provider.get_dataset_spec('historical_csv' if normalized_country == 'CZ' else 'historical', 'daily')
    except Exception as exc:
        raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.') from exc
    if fill_missing not in FILL_MISSING_CHOICES:
        raise ValueError(f'Unsupported fill policy: {fill_missing}')

    canonical_to_raw = daily_spec.canonical_elements or {}
    raw_to_provider_canonical = raw_to_canonical_map_for_spec(daily_spec)
    if normalized_country == 'AT':
        query_elements = AT_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'BE':
        query_elements = BE_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'CH':
        query_elements = FAO_CANONICAL_ELEMENTS
    elif normalized_country == 'DK':
        query_elements = DK_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'HU':
        query_elements = HU_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'PL':
        query_elements = PL_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'NL':
        query_elements = NL_REQUIRED_OBSERVED_ELEMENTS
    elif normalized_country == 'SE':
        query_elements = SE_REQUIRED_OBSERVED_ELEMENTS
    else:
        query_elements = FAO_CANONICAL_ELEMENTS
    query_elements = tuple(query_elements)
    if fill_missing == 'allow-derived' and normalized_country in {'AT', 'BE', 'CH', 'DK', 'HU', 'NL'}:
        query_elements = tuple(dict.fromkeys([*query_elements, 'relative_humidity']))

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
        return FaoCountryConfig('CZ', 'historical_csv', 'daily', ('DLY',), selected_canonical_to_raw, raw_to_canonical, dict(CZ_TIMEFUNC_BY_CANONICAL), FAO_CANONICAL_ELEMENTS, query_elements, build_observed_provider_element_mapping(selected_canonical_to_raw, dict(CZ_TIMEFUNC_BY_CANONICAL)), {}, 'CHMI observed daily input bundle prepared for later FAO workflow packaging', 'CHMI OpenData historical_csv metadata and daily observations')
    if normalized_country == 'DE':
        return FaoCountryConfig('DE', 'historical', 'daily', ('DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, FAO_CANONICAL_ELEMENTS, query_elements, build_observed_provider_element_mapping(selected_canonical_to_raw, {}), {}, 'DWD observed daily input bundle prepared for later FAO workflow packaging', 'DWD CDC historical daily metadata and observations')
    if normalized_country == 'AT':
        return FaoCountryConfig('AT', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, AT_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(AT_PROVIDER_ELEMENT_MAPPING), dict(AT_ASSUMPTIONS), 'GeoSphere Austria observed daily input bundle prepared for later FAO workflow packaging', 'GeoSphere Austria Dataset API station historical daily klima-v2-1d')
    if normalized_country == 'BE':
        return FaoCountryConfig('BE', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, BE_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(BE_PROVIDER_ELEMENT_MAPPING), dict(BE_ASSUMPTIONS), 'RMI/KMI Belgium observed daily input bundle prepared for later FAO workflow packaging', 'RMI/KMI open-data platform aws_1day daily station observations')
    if normalized_country == 'CH':
        return FaoCountryConfig('CH', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, CH_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(CH_PROVIDER_ELEMENT_MAPPING), dict(CH_ASSUMPTIONS), 'MeteoSwiss Switzerland observed daily input bundle prepared for later FAO workflow packaging', 'MeteoSwiss A1 automatic weather stations historical daily observations')
    if normalized_country == 'DK':
        return FaoCountryConfig('DK', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, DK_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(DK_PROVIDER_ELEMENT_MAPPING), dict(DK_ASSUMPTIONS), 'DMI Denmark observed daily input bundle prepared for later FAO workflow packaging', 'DMI Climate Data station and stationValue daily station observations for Denmark')
    if normalized_country == 'HU':
        return FaoCountryConfig('HU', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, HU_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(HU_PROVIDER_ELEMENT_MAPPING), dict(HU_ASSUMPTIONS), 'HungaroMet Hungary observed daily input bundle prepared for later FAO workflow packaging', 'HungaroMet open data daily station observations from odp.met.hu')
    if normalized_country == 'PL':
        provider_mapping = dict(PL_PROVIDER_ELEMENT_MAPPING)
        assumptions = dict(PL_ASSUMPTIONS)
        dataset_type = 'IMGW-PIB Poland observed daily input bundle prepared for later FAO workflow packaging'
        source = 'IMGW-PIB public daily synop station observations from the official archive'
        hourly_query_elements: tuple[str, ...] = ()
        if fill_missing == 'allow-hourly-aggregate':
            provider_mapping = {
                **provider_mapping,
                'wind_speed': dict(PL_HOURLY_PROVIDER_ELEMENT_MAPPING['wind_speed']),
                'vapour_pressure': dict(PL_HOURLY_PROVIDER_ELEMENT_MAPPING['vapour_pressure']),
            }
            assumptions.update(
                {
                    'hourly_supplement_policy': (
                        'The explicit allow-hourly-aggregate policy may supplement missing daily wind_speed and vapour_pressure from official IMGW historical/1hour synop observations. '
                        'This remains a daily FAO-input-preparation workflow and does not compute FAO-56 ET0.'
                    ),
                    'hourly_aggregation_boundary': (
                        'Poland hourly supplementation groups normalized IMGW historical/1hour timestamps by UTC calendar day in this first workflow slice.'
                    ),
                    'hourly_aggregation_threshold': (
                        f'Daily hourly supplements are filled only when at least {PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS} hourly observations are available for that day; otherwise the field stays missing.'
                    ),
                }
            )
            dataset_type = 'IMGW-PIB Poland daily input bundle with opt-in hourly supplementation prepared for later FAO workflow packaging'
            source = 'IMGW-PIB public daily synop station observations plus opt-in official historical/1hour synop supplementation from the official archive'
            hourly_query_elements = ('wind_speed', 'vapour_pressure')
        return FaoCountryConfig('PL', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, PL_REQUIRED_OBSERVED_ELEMENTS, query_elements, provider_mapping, assumptions, dataset_type, source, 'historical' if hourly_query_elements else None, '1hour' if hourly_query_elements else None, hourly_query_elements)
    if normalized_country == 'NL':
        return FaoCountryConfig('NL', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, NL_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(NL_PROVIDER_ELEMENT_MAPPING), dict(NL_ASSUMPTIONS), 'KNMI observed daily input bundle prepared for later FAO workflow packaging', 'KNMI Open Data API validated daily in-situ meteorological observations')
    if normalized_country == 'SE':
        return FaoCountryConfig('SE', 'historical', 'daily', ('HISTORICAL_DAILY',), selected_canonical_to_raw, raw_to_canonical, {}, SE_REQUIRED_OBSERVED_ELEMENTS, query_elements, dict(SE_PROVIDER_ELEMENT_MAPPING), dict(SE_ASSUMPTIONS), 'SMHI Sweden observed daily input bundle prepared for later FAO workflow packaging', 'SMHI Meteorological Observations corrected-archive daily station observations')
    raise ValueError(f'FAO preparation example is not implemented for country {normalized_country}.')


def build_observed_provider_element_mapping(canonical_to_raw: dict[str, tuple[str, ...]], time_function_by_canonical: dict[str, str]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for canonical_name in FINAL_SERIES_COLUMNS:
        mapping[canonical_name] = {'raw_codes': list(canonical_to_raw[canonical_name]), 'selection_rule': time_function_by_canonical.get(canonical_name), 'status': 'observed'}
    return mapping


def build_data_info(config: FaoCountryConfig, station_rows: list[dict[str, Any]], *, min_complete_days: int, fill_missing: str = 'none') -> dict[str, Any]:
    data_info = {
        'created_at': pd.Timestamp.now('UTC').isoformat(),
        'dataset_type': config.dataset_type,
        'source': config.source,
        'country': config.country,
        'elements': FINAL_SERIES_COLUMNS,
        'provider_element_mapping': build_provider_element_mapping(config),
        'min_complete_days': int(min_complete_days),
        'num_stations': int(len(station_rows)),
        'fill_policy': {'selected': fill_missing},
    }
    if config.assumptions:
        data_info['assumptions'] = dict(config.assumptions)
    if fill_missing == 'allow-hourly-aggregate' and config.country == 'PL':
        data_info['fill_policy'].update(
            {
                'hourly_aggregation_day_boundary': 'UTC calendar day based on normalized historical/1hour timestamps',
                'hourly_aggregation_min_observations': int(PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS),
                'hourly_aggregation_fields': ['wind_speed', 'vapour_pressure'],
            }
        )
    return data_info


def resolve_info_sidecar_path(output_path: Path) -> Path:
    if output_path.suffix:
        return output_path.with_suffix('.info')
    return output_path.with_name(output_path.name + '.info')


def summarize_field_fill_status(
    provenance_tables: list[pd.DataFrame],
    *,
    fill_missing: str,
    applied_rules_by_field: dict[str, set[str]],
) -> list[FieldFillSummary]:
    summaries: list[FieldFillSummary] = []
    for field_name in FINAL_SERIES_COLUMNS:
        observed_count = 0
        aggregated_count = 0
        derived_count = 0
        missing_count = 0
        for provenance in provenance_tables:
            if field_name not in provenance.columns:
                continue
            counts = provenance[field_name].astype('string').value_counts(dropna=False)
            observed_count += int(counts.get('observed_daily', 0))
            aggregated_count += int(counts.get('aggregated_hourly_opt_in', 0))
            derived_count += int(counts.get('derived_opt_in', 0))
            missing_count += int(counts.get('missing', 0))
        if aggregated_count > 0 and (observed_count > 0 or derived_count > 0):
            status = 'partially hourly-aggregated'
        elif aggregated_count > 0:
            status = 'hourly-aggregated opt-in'
        elif derived_count > 0 and observed_count > 0:
            status = 'partially derived'
        elif derived_count > 0:
            status = 'fully derived'
        elif observed_count > 0:
            status = 'observed-only'
        else:
            status = 'still missing'
        if applied_rules_by_field.get(field_name):
            rule = '; '.join(sorted(applied_rules_by_field[field_name]))
        elif fill_missing == 'allow-derived' and field_name == 'vapour_pressure':
            rule = 'No fill rule applied.'
        elif fill_missing == 'allow-hourly-aggregate' and field_name in {'wind_speed', 'vapour_pressure'}:
            rule = 'No hourly aggregation rule applied.'
        elif observed_count > 0:
            rule = 'Observed daily source values only.'
        else:
            rule = 'No fill rule applied.'
        summaries.append(
            FieldFillSummary(
                field=field_name,
                status=status,
                rule=rule,
                observed_count=observed_count,
                aggregated_count=aggregated_count,
                derived_count=derived_count,
                missing_count=missing_count,
            )
        )
    return summaries


def _describe_fill_mode(fill_missing: str) -> str:
    if fill_missing == 'none':
        return 'observed-only'
    if fill_missing == 'allow-derived':
        return 'allowed derived values'
    if fill_missing == 'allow-hourly-aggregate':
        return 'allowed hourly aggregation values'
    return fill_missing


def render_info_sidecar_text(
    *,
    output_path: Path,
    country: str,
    export_timestamp: str,
    fill_missing: str,
    bundle_sections: tuple[str, ...],
    requested_fields: list[str] | tuple[str, ...],
    field_summaries: list[FieldFillSummary],
) -> str:
    lines = [
        'WeatherDownload FAO-oriented input export info',
        '',
        f'Export timestamp: {export_timestamp}',
        f'Output path: {output_path.resolve()}',
        f'Country: {country}',
        f'Selected fill policy: {fill_missing}',
        f'Run mode: {_describe_fill_mode(fill_missing)}',
        f'Bundle sections exported: {", ".join(bundle_sections)}',
        f'Requested FAO-oriented fields: {", ".join(requested_fields)}',
        'ET0 computation: not performed',
    ]
    if any(summary.aggregated_count > 0 or summary.derived_count > 0 for summary in field_summaries):
        lines.append('Warning: This export contains opt-in hourly-aggregated and/or derived values from the example-layer fill policy.')
    else:
        lines.append('Warning: No hourly-aggregated or derived values were used in this export.')
    lines.extend(['', 'Field summary:'])
    for summary in field_summaries:
        lines.extend(
            [
                f'- {summary.field}',
                f'  status: {summary.status}',
                f'  rule: {summary.rule}',
                f'  observed values: {summary.observed_count}',
                f'  hourly-aggregated values: {summary.aggregated_count}',
                f'  derived values: {summary.derived_count}',
                f'  missing values: {summary.missing_count}',
            ]
        )
    return '\n'.join(lines) + '\n'


def write_info_sidecar(
    output_path: Path,
    *,
    country: str,
    export_timestamp: str,
    fill_missing: str,
    bundle_sections: tuple[str, ...],
    requested_fields: list[str] | tuple[str, ...],
    field_summaries: list[FieldFillSummary],
) -> Path:
    info_path = resolve_info_sidecar_path(output_path)
    info_path.parent.mkdir(parents=True, exist_ok=True)
    text = render_info_sidecar_text(
        output_path=output_path,
        country=country,
        export_timestamp=export_timestamp,
        fill_missing=fill_missing,
        bundle_sections=bundle_sections,
        requested_fields=requested_fields,
        field_summaries=field_summaries,
    )
    if info_path.exists():
        existing = info_path.read_text(encoding='utf-8').rstrip()
        text = existing + '\n\n---\n\n' + text
    info_path.write_text(text, encoding='utf-8')
    return info_path

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


def cache_candidate_hourly_inputs(candidates: pd.DataFrame, *, cache_dir: Path, config: FaoCountryConfig, mode: str, timeout: int, reporter: ProgressReporter, stats: CacheStats) -> tuple[int, list[str], list[str]]:
    available_station_count = 0
    missing_station_ids: list[str] = []
    failed_station_ids: list[str] = []
    total_candidates = len(candidates)
    reporter.info(f'Checking hourly supplement cache for {total_candidates} station(s).')
    for index, station in enumerate(candidates.itertuples(index=False), start=1):
        result = ensure_hourly_observations_cached(station.station_id, cache_dir=cache_dir, config=config, mode=mode, timeout=timeout, stats=stats)
        if result.available:
            available_station_count += 1
        if result.missing:
            missing_station_ids.append(station.station_id)
        if result.failed:
            failed_station_ids.append(station.station_id)
        reporter.info(f'[{index}/{total_candidates}] hourly supplement {station.station_id} ({station.full_name}): {result.summary()}')
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


def ensure_hourly_observations_cached(station_id: str, *, cache_dir: Path, config: FaoCountryConfig, mode: str, timeout: int, stats: CacheStats) -> StationCacheResult:
    result = StationCacheResult(station_id)
    cache_path = cached_hourly_observations_path(cache_dir, station_id)
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
            dataset_scope=config.hourly_dataset_scope,
            resolution=config.hourly_resolution,
            station_ids=[station_id],
            all_history=True,
            elements=list(config.hourly_query_elements),
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


def read_cached_hourly_observations(station_id: str, *, cache_dir: Path) -> pd.DataFrame:
    cache_path = cached_hourly_observations_path(cache_dir, station_id)
    if not cache_path.exists():
        raise CacheMissingError(f'Missing cached hourly observations for station {station_id}: {cache_path}')
    table = pd.read_csv(cache_path, dtype={'flag': 'string'})
    table['timestamp'] = pd.to_datetime(table['timestamp'], utc=True, errors='coerce')
    return table


def cached_daily_observations_path(cache_dir: Path, station_id: str) -> Path:
    return cache_dir / 'daily' / station_id / f'daily-{station_id}.csv'


def cached_hourly_observations_path(cache_dir: Path, station_id: str) -> Path:
    return cache_dir / 'hourly' / station_id / f'hourly-{station_id}.csv'


def prepare_complete_station_series(daily_table: pd.DataFrame, *, config: FaoCountryConfig, fill_missing: str = 'none') -> pd.DataFrame:
    complete, _, _ = prepare_complete_station_series_with_provenance(
        daily_table,
        hourly_table=None,
        config=config,
        fill_missing=fill_missing,
    )
    return complete


def prepare_complete_station_series_with_provenance(
    daily_table: pd.DataFrame,
    *,
    hourly_table: pd.DataFrame | None = None,
    config: FaoCountryConfig,
    fill_missing: str = 'none',
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, str | None]]:
    selected_tables: dict[str, pd.DataFrame] = {}
    selected_names = set(config.required_complete_elements) | set(FINAL_SERIES_COLUMNS)
    if fill_policy_uses_derived(fill_missing):
        selected_names.add('relative_humidity')
    for canonical_name in selected_names:
        selected = select_daily_variable_rows(daily_table, canonical_name=canonical_name, config=config)
        if canonical_name in config.required_complete_elements and selected.empty:
            empty = pd.DataFrame(columns=['date', *FINAL_SERIES_COLUMNS])
            empty_provenance = pd.DataFrame(columns=['date', *FINAL_SERIES_COLUMNS])
            return empty, empty_provenance, {field: None for field in FINAL_SERIES_COLUMNS}
        if not selected.empty:
            selected_tables[canonical_name] = selected

    merged = selected_tables[config.required_complete_elements[0]]
    for canonical_name in config.required_complete_elements[1:]:
        merged = merged.merge(selected_tables[canonical_name], on='date', how='inner')

    for canonical_name in (set(FINAL_SERIES_COLUMNS) | {'relative_humidity'}) - set(config.required_complete_elements):
        table = selected_tables.get(canonical_name)
        if table is not None:
            merged = merged.merge(table, on='date', how='left')

    for canonical_name in set(FINAL_SERIES_COLUMNS) | {'relative_humidity'}:
        if canonical_name not in merged.columns:
            merged[canonical_name] = pd.NA

    complete = merged.dropna(subset=list(config.required_complete_elements)).sort_values('date').reset_index(drop=True)
    provenance = pd.DataFrame({'date': complete['date']})
    for canonical_name in FINAL_SERIES_COLUMNS:
        provenance[canonical_name] = pd.Series(
            np.where(complete[canonical_name].notna(), 'observed_daily', 'missing'),
            dtype='string',
        )

    applied_rules = {field: None for field in FINAL_SERIES_COLUMNS}
    if fill_policy_uses_hourly_aggregate(fill_missing) and config.country == 'PL':
        hourly_fills = build_hourly_daily_fill_tables(hourly_table, fields=('wind_speed', 'vapour_pressure'))
        for field_name, rule_text in (
            ('wind_speed', PL_HOURLY_WIND_SPEED_RULE_DESCRIPTION),
            ('vapour_pressure', PL_HOURLY_VAPOUR_PRESSURE_RULE_DESCRIPTION),
        ):
            fill_table = hourly_fills.get(field_name)
            if fill_table is None or fill_table.empty:
                continue
            complete = complete.merge(fill_table, on='date', how='left')
            fill_column = f'{field_name}__hourly_fill'
            fill_mask = complete[field_name].isna() & complete[fill_column].notna()
            if fill_mask.any():
                complete.loc[fill_mask, field_name] = complete.loc[fill_mask, fill_column]
                provenance.loc[fill_mask, field_name] = 'aggregated_hourly_opt_in'
                applied_rules[field_name] = rule_text
            complete = complete.drop(columns=[fill_column])

    if fill_policy_uses_derived(fill_missing):
        vapour_pressure_mask = (
            complete['vapour_pressure'].isna()
            & complete['tas_mean'].notna()
            & complete['relative_humidity'].notna()
        )
        if vapour_pressure_mask.any():
            relative_humidity = pd.to_numeric(complete.loc[vapour_pressure_mask, 'relative_humidity'], errors='coerce')
            valid_mask = relative_humidity.between(0, 100, inclusive='both')
            if valid_mask.any():
                target_index = relative_humidity[valid_mask].index
                tas_mean = pd.to_numeric(complete.loc[target_index, 'tas_mean'], errors='coerce')
                complete.loc[target_index, 'vapour_pressure'] = 6.108 * np.exp((17.27 * tas_mean) / (tas_mean + 237.3)) * (relative_humidity.loc[target_index] / 100.0)
                provenance.loc[target_index, 'vapour_pressure'] = 'derived_opt_in'
                applied_rules['vapour_pressure'] = DERIVED_VAPOUR_PRESSURE_RULE_DESCRIPTION

    return complete.loc[:, ['date', *FINAL_SERIES_COLUMNS]], provenance.loc[:, ['date', *FINAL_SERIES_COLUMNS]], applied_rules


def build_hourly_daily_fill_tables(hourly_table: pd.DataFrame | None, *, fields: tuple[str, ...]) -> dict[str, pd.DataFrame]:
    if hourly_table is None or hourly_table.empty:
        return {}
    tables: dict[str, pd.DataFrame] = {}
    for field_name in fields:
        aggregated = aggregate_hourly_field_to_daily(hourly_table, field_name)
        if aggregated is not None and not aggregated.empty:
            tables[field_name] = aggregated
    return tables


def aggregate_hourly_field_to_daily(hourly_table: pd.DataFrame, canonical_name: str) -> pd.DataFrame | None:
    filtered = hourly_table[hourly_table['element'].astype(str) == canonical_name].copy()
    if filtered.empty:
        return None
    filtered['timestamp'] = pd.to_datetime(filtered['timestamp'], utc=True, errors='coerce')
    filtered = filtered[filtered['timestamp'].notna()]
    if filtered.empty:
        return None
    filtered['date'] = filtered['timestamp'].dt.date
    filtered['numeric_value'] = pd.to_numeric(filtered['value'], errors='coerce')
    filtered = filtered.dropna(subset=['numeric_value'])
    if filtered.empty:
        return None
    aggregated = (
        filtered.groupby('date', as_index=False)
        .agg(hourly_count=('numeric_value', 'count'), hourly_mean=('numeric_value', 'mean'))
    )
    aggregated = aggregated[aggregated['hourly_count'] >= PL_HOURLY_AGGREGATION_MIN_OBSERVATIONS].copy()
    if aggregated.empty:
        return None
    return aggregated.rename(columns={'hourly_mean': f'{canonical_name}__hourly_fill'})[['date', f'{canonical_name}__hourly_fill']]


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


# Reuse the shared library-facing FAO helpers while keeping the example module's
# long-standing import surface stable for tests and scripts.
FieldFillSummary = shared_fao.FieldFillSummary
fill_policy_uses_derived = shared_fao.fill_policy_uses_derived
fill_policy_uses_hourly_aggregate = shared_fao.fill_policy_uses_hourly_aggregate
build_provider_element_mapping = shared_fao.build_provider_element_mapping
build_data_info = shared_fao.build_data_info
summarize_field_fill_status = shared_fao.summarize_field_fill_status
prepare_complete_station_series = shared_fao.prepare_complete_station_series
prepare_complete_station_series_with_provenance = shared_fao.prepare_complete_station_series_with_provenance
build_hourly_daily_fill_tables = shared_fao.build_hourly_daily_fill_tables
aggregate_hourly_field_to_daily = shared_fao.aggregate_hourly_field_to_daily
select_daily_variable_rows = shared_fao.select_daily_variable_rows
build_series_record = shared_fao.build_series_record
build_station_table = shared_fao.build_station_table
build_series_table = shared_fao.build_series_table
FaoCountryConfig = shared_fao_config.FaoCountryConfig
FILL_MISSING_CHOICES = shared_fao_config.FILL_MISSING_CHOICES
get_fao_country_config = shared_fao_config.get_fao_country_config
build_observed_provider_element_mapping = shared_fao_config.build_observed_provider_element_mapping


if __name__ == '__main__':
    raise SystemExit(main())
