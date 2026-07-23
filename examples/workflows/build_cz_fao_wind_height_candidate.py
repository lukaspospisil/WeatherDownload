from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from weatherdownload import export_table
from weatherdownload.cz_wind_height import (
    FALLBACK_HEIGHT_SOURCE,
    METADATA_HEIGHT_SOURCE,
    audit_and_standardize_cz_daily_wind,
    build_cz_daily_wind_height_lookup,
    collapse_issue_intervals,
)

from examples.workflows import download_fao


ARTICLE_START_DATE = pd.Timestamp('2000-01-01').date()
ARTICLE_END_DATE = pd.Timestamp('2025-12-31').date()
DEFAULT_OUTPUT_DIR = Path('outputs/fao_daily.cz.wind_height_candidate')
DEFAULT_MAT_OUTPUT = Path('outputs/fao_daily.cz.wind_height_candidate.mat')
DEFAULT_AUDIT_CSV = Path('outputs/fao_daily.cz.wind_height_audit.csv')
DEFAULT_SUMMARY_JSON = Path('outputs/fao_daily.cz.wind_height_audit_summary.json')
DEFAULT_MIN_COMPLETE_DAYS = 3650
DEFAULT_FALLBACK_HEIGHT_M = 10.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Build a corrected CZ FAO-prep candidate export with station/date-specific CHMI daily wind heights.'
    )
    parser.add_argument('--cache-dir', type=Path, default=Path('outputs/fao_cache'))
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument('--mat-output', type=Path, default=DEFAULT_MAT_OUTPUT)
    parser.add_argument('--audit-csv', type=Path, default=DEFAULT_AUDIT_CSV)
    parser.add_argument('--summary-json', type=Path, default=DEFAULT_SUMMARY_JSON)
    parser.add_argument('--min-complete-days', type=int, default=DEFAULT_MIN_COMPLETE_DAYS)
    parser.add_argument('--fallback-height-m', type=float, default=DEFAULT_FALLBACK_HEIGHT_M)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = download_fao.get_fao_country_config('CZ', fill_missing='none')
    country_cache_dir = download_fao.resolve_country_cache_dir(args.cache_dir, 'CZ')

    meta1 = pd.read_csv(country_cache_dir / 'meta1.csv')
    meta2 = pd.read_csv(country_cache_dir / 'meta2.csv')
    candidates = download_fao.screen_candidate_stations(
        meta1,
        meta2,
        config=config,
        min_complete_days=args.min_complete_days,
    )

    complete_tables: list[pd.DataFrame] = []
    station_rows: list[dict[str, Any]] = []
    for station in candidates.itertuples(index=False):
        daily_table = download_fao.read_cached_daily_observations(station.station_id, cache_dir=country_cache_dir)
        complete, _provenance, _applied_rules = download_fao.prepare_complete_station_series_with_provenance(
            daily_table,
            config=config,
            fill_missing='none',
        )
        if complete.empty:
            continue
        # Match the original download_fao.py protocol: apply the minimum
        # complete-day threshold to the full available station history,
        # then restrict the retained rows to the article period.  Do not
        # impose the threshold a second time inside 2000--2025.
        if len(complete) < args.min_complete_days:
            continue
        in_window = complete['date'].between(ARTICLE_START_DATE, ARTICLE_END_DATE, inclusive='both')
        complete = complete.loc[in_window].copy()
        if complete.empty:
            continue
        complete.insert(0, 'station_id', station.station_id)
        complete.insert(1, 'full_name', station.full_name)
        complete.insert(2, 'latitude', station.latitude)
        complete.insert(3, 'longitude', station.longitude)
        complete.insert(4, 'elevation_m', station.elevation_m)
        complete_tables.append(complete)
        station_rows.append(
            {
                'station_id': station.station_id,
                'full_name': station.full_name,
                'latitude': station.latitude,
                'longitude': station.longitude,
                'elevation_m': station.elevation_m,
                'num_complete_days': int(len(complete)),
                'first_complete_date': complete['date'].min().isoformat(),
                'last_complete_date': complete['date'].max().isoformat(),
            }
        )

    if not complete_tables:
        raise RuntimeError('No CZ station series remained after article-window filtering.')

    dataset = pd.concat(complete_tables, ignore_index=True, sort=False)
    wind_lookup = build_cz_daily_wind_height_lookup(meta2)
    wind_audit = audit_and_standardize_cz_daily_wind(
        dataset[['station_id', 'date', 'wind_speed']].copy(),
        wind_lookup,
        fallback_height_m=args.fallback_height_m,
    )

    candidate = dataset.merge(
        wind_audit[
            [
                'station_id',
                'date',
                'matched_metadata_rows',
                'matched_zero_height',
                'matched_missing_height',
                'matched_height_m',
                'matched_height_conflict',
                'wind_measurement_height_m',
                'wind_height_source',
                'wind_height_issue',
                'u2_fixed10_m_s',
                'u2_m_s',
                'u2_abs_diff_m_s',
                'u2_rel_diff',
            ]
        ],
        on=['station_id', 'date'],
        how='left',
        validate='1:1',
    )
    candidate['wind_speed_raw_m_s'] = pd.to_numeric(candidate['wind_speed'], errors='coerce')

    summary = build_summary(candidate, fallback_height_m=args.fallback_height_m)
    issue_intervals = collapse_issue_intervals(
        candidate,
        issue_mask=(
            candidate['wind_height_issue'].fillna('').astype(str).ne('')
            | candidate['matched_zero_height'].fillna(False)
            | candidate['matched_metadata_rows'].fillna(0).gt(1)
        ),
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    export_table(pd.DataFrame.from_records(station_rows), args.output_dir / 'stations.parquet', format='parquet')
    export_table(candidate, args.output_dir / 'series.parquet', format='parquet')
    (args.output_dir / 'data_info.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    download_fao.export_mat_bundle(
        args.mat_output,
        data_info=summary,
        stations=station_rows,
        series=build_candidate_series_records(candidate, station_rows=station_rows),
    )

    audit_export = candidate[
        [
            'station_id',
            'date',
            'wind_speed_raw_m_s',
            'matched_metadata_rows',
            'matched_height_m',
            'matched_zero_height',
            'matched_missing_height',
            'matched_height_conflict',
            'wind_measurement_height_m',
            'wind_height_source',
            'wind_height_issue',
            'u2_fixed10_m_s',
            'u2_m_s',
            'u2_abs_diff_m_s',
            'u2_rel_diff',
        ]
    ].copy()
    export_table(audit_export, args.audit_csv, format='csv')
    if not issue_intervals.empty:
        export_table(issue_intervals, args.audit_csv.with_name(args.audit_csv.stem + '.issues.csv'), format='csv')
    args.summary_json.write_text(json.dumps(summary, indent=2), encoding='utf-8')

    print(f"Exact CZ article stations: {summary['num_stations']}")
    print(f"Exact CZ article wind observations: {summary['num_wind_observations']}")
    print('Matched heights (count, pct):')
    for item in summary['matched_height_distribution']:
        print(f"  {item['height_m']}: {item['count']} ({item['pct']:.4f}%)")
    print(f"HEIGHT=10 observations: {summary['height_10_count']} ({summary['height_10_pct']:.4f}%)")
    print(f"HEIGHT!=10 observations: {summary['height_non10_count']} ({summary['height_non10_pct']:.4f}%)")
    print(f"HEIGHT=0 observations: {summary['height_zero_count']} ({summary['height_zero_pct']:.4f}%)")
    print(f"No matching metadata: {summary['no_metadata_count']} ({summary['no_metadata_pct']:.4f}%)")
    print(f"Matched to >1 metadata interval: {summary['overlap_count']} ({summary['overlap_pct']:.4f}%)")
    print(f"Fallback 10 m rows: {summary['fallback_count']} ({summary['fallback_pct']:.4f}%)")
    print(
        'U2 comparison: '
        f"mean_abs_diff={summary['u2_comparison']['mean_abs_diff_m_s']:.8f}, "
        f"max_abs_diff={summary['u2_comparison']['max_abs_diff_m_s']:.8f}, "
        f"mean_rel_diff={summary['u2_comparison']['mean_rel_diff']:.8f}, "
        f"max_rel_diff={summary['u2_comparison']['max_rel_diff']:.8f}"
    )
    print(f'Candidate export: {args.output_dir}')
    print(f'MAT export: {args.mat_output}')
    print(f'Audit CSV: {args.audit_csv}')
    print(f'Summary JSON: {args.summary_json}')
    return 0


def build_summary(candidate: pd.DataFrame, *, fallback_height_m: float) -> dict[str, Any]:
    total = int(len(candidate))
    matched_height = pd.to_numeric(candidate['matched_height_m'], errors='coerce')
    matched_distribution = (
        candidate.loc[matched_height.notna(), ['matched_height_m']]
        .assign(matched_height_m=lambda frame: pd.to_numeric(frame['matched_height_m'], errors='coerce'))
        .groupby('matched_height_m', dropna=False)
        .size()
        .reset_index(name='count')
        .sort_values('matched_height_m')
    )
    rel_diff = pd.to_numeric(candidate['u2_rel_diff'], errors='coerce')
    valid_rel_diff = rel_diff[np.isfinite(rel_diff)]
    abs_diff = pd.to_numeric(candidate['u2_abs_diff_m_s'], errors='coerce')

    issue_rows = candidate[
        candidate['wind_height_issue'].fillna('').astype(str).ne('')
        | candidate['matched_zero_height'].fillna(False)
        | candidate['matched_metadata_rows'].fillna(0).gt(1)
    ].copy()

    return {
        'country': 'CZ',
        'dataset_type': 'Corrected CZ FAO-prep candidate with station/date-specific CHMI daily wind heights',
        'source': 'CHMI OpenData historical_csv cached daily observations and cached meta2 metadata',
        'analysis_period': {
            'start_date': ARTICLE_START_DATE.isoformat(),
            'end_date': ARTICLE_END_DATE.isoformat(),
        },
        'fallback_policy': {
            'height_m': float(fallback_height_m),
            'rule': 'Use fallback_10m only when no non-zero metadata height can be resolved for a station/day.',
        },
        'height_zero_policy': (
            'CHMI metadata documentation identifies HEIGHT as a meta2 field but does not define HEIGHT=0 as a physical sensor height. '
            'This audit therefore treats HEIGHT=0 as unresolved metadata and never passes 0 into the logarithmic wind conversion.'
        ),
        'num_stations': int(candidate['station_id'].nunique()),
        'num_wind_observations': total,
        'matched_height_distribution': [
            {
                'height_m': float(row.matched_height_m),
                'count': int(row.count),
                'pct': _pct(int(row.count), total),
            }
            for row in matched_distribution.itertuples(index=False)
        ],
        'unique_matched_heights_m': [
            float(value)
            for value in matched_distribution['matched_height_m'].tolist()
        ],
        'height_10_count': int(matched_height.eq(10.0).sum()),
        'height_10_pct': _pct(int(matched_height.eq(10.0).sum()), total),
        'height_non10_count': int(matched_height.notna().sum() - matched_height.eq(10.0).sum()),
        'height_non10_pct': _pct(int(matched_height.notna().sum() - matched_height.eq(10.0).sum()), total),
        'height_zero_count': int(candidate['matched_zero_height'].sum()),
        'height_zero_pct': _pct(int(candidate['matched_zero_height'].sum()), total),
        'no_metadata_count': int(candidate['matched_metadata_rows'].eq(0).sum()),
        'no_metadata_pct': _pct(int(candidate['matched_metadata_rows'].eq(0).sum()), total),
        'overlap_count': int(candidate['matched_metadata_rows'].gt(1).sum()),
        'overlap_pct': _pct(int(candidate['matched_metadata_rows'].gt(1).sum()), total),
        'fallback_count': int(candidate['wind_height_source'].eq(FALLBACK_HEIGHT_SOURCE).sum()),
        'fallback_pct': _pct(int(candidate['wind_height_source'].eq(FALLBACK_HEIGHT_SOURCE).sum()), total),
        'u2_comparison': {
            'mean_abs_diff_m_s': float(abs_diff.mean(skipna=True)),
            'max_abs_diff_m_s': float(abs_diff.max(skipna=True)),
            'mean_rel_diff': float(valid_rel_diff.mean()) if len(valid_rel_diff) else float('nan'),
            'max_rel_diff': float(valid_rel_diff.max()) if len(valid_rel_diff) else float('nan'),
            'rel_diff_percentiles': {
                'p50': float(np.nanpercentile(valid_rel_diff, 50)) if len(valid_rel_diff) else float('nan'),
                'p90': float(np.nanpercentile(valid_rel_diff, 90)) if len(valid_rel_diff) else float('nan'),
                'p95': float(np.nanpercentile(valid_rel_diff, 95)) if len(valid_rel_diff) else float('nan'),
                'p99': float(np.nanpercentile(valid_rel_diff, 99)) if len(valid_rel_diff) else float('nan'),
            },
        },
        'issue_station_intervals': (
            issue_rows[['station_id', 'date', 'wind_height_issue']]
            .sort_values(['station_id', 'date'], kind='stable')
            .head(200)
            .assign(date=lambda frame: frame['date'].astype(str))
            .to_dict(orient='records')
        ),
    }


def _pct(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return 100.0 * count / total


def build_candidate_series_records(candidate: pd.DataFrame, *, station_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if candidate.empty:
        return []

    station_lookup = {
        str(row['station_id']): row
        for row in station_rows
    }
    series_records: list[dict[str, Any]] = []
    for station_id, station_table in candidate.groupby('station_id', sort=True, dropna=False):
        station_meta = station_lookup[str(station_id)]
        ordered = station_table.sort_values('date', kind='stable').reset_index(drop=True)
        record: dict[str, Any] = {
            'station_id': station_meta['station_id'],
            'full_name': station_meta['full_name'],
            'latitude': station_meta['latitude'],
            'longitude': station_meta['longitude'],
            'elevation_m': station_meta['elevation_m'],
            'date': [value.isoformat() for value in ordered['date'].tolist()],
            'tas_mean': pd.to_numeric(ordered['tas_mean'], errors='coerce').tolist(),
            'tas_max': pd.to_numeric(ordered['tas_max'], errors='coerce').tolist(),
            'tas_min': pd.to_numeric(ordered['tas_min'], errors='coerce').tolist(),
            'wind_speed': pd.to_numeric(ordered['wind_speed'], errors='coerce').tolist(),
            'wind_speed_raw_m_s': pd.to_numeric(ordered['wind_speed_raw_m_s'], errors='coerce').tolist(),
            'vapour_pressure': pd.to_numeric(ordered['vapour_pressure'], errors='coerce').tolist(),
            'sunshine_duration': pd.to_numeric(ordered['sunshine_duration'], errors='coerce').tolist(),
            'wind_measurement_height_m': pd.to_numeric(ordered['wind_measurement_height_m'], errors='coerce').tolist(),
            'wind_height_source': ordered['wind_height_source'].astype('string').fillna('').tolist(),
            'u2_m_s': pd.to_numeric(ordered['u2_m_s'], errors='coerce').tolist(),
        }
        series_records.append(record)
    return series_records


if __name__ == '__main__':
    raise SystemExit(main())
