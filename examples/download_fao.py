from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.io import savemat

from weatherdownload import get_dataset_spec, read_station_metadata, read_station_observation_metadata
from weatherdownload.chmi_daily import DailyDownloadTarget, download_daily_csv, parse_daily_csv

REQUIRED_ELEMENTS = ['T', 'TMA', 'TMI', 'F', 'E', 'SSV']
TIMEFUNC_BY_ELEMENT = {
    'T': 'AVG',
    'F': 'AVG',
    'E': 'AVG',
    'TMA': '20:00',
    'TMI': '20:00',
    'SSV': '00:00',
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    meta1 = read_station_metadata(timeout=args.timeout)
    meta2 = read_station_observation_metadata(timeout=args.timeout)

    if args.station_ids:
        requested = {station_id.strip().upper() for station_id in args.station_ids}
        meta1 = meta1[meta1['station_id'].str.upper().isin(requested)].reset_index(drop=True)
        meta2 = meta2[meta2['station_id'].str.upper().isin(requested)].reset_index(drop=True)

    candidates = screen_candidate_stations(meta1, meta2, min_complete_days=args.min_complete_days)
    print(f'Meta2-screened candidate stations: {len(candidates)}')

    retained_series: list[dict[str, Any]] = []
    station_rows: list[dict[str, Any]] = []

    for station in candidates.itertuples(index=False):
        print(f'Processing {station.station_id} ({station.full_name})')
        csv_tables = fetch_required_daily_tables(station.station_id, timeout=args.timeout)
        if csv_tables is None:
            print('  Missing one or more required daily CSV files, skipping.')
            continue

        complete = prepare_complete_station_series(csv_tables)
        if complete.empty:
            print('  No complete E-based days after TIMEFUNC selection, skipping.')
            continue
        if len(complete) < args.min_complete_days:
            print(f'  Only {len(complete)} complete days, below threshold {args.min_complete_days}.')
            continue

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
        'DatasetType': 'CHMI daily meteorological dataset prepared for later FAO Penman-Monteith processing',
        'Source': 'CHMI OpenData historical_csv meta1.csv, meta2.csv, and daily CSV files',
        'Elements': REQUIRED_ELEMENTS,
        'MinCompleteDays': int(args.min_complete_days),
        'NumStations': int(len(retained_series)),
    }
    export_bundle(args.output, data_info=data_info, stations=station_rows, series=retained_series)
    print(f'Exported MATLAB-oriented FAO-prep bundle to {args.output}')
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Prepare a MATLAB-oriented CHMI daily dataset for later FAO processing.')
    parser.add_argument('--output', type=Path, default=Path('outputs/fao_daily.mat'), help='MATLAB .mat output path.')
    parser.add_argument('--station-id', action='append', dest='station_ids', help='Optional CHMI WSI station_id filter. Can be provided multiple times.')
    parser.add_argument('--min-complete-days', type=int, default=3650, help='Minimum number of complete E-based days required per station.')
    parser.add_argument('--timeout', type=int, default=60, help='HTTP timeout in seconds.')
    return parser


def screen_candidate_stations(meta1: pd.DataFrame, meta2: pd.DataFrame, *, min_complete_days: int) -> pd.DataFrame:
    relevant = meta2[(meta2['obs_type'] == 'DLY') & (meta2['element'].isin(REQUIRED_ELEMENTS))].copy()
    supported = (
        relevant.groupby('station_id')['element']
        .agg(lambda values: set(values))
        .reset_index(name='elements')
    )
    supported['has_required_elements'] = supported['elements'].apply(lambda values: set(REQUIRED_ELEMENTS).issubset(values))

    overlap = estimate_station_overlap_days(relevant)
    screened = supported.merge(overlap, on='station_id', how='left')
    screened['overlap_days_estimate'] = screened['overlap_days_estimate'].fillna(0).astype(int)
    candidate_ids = screened[
        screened['has_required_elements'] & (screened['overlap_days_estimate'] >= min_complete_days)
    ]['station_id']
    return meta1[meta1['station_id'].isin(candidate_ids)].reset_index(drop=True)


def estimate_station_overlap_days(meta2: pd.DataFrame) -> pd.DataFrame:
    dated = meta2.copy()
    dated['begin_ts'] = pd.to_datetime(dated['begin_date'], utc=True)
    dated['end_ts'] = pd.to_datetime(dated['end_date'], utc=True)
    element_spans = (
        dated.groupby(['station_id', 'element'])
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


def fetch_required_daily_tables(station_id: str, timeout: int = 60) -> dict[str, pd.DataFrame] | None:
    tables: dict[str, pd.DataFrame] = {}
    for element in REQUIRED_ELEMENTS:
        try:
            csv_text = download_daily_csv(_build_daily_target(station_id, element), timeout=timeout)
        except FileNotFoundError:
            return None
        table = parse_daily_csv(csv_text)
        tables[element] = table[table['ELEMENT'].astype(str).str.upper() == element].reset_index(drop=True)
    return tables


def prepare_complete_station_series(raw_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    selected_tables: list[pd.DataFrame] = []
    for element in REQUIRED_ELEMENTS:
        table = raw_tables[element]
        selected = _select_timefunc_rows(table, element, TIMEFUNC_BY_ELEMENT[element])
        if selected.empty:
            return pd.DataFrame(columns=['Date', *REQUIRED_ELEMENTS])
        selected_tables.append(selected)

    merged = selected_tables[0]
    for table in selected_tables[1:]:
        merged = merged.merge(table, on='Date', how='inner')

    complete = merged.dropna(subset=REQUIRED_ELEMENTS).sort_values('Date').reset_index(drop=True)
    return complete


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
        'T': complete['T'].astype(float).tolist(),
        'TMA': complete['TMA'].astype(float).tolist(),
        'TMI': complete['TMI'].astype(float).tolist(),
        'F': complete['F'].astype(float).tolist(),
        'E': complete['E'].astype(float).tolist(),
        'SSV': complete['SSV'].astype(float).tolist(),
    }


def export_bundle(output_path: Path, *, data_info: dict[str, Any], stations: list[dict[str, Any]], series: list[dict[str, Any]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'dataInfo': _to_mat_struct(data_info),
        'stations': _station_rows_to_struct(stations),
        'series': np.array([_to_mat_struct(item) for item in series], dtype=object),
    }
    savemat(output_path, payload)


def _build_daily_target(station_id: str, element: str) -> DailyDownloadTarget:
    spec = get_dataset_spec('historical_csv', 'daily')
    group = spec.element_groups[element]
    url = spec.endpoint_pattern.format(group=group, station_id=station_id, element=element)
    return DailyDownloadTarget(station_id=station_id, element=element, group=group, url=url)


def _select_timefunc_rows(table: pd.DataFrame, element: str, time_function: str) -> pd.DataFrame:
    filtered = table[table['TIMEFUNC'].astype(str).str.strip() == time_function].copy()
    if filtered.empty:
        return pd.DataFrame(columns=['Date', element])
    filtered['Date'] = pd.to_datetime(filtered['DT'], utc=True).dt.date
    filtered[element] = pd.to_numeric(filtered['VALUE'], errors='coerce')
    filtered = filtered[['Date', element]].dropna(subset=[element]).drop_duplicates(subset=['Date'], keep='last')
    return filtered.reset_index(drop=True)


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
    if isinstance(value, list):
        if not value:
            return np.array([], dtype=object)
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
