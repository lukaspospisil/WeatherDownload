from __future__ import annotations

"""Internal utility for auditing CZ FAO-style station coverage plus measured evaporation."""

import argparse
from pathlib import Path
from typing import Callable

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    export_table,
    read_station_observation_metadata,
    read_station_metadata,
)

COUNTRY = 'CZ'
PROVIDER = 'historical_csv'
RESOLUTION = 'daily'
REQUIRED_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'wind_speed',
    'vapour_pressure',
    'sunshine_duration',
    'open_water_evaporation',
)
OBS_TYPE = 'DLY'
TIMEFUNC_BY_CANONICAL = {
    'tas_mean': 'AVG',
    'tas_max': '20:00',
    'tas_min': '20:00',
    'wind_speed': 'AVG',
    'vapour_pressure': 'AVG',
    'sunshine_duration': '00:00',
    'open_water_evaporation': '06:00',
}
RAW_TO_CANONICAL = {
    'T': 'tas_mean',
    'TMA': 'tas_max',
    'TMI': 'tas_min',
    'F': 'wind_speed',
    'WSPD': 'wind_speed',
    'E': 'vapour_pressure',
    'SSV': 'sunshine_duration',
    'VY': 'open_water_evaporation',
}
ELEMENT_UNITS = {
    'tas_mean': 'degC',
    'tas_max': 'degC',
    'tas_min': 'degC',
    'wind_speed': 'm/s',
    'vapour_pressure': 'hPa',
    'sunshine_duration': 'h',
    'open_water_evaporation': 'mm',
}
DEFAULT_OUTPUT_DIR = Path('outputs/cz_fao_plus_evaporation')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Internal utility: find Czech CHMI daily stations with the FAO-style input set plus measured open-water evaporation, then optionally download normalized observations.'
    )
    parser.add_argument('--start-date', help='Start date in YYYY-MM-DD format.')
    parser.add_argument('--end-date', help='End date in YYYY-MM-DD format.')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR, help='Directory for exported CSV outputs.')
    parser.add_argument('--max-stations', type=int, default=None, help='Optional limit after discovery for quick testing.')
    parser.add_argument('--station-id', action='append', dest='station_ids', default=None, help='Optional canonical station_id filter. Can be repeated.')
    parser.add_argument('--dry-run', action='store_true', help='Only discover/export qualifying stations. Do not download observations.')
    return parser


def discover_matching_stations(
    stations: pd.DataFrame,
    observation_metadata: pd.DataFrame,
    *,
    station_ids: list[str] | None = None,
    max_stations: int | None = None,
    metadata_projector: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, int]:
    projector = metadata_projector or _project_cz_daily_required_metadata
    inspected = stations.copy()
    if station_ids:
        normalized_station_ids = [station_id.strip().upper() for station_id in station_ids if station_id and station_id.strip()]
        inspected = inspected[inspected['station_id'].astype(str).isin(normalized_station_ids)].copy()
    inspected = inspected.drop_duplicates(subset=['station_id'], keep='first').reset_index(drop=True)
    inspected_count = len(inspected)
    projected = projector(observation_metadata)
    if projected.empty:
        return pd.DataFrame(columns=['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'available_required_elements', 'first_required_date', 'last_required_date']), inspected_count
    projected = projected[projected['station_id'].astype(str).isin(inspected['station_id'].astype(str))].copy()
    rows: list[dict[str, object]] = []
    merged = inspected.merge(projected, on='station_id', how='inner')
    for station in merged.itertuples(index=False):
        rows.append(
            {
                'station_id': station.station_id,
                'full_name': station.full_name,
                'latitude': station.latitude,
                'longitude': station.longitude,
                'elevation_m': station.elevation_m,
                'available_required_elements': ','.join(station.available_required_elements),
                'first_required_date': station.first_required_date,
                'last_required_date': station.last_required_date,
            }
        )

    candidates = pd.DataFrame.from_records(
        rows,
        columns=['station_id', 'full_name', 'latitude', 'longitude', 'elevation_m', 'available_required_elements', 'first_required_date', 'last_required_date'],
    )
    if not candidates.empty:
        candidates = candidates.sort_values('station_id').reset_index(drop=True)
    if max_stations is not None:
        if max_stations < 1:
            raise ValueError('--max-stations must be positive when provided.')
        candidates = candidates.head(max_stations).reset_index(drop=True)
    return candidates, inspected_count


def export_station_list(candidates: pd.DataFrame, output_dir: Path) -> Path:
    return export_table(candidates, output_dir / 'stations_cz_fao_plus_evaporation.csv', format='csv')


def download_candidate_observations(
    candidate_station_ids: list[str],
    *,
    start_date: str,
    end_date: str,
    station_metadata: pd.DataFrame,
) -> pd.DataFrame:
    query = ObservationQuery(
        country=COUNTRY,
        provider=PROVIDER,
        resolution=RESOLUTION,
        station_ids=candidate_station_ids,
        start_date=start_date,
        end_date=end_date,
        elements=list(REQUIRED_ELEMENTS),
    )
    observations = download_observations(query, country=COUNTRY, station_metadata=station_metadata)
    prepared = _filter_cz_daily_selection(observations.copy())
    prepared.insert(0, 'country', COUNTRY)
    prepared.insert(1, 'provider', PROVIDER)
    prepared.insert(6, 'date', prepared['observation_date'])
    prepared.insert(9, 'unit', prepared['element'].map(ELEMENT_UNITS).astype('string'))
    ordered_columns = [
        'country',
        'provider',
        'station_id',
        'gh_id',
        'element',
        'element_raw',
        'date',
        'observation_date',
        'time_function',
        'unit',
        'value',
        'flag',
        'quality',
        'dataset_scope',
        'resolution',
    ]
    return prepared.loc[:, ordered_columns]


def export_observations(table: pd.DataFrame, output_dir: Path) -> Path:
    return export_table(table, output_dir / 'observations_cz_fao_plus_evaporation_long.csv', format='csv')


def main() -> int:
    args = build_parser().parse_args()
    if not args.dry_run and (args.start_date is None or args.end_date is None):
        raise SystemExit('Use --dry-run or provide both --start-date and --end-date.')

    stations = read_station_metadata(country=COUNTRY)
    observation_metadata = read_station_observation_metadata(country=COUNTRY)
    candidates, inspected_count = discover_matching_stations(
        stations,
        observation_metadata,
        station_ids=args.station_ids,
        max_stations=args.max_stations,
    )
    station_path = export_station_list(candidates, args.output_dir)

    observation_path: Path | None = None
    observations = pd.DataFrame()
    if not args.dry_run and not candidates.empty:
        selected_metadata = stations[stations['station_id'].astype(str).isin(candidates['station_id'].astype(str))].copy()
        observations = download_candidate_observations(
            candidates['station_id'].astype(str).tolist(),
            start_date=args.start_date,
            end_date=args.end_date,
            station_metadata=selected_metadata,
        )
        observation_path = export_observations(observations, args.output_dir)

    print(f'CZ stations inspected: {inspected_count}')
    print(f'CZ stations with all required elements: {len(candidates)}')
    if args.dry_run:
        print('Selected date range: dry-run only')
    else:
        print(f'Selected date range: {args.start_date} to {args.end_date}')
    print(f'Downloaded rows: {len(observations)}')
    print(f'Station list: {station_path}')
    if observation_path is not None:
        print(f'Observations: {observation_path}')
    return 0


def _project_cz_daily_required_metadata(observation_metadata: pd.DataFrame) -> pd.DataFrame:
    relevant = observation_metadata[
        observation_metadata['obs_type'].astype(str).str.upper().eq(OBS_TYPE)
        & observation_metadata['element'].astype(str).str.upper().isin(RAW_TO_CANONICAL)
    ].copy()
    if relevant.empty:
        return pd.DataFrame(columns=['station_id', 'available_required_elements', 'first_required_date', 'last_required_date'])
    relevant['canonical_element'] = relevant['element'].astype(str).str.upper().map(RAW_TO_CANONICAL)
    relevant['begin_ts'] = pd.to_datetime(relevant['begin_date'], utc=True, errors='coerce')
    relevant['end_ts'] = pd.to_datetime(relevant['end_date'], utc=True, errors='coerce')
    element_spans = (
        relevant.groupby(['station_id', 'canonical_element'], as_index=False)
        .agg(begin_ts=('begin_ts', 'min'), end_ts=('end_ts', 'max'))
    )
    supported = (
        element_spans.groupby('station_id', as_index=False)
        .agg(
            available_required_elements=('canonical_element', lambda values: tuple(sorted(set(values), key=REQUIRED_ELEMENTS.index))),
            first_required_date=('begin_ts', 'max'),
            last_required_date=('end_ts', 'min'),
        )
    )
    supported = supported[
        supported['available_required_elements'].apply(lambda values: set(REQUIRED_ELEMENTS).issubset(values))
    ].copy()
    supported['first_required_date'] = supported['first_required_date'].dt.strftime('%Y-%m-%d')
    supported['last_required_date'] = supported['last_required_date'].dt.strftime('%Y-%m-%d')
    return supported.reset_index(drop=True)


def _filter_cz_daily_selection(observations: pd.DataFrame) -> pd.DataFrame:
    filtered = observations.copy()
    matching_mask = filtered.apply(
        lambda row: str(row.get('time_function', '')).strip() == TIMEFUNC_BY_CANONICAL.get(str(row.get('element', '')).strip()),
        axis=1,
    )
    matched = filtered[matching_mask].copy()
    if matched.empty:
        return filtered
    return matched.reset_index(drop=True)


if __name__ == '__main__':
    raise SystemExit(main())
