from __future__ import annotations

import argparse
from pathlib import Path
import re
import time

COUNTRY = "CZ"
DATASET_SCOPE = "historical_csv"
RESOLUTION = "daily"
DEFAULT_OUTPUT_DIR = Path("outputs/mape2026")
DEFAULT_CACHE_DIR = Path("outputs/mape2026_cache")
DEFAULT_MAPE2026_STATIONS = {
    "0-20000-0-11438": "Tušimice",
    "0-20000-0-11502": "Ústí nad Labem Kočkov",
    "0-20000-0-11509": "Doksany",
    "0-20000-0-11520": "Praha Libuš",
    "0-20000-0-11450": "Plzeň Mikulka",
    "0-20000-0-11406": "Cheb",
    "0-20000-0-11603": "Liberec",
    "0-20000-0-11423": "Přimda",
    "0-20000-0-11487": "Kocelovice Nový Dvůr",
    "0-20000-0-11628": "Košetice",
    "0-20000-0-11643": "Pec pod Sněžkou",
    "0-20000-0-11659": "Přibyslav Keřkov",
    "0-20000-0-11683": "Svratouch",
    "0-20000-0-11636": "Kostelní Myslová",
    "0-20000-0-11679": "Ústí nad Orlicí",
    "0-20000-0-11693": "Dukovany",
    "0-20000-0-11698": "Kuchařovice",
    "0-20000-0-11710": "Luká",
    "0-20000-0-11723": "Brno Černovice",
    "0-203-0-20201031001": "Nové Heřminovy",
    "0-20000-0-11766": "Červená",
    "0-20000-0-11774": "Holešov",
    "0-203-0-11790": "Ostrava Poruba",
}
DEFAULT_MAPE2026_STATION_IDS = (
    "0-20000-0-11438",        # Tušimice
    "0-20000-0-11502",        # Ústí nad Labem Kočkov
    "0-20000-0-11509",        # Doksany
    "0-20000-0-11520",        # Praha Libuš
    "0-20000-0-11450",        # Plzeň Mikulka
    "0-20000-0-11406",        # Cheb
    "0-20000-0-11603",        # Liberec
    "0-20000-0-11423",        # Přimda
    "0-20000-0-11487",        # Kocelovice Nový Dvůr
    "0-20000-0-11628",        # Košetice
    "0-20000-0-11643",        # Pec pod Sněžkou
    "0-20000-0-11659",        # Přibyslav Keřkov
    "0-20000-0-11683",        # Svratouch
    "0-20000-0-11636",        # Kostelní Myslová
    "0-20000-0-11679",        # Ústí nad Orlicí
    "0-20000-0-11693",        # Dukovany
    "0-20000-0-11698",        # Kuchařovice
    "0-20000-0-11710",        # Luká
    "0-20000-0-11723",        # Brno Černovice
    "0-203-0-20201031001",    # Nové Heřminovy
    "0-20000-0-11766",        # Červená
    "0-20000-0-11774",        # Holešov
    "0-203-0-11790",          # Ostrava Poruba
)
assert len(DEFAULT_MAPE2026_STATION_IDS) == 23
assert tuple(DEFAULT_MAPE2026_STATIONS) == DEFAULT_MAPE2026_STATION_IDS
DAILY_ELEMENTS = (
    "tas_mean",
    "tas_max",
    "tas_min",
    "wind_speed",
    "vapour_pressure",
    "sunshine_duration",
    "relative_humidity",
    "pressure",
    "open_water_evaporation",
)
PREFERRED_TIME_FUNCTION_BY_ELEMENT = {
    "tas_mean": "AVG",
    "wind_speed": "AVG",
    "vapour_pressure": "AVG",
    "relative_humidity": "AVG",
    "pressure": "AVG",
}
FAO_REQUIRED_COLUMNS = (
    "tas_mean",
    "tas_max",
    "tas_min",
    "wind_speed",
    "vapour_pressure",
    "sunshine_duration",
)
EXTENDED_REQUIRED_COLUMNS = (
    *FAO_REQUIRED_COLUMNS,
    "relative_humidity",
    "pressure_observed",
    "open_water_evaporation",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download daily CZ observations for the fixed MAPE 2026 station set "
            "and export wide tables plus metadata."
        )
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument(
        "--station-id",
        action="append",
        dest="station_ids",
        default=None,
        help="Repeatable station_id override. If any station override is provided, defaults are replaced.",
    )
    parser.add_argument(
        "--station-file",
        type=Path,
        default=None,
        help="Optional TXT or CSV file with station_id values.",
    )
    parser.add_argument("--start-date", default=None, help="Optional YYYY-MM-DD start date.")
    parser.add_argument("--end-date", default=None, help="Optional YYYY-MM-DD end date.")
    parser.add_argument("--force-refresh", action="store_true", help="Redownload station observations even if a matching cache file exists.")
    parser.add_argument("--debug-duplicates", action="store_true", help="Print detailed duplicate diagnostics before CZ time_function filtering.")
    parser.add_argument("--no-full-csv", action="store_true", help="Skip the large full reproducibility CSV files while still writing Parquet and analysis-ready CSV.")
    parser.add_argument("--no-parquet", action="store_true", help="Skip Parquet exports.")
    parser.add_argument("--analysis-start-date", default=None, help="Optional YYYY-MM-DD lower bound applied only to the analysis-ready output.")
    parser.add_argument(
        "--min-complete-days",
        type=int,
        default=1,
        help="Reporting threshold for complete-row counts.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    overall_started_at = time.perf_counter()
    args = build_parser().parse_args(argv)
    if (args.start_date is None) != (args.end_date is None):
        raise SystemExit("Provide both --start-date and --end-date together.")
    if args.min_complete_days < 1:
        raise SystemExit("--min-complete-days must be at least 1.")

    selected_station_ids = resolve_selected_station_ids(
        station_ids=args.station_ids,
        station_file=args.station_file,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    print(f"Selected {len(selected_station_ids)} station(s) for MAPE 2026.")
    print("Station IDs: " + ", ".join(selected_station_ids))
    print("Requested daily elements: " + ", ".join(DAILY_ELEMENTS))
    print(f"Output directory: {args.output_dir}")
    print(f"Cache directory: {args.cache_dir}")

    print("Downloading daily observations...")
    selected_stations, station_metadata_for_download = load_station_metadata_best_effort(selected_station_ids)

    observations = download_selected_observations(
        station_ids=selected_station_ids,
        station_metadata=station_metadata_for_download,
        start_date=args.start_date,
        end_date=args.end_date,
        cache_dir=args.cache_dir,
        force_refresh=args.force_refresh,
        debug_duplicates=args.debug_duplicates,
    )
    print(f"Combined filtered observations: {len(observations)} normalized long observation row(s).")
    report_element_completeness(observations, label="Combined filtered observations")

    print("Checking duplicate station/date/element keys after filtering...")
    duplicate_check_started_at = time.perf_counter()
    ensure_no_duplicate_observation_keys(observations)
    print(f"Post-filter duplicate check finished in {format_elapsed(time.perf_counter() - duplicate_check_started_at)}.")

    print("Converting observations to wide daily format...")
    wide_started_at = time.perf_counter()
    from weatherdownload.utils import observations_to_wide

    wide = observations_to_wide(
        observations,
        rename_elements={"pressure": "pressure_observed"},
    )
    wide = ensure_expected_wide_columns(wide)
    print(f"Prepared {len(wide)} wide daily row(s).")
    print(f"Wide conversion finished in {format_elapsed(time.perf_counter() - wide_started_at)}.")

    print("Computing FAO-56 daily reference evapotranspiration...")
    fao_started_at = time.perf_counter()
    from weatherdownload import compute_fao56_daily_from_wide

    wide_with_fao = compute_fao56_daily_from_wide(
        wide,
        selected_stations,
        wind_measurement_height_m=10.0,
        use_observed_pressure=False,
    )
    fao_output_rows = int(wide_with_fao["E_FAO"].notna().sum()) if "E_FAO" in wide_with_fao.columns else 0
    negative_e_fao_raw_rows = int(wide_with_fao["E_FAO_raw"].lt(0).sum()) if "E_FAO_raw" in wide_with_fao.columns else 0
    min_e_fao_raw = wide_with_fao["E_FAO_raw"].min(skipna=True) if "E_FAO_raw" in wide_with_fao.columns else None
    min_e_fao = wide_with_fao["E_FAO"].min(skipna=True) if "E_FAO" in wide_with_fao.columns else None
    print(f"Computed non-missing E_FAO for {fao_output_rows} row(s).")
    print(f"Negative E_FAO_raw rows clipped to zero: {negative_e_fao_raw_rows}")
    print(f"Minimum E_FAO_raw: {min_e_fao_raw if min_e_fao_raw == min_e_fao_raw else 'n/a'}")
    print(f"Minimum E_FAO: {min_e_fao if min_e_fao == min_e_fao else 'n/a'}")
    analysis_ready = build_analysis_ready_table(wide_with_fao, analysis_start_date=args.analysis_start_date)
    overlap_stats = summarize_overlap_stats(wide_with_fao)
    print(f"Non-missing open_water_evaporation rows: {overlap_stats['n_open_water_evaporation_rows']}")
    print(f"Overlap rows with both E_FAO and open_water_evaporation: {overlap_stats['n_overlap_e_fao_open_water_rows']}")
    print(
        "Overlap date range: "
        f"{overlap_stats['first_overlap_date'] or 'n/a'} to {overlap_stats['last_overlap_date'] or 'n/a'}"
    )
    print(f"FAO computation finished in {format_elapsed(time.perf_counter() - fao_started_at)}.")

    print("Building station-level summary...")
    summary_started_at = time.perf_counter()
    summary = build_summary_table(wide_with_fao)
    qualifying_fao = int((summary["n_complete_fao_rows"] >= args.min_complete_days).sum()) if not summary.empty else 0
    qualifying_extended = int((summary["n_complete_extended_rows"] >= args.min_complete_days).sum()) if not summary.empty else 0
    print(f"Summary building finished in {format_elapsed(time.perf_counter() - summary_started_at)}.")

    output_dir = args.output_dir
    wide_csv_path = output_dir / "fao_mape2026_daily_wide.csv"
    wide_with_fao_csv_path = output_dir / "fao_mape2026_daily_wide_with_fao.csv"
    analysis_ready_csv_path = output_dir / "fao_mape2026_analysis_ready.csv"
    wide_parquet_path = output_dir / "fao_mape2026_daily_wide.parquet"
    wide_with_fao_parquet_path = output_dir / "fao_mape2026_daily_wide_with_fao.parquet"
    analysis_ready_parquet_path = output_dir / "fao_mape2026_analysis_ready.parquet"
    stations_csv_path = output_dir / "fao_mape2026_stations.csv"
    stations_parquet_path = output_dir / "fao_mape2026_stations.parquet"
    summary_csv_path = output_dir / "fao_mape2026_summary.csv"

    wide = strip_table_attrs(wide)
    wide_with_fao = strip_table_attrs(wide_with_fao)
    analysis_ready = strip_table_attrs(analysis_ready)
    summary = strip_table_attrs(summary)
    selected_stations = strip_table_attrs(selected_stations)

    print("Writing CSV outputs...")
    csv_started_at = time.perf_counter()
    from weatherdownload import export_table

    if args.no_full_csv:
        print("Skipping full reproducibility CSV files because --no-full-csv was used.")
    else:
        export_table(wide, wide_csv_path, format="csv")
        export_table(wide_with_fao, wide_with_fao_csv_path, format="csv")
    export_table(analysis_ready, analysis_ready_csv_path, format="csv")
    export_table(summary, summary_csv_path, format="csv")
    export_table(selected_stations, stations_csv_path, format="csv")
    print(f"CSV export finished in {format_elapsed(time.perf_counter() - csv_started_at)}.")

    if args.no_parquet:
        print("Skipping Parquet exports because --no-parquet was used.")
    else:
        print("Writing Parquet outputs...")
        parquet_started_at = time.perf_counter()
        export_optional_parquet(wide, wide_parquet_path, label="wide daily table")
        export_optional_parquet(wide_with_fao, wide_with_fao_parquet_path, label="wide daily table with FAO")
        export_optional_parquet(analysis_ready, analysis_ready_parquet_path, label="analysis-ready table")
        export_optional_parquet(selected_stations, stations_parquet_path, label="station metadata")
        print(f"Parquet export finished in {format_elapsed(time.perf_counter() - parquet_started_at)}.")

    print(f"Wide daily CSV: {wide_csv_path}")
    print(f"Wide daily CSV with FAO: {wide_with_fao_csv_path}")
    print(f"Analysis-ready CSV: {analysis_ready_csv_path}")
    print(f"Station metadata CSV: {stations_csv_path}")
    print(f"Summary CSV: {summary_csv_path}")
    print(
        "Summary counts: "
        f"{qualifying_fao} station(s) with >= {args.min_complete_days} complete FAO rows, "
        f"{qualifying_extended} station(s) with >= {args.min_complete_days} complete extended rows."
    )
    print(f"Total elapsed time: {format_elapsed(time.perf_counter() - overall_started_at)}")
    return 0


def resolve_selected_station_ids(
    *,
    station_ids: list[str] | None,
    station_file: Path | None,
) -> list[str]:
    override_station_ids: list[str] = []
    if station_ids:
        override_station_ids.extend(station_ids)
    if station_file is not None:
        override_station_ids.extend(read_station_ids_from_file(station_file))
    if override_station_ids:
        return normalize_station_ids(override_station_ids)
    return list(DEFAULT_MAPE2026_STATION_IDS)


def read_station_ids_from_file(path: Path) -> list[str]:
    import pandas as pd

    if not path.exists():
        raise ValueError(f"Station file does not exist: {path}")
    if path.suffix.lower() == ".csv":
        table = pd.read_csv(path, dtype={"station_id": "string"})
        if "station_id" not in table.columns:
            raise ValueError(f"CSV station file must contain a 'station_id' column: {path}")
        return table["station_id"].dropna().astype(str).tolist()
    return path.read_text(encoding="utf-8").splitlines()


def normalize_station_ids(station_ids: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for station_id in station_ids:
        cleaned = str(station_id).strip().upper()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    if not normalized:
        raise ValueError("No valid station_id values were provided.")
    return normalized


def load_station_metadata_best_effort(station_ids: list[str]) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    from weatherdownload import DEFAULT_META1_URL, read_station_metadata

    try:
        station_metadata = read_station_metadata(
            country=COUNTRY,
            source_url=DEFAULT_META1_URL,
        )
    except Exception as exc:
        print(
            "Warning: station metadata lookup failed; continuing with fixed station names only. "
            f"Reason: {exc}"
        )
        fallback = build_minimal_station_metadata(station_ids)
        return fallback, fallback

    selected = select_station_metadata_with_fallback(station_metadata, station_ids)
    print(f"Prepared station metadata for {len(selected)} selected station(s).")
    return selected, station_metadata


def build_minimal_station_metadata(station_ids: list[str]) -> pd.DataFrame:
    import pandas as pd

    rows = [
        {
            "station_id": station_id,
            "gh_id": pd.NA,
            "station_name": DEFAULT_MAPE2026_STATIONS.get(station_id, station_id),
        }
        for station_id in station_ids
    ]
    return pd.DataFrame.from_records(rows, columns=["station_id", "gh_id", "station_name"])


def select_station_metadata_with_fallback(stations: pd.DataFrame, station_ids: list[str]) -> pd.DataFrame:
    import pandas as pd

    selected = stations[stations["station_id"].astype(str).isin(station_ids)].copy()
    if selected.empty:
        return build_minimal_station_metadata(station_ids)

    selected = selected.drop_duplicates(subset=["station_id"], keep="first").copy()
    selected["station_name"] = selected["full_name"].fillna(selected["station_id"]).astype(str)
    selected["station_name"] = selected["station_id"].map(DEFAULT_MAPE2026_STATIONS).fillna(selected["station_name"])

    missing_station_ids = [station_id for station_id in station_ids if station_id not in set(selected["station_id"].astype(str))]
    if missing_station_ids:
        fallback = build_minimal_station_metadata(missing_station_ids)
        selected = pd.concat([selected, fallback], ignore_index=True, sort=False)

    preferred_columns = [
        "station_id",
        "station_name",
        "full_name",
        "gh_id",
        "begin_date",
        "end_date",
        "longitude",
        "latitude",
        "elevation_m",
    ]
    available_columns = [column for column in preferred_columns if column in selected.columns]
    ordered = selected.loc[:, available_columns].copy()
    return ordered.sort_values("station_id").reset_index(drop=True)


def download_selected_observations(
    *,
    station_ids: list[str],
    station_metadata: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
    cache_dir: Path,
    force_refresh: bool,
    debug_duplicates: bool,
) -> pd.DataFrame:
    import pandas as pd

    from weatherdownload import ObservationQuery, download_observations

    started_at = time.perf_counter()
    tables: list[pd.DataFrame] = []
    total_stations = len(station_ids)
    for index, station_id in enumerate(station_ids, start=1):
        station_started_at = time.perf_counter()
        print(f"[{index}/{total_stations}] Station {station_id}")
        try:
            station_table = process_station_observations(
                station_id=station_id,
                station_metadata=station_metadata,
                start_date=start_date,
                end_date=end_date,
                cache_dir=cache_dir,
                force_refresh=force_refresh,
                debug_duplicates=debug_duplicates,
            )
            tables.append(station_table)
            print(f"  elapsed for station: {format_elapsed(time.perf_counter() - station_started_at)}")
            print(f"  total elapsed: {format_elapsed(time.perf_counter() - started_at)}")
        except Exception as exc:
            print(
                f"  Station {station_id} failed after "
                f"{format_elapsed(time.perf_counter() - station_started_at)}."
            )
            raise RuntimeError(f"Failed while processing station {station_id}") from exc

    if not tables:
        return pd.DataFrame()
    combined = pd.concat(tables, ignore_index=True, sort=False)
    print(f"Total filtered long rows: {len(combined)}")
    return combined


def process_station_observations(
    *,
    station_id: str,
    station_metadata: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
    cache_dir: Path,
    force_refresh: bool,
    debug_duplicates: bool,
) -> pd.DataFrame:
    from weatherdownload import ObservationQuery, download_observations

    filtered_cache_path = resolve_station_cache_path(
        cache_dir=cache_dir,
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
        elements=DAILY_ELEMENTS,
        stage="filtered",
    )
    raw_cache_path = resolve_station_cache_path(
        cache_dir=cache_dir,
        station_id=station_id,
        start_date=start_date,
        end_date=end_date,
        elements=DAILY_ELEMENTS,
        stage="raw",
    )

    if not force_refresh:
        cached_filtered = read_cached_station_observations(filtered_cache_path)
        if cached_filtered is not None:
            print(f"  loaded filtered rows: {len(cached_filtered)}")
            return cached_filtered

    raw_station_table: pd.DataFrame | None = None
    if not force_refresh:
        raw_station_table = read_cached_station_observations(raw_cache_path)
        if raw_station_table is not None:
            print(f"  loaded raw rows: {len(raw_station_table)}")

    if raw_station_table is None:
        query_kwargs: dict[str, object] = {
            "country": COUNTRY,
            "dataset_scope": DATASET_SCOPE,
            "resolution": RESOLUTION,
            "station_ids": [station_id],
            "elements": list(DAILY_ELEMENTS),
        }
        if start_date is not None and end_date is not None:
            query_kwargs["start_date"] = start_date
            query_kwargs["end_date"] = end_date
        else:
            query_kwargs["all_history"] = True

        query = ObservationQuery(**query_kwargs)
        raw_station_table = download_observations(
            query,
            country=COUNTRY,
            station_metadata=station_metadata,
        )
        write_cached_station_observations(raw_station_table, raw_cache_path)
        print(f"  downloaded raw rows: {len(raw_station_table)}")

    if debug_duplicates:
        print("  debug duplicate diagnosis before time_function filtering...")
        report_duplicate_observations(raw_station_table)

    print("  applying CZ time_function filter...")
    filter_started_at = time.perf_counter()
    filtered_station_table, removed_rows = filter_cz_daily_time_functions_with_stats(raw_station_table)
    print(
        "  time_function filter removed "
        f"{removed_rows} rows and kept {len(filtered_station_table)} rows in "
        f"{format_elapsed(time.perf_counter() - filter_started_at)}"
    )
    report_element_completeness(filtered_station_table, label="  filtered observations")
    write_cached_station_observations(filtered_station_table, filtered_cache_path)
    return filtered_station_table


def resolve_station_cache_path(
    *,
    cache_dir: Path,
    station_id: str,
    start_date: str | None,
    end_date: str | None,
    elements: tuple[str, ...],
    stage: str,
) -> Path:
    range_token = f"{start_date or 'all'}_to_{end_date or 'all'}"
    station_token = re.sub(r"[^A-Za-z0-9._-]+", "_", station_id)
    element_token = stable_elements_signature(elements)
    filename = f"{station_token}_{range_token}_{element_token}_{stage}.parquet"
    return cache_dir / filename


def stable_elements_signature(elements: tuple[str, ...]) -> str:
    joined = ",".join(elements)
    compact = re.sub(r"[^A-Za-z0-9]+", "", joined)
    return compact[:32] or "elements"


def read_cached_station_observations(cache_path: Path) -> pd.DataFrame | None:
    import pandas as pd

    parquet_path = cache_path
    csv_path = cache_path.with_suffix(".csv")
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except (ImportError, ValueError, OSError) as exc:
            print(f"  Warning: failed to read cache {parquet_path}; redownloading. Reason: {exc}")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return None


def write_cached_station_observations(table: pd.DataFrame, cache_path: Path) -> None:
    csv_path = cache_path.with_suffix(".csv")
    try:
        strip_table_attrs(table).to_parquet(cache_path, index=False)
    except ImportError:
        strip_table_attrs(table).to_csv(csv_path, index=False)
    except ValueError:
        strip_table_attrs(table).to_csv(csv_path, index=False)


def format_elapsed(seconds: float) -> str:
    if seconds < 60.0:
        return f"{seconds:.1f}s"
    minutes, remaining_seconds = divmod(seconds, 60.0)
    if minutes < 60.0:
        return f"{int(minutes)}m {remaining_seconds:.1f}s"
    hours, remaining_minutes = divmod(int(minutes), 60)
    return f"{hours}h {remaining_minutes}m {remaining_seconds:.1f}s"


def build_summary_table(wide: pd.DataFrame) -> pd.DataFrame:
    import pandas as pd

    summary_columns = [
        "station_id",
        "first_date",
        "last_date",
        "n_days",
        "n_complete_fao_rows",
        "n_complete_extended_rows",
        "n_complete_fao_output_rows",
        "n_rows_analysis_ready",
        "first_analysis_ready_date",
        "last_analysis_ready_date",
        "n_open_water_evaporation_rows",
        "n_e_fao_rows",
        "n_overlap_e_fao_open_water_rows",
        "n_negative_E_FAO_raw_rows",
        "min_E_FAO_raw",
        "min_E_FAO",
    ]
    if wide.empty:
        return pd.DataFrame(columns=summary_columns)

    prepared = wide.copy()
    for column in EXTENDED_REQUIRED_COLUMNS:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    if "E_FAO_raw" not in prepared.columns:
        prepared["E_FAO_raw"] = pd.NA
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared["complete_fao_row"] = prepared.loc[:, list(FAO_REQUIRED_COLUMNS)].notna().all(axis=1)
    prepared["complete_extended_row"] = prepared.loc[:, list(EXTENDED_REQUIRED_COLUMNS)].notna().all(axis=1)
    prepared["complete_fao_output_row"] = prepared["E_FAO"].notna() if "E_FAO" in prepared.columns else False
    prepared["open_water_evaporation_row"] = prepared["open_water_evaporation"].notna() if "open_water_evaporation" in prepared.columns else False
    prepared["overlap_e_fao_open_water_row"] = prepared["complete_fao_output_row"] & prepared["open_water_evaporation_row"]
    prepared["analysis_ready_row"] = analysis_ready_mask(prepared)
    prepared["analysis_ready_date"] = prepared["date"].where(prepared["analysis_ready_row"])
    prepared["negative_e_fao_raw_row"] = prepared["E_FAO_raw"].lt(0) if "E_FAO_raw" in prepared.columns else False

    summary = (
        prepared.groupby("station_id", as_index=False)
        .agg(
            first_date=("date", "min"),
            last_date=("date", "max"),
            n_days=("date", "size"),
            n_complete_fao_rows=("complete_fao_row", "sum"),
            n_complete_extended_rows=("complete_extended_row", "sum"),
            n_complete_fao_output_rows=("complete_fao_output_row", "sum"),
            n_rows_analysis_ready=("analysis_ready_row", "sum"),
            first_analysis_ready_date=("analysis_ready_date", "min"),
            last_analysis_ready_date=("analysis_ready_date", "max"),
            n_open_water_evaporation_rows=("open_water_evaporation_row", "sum"),
            n_e_fao_rows=("complete_fao_output_row", "sum"),
            n_overlap_e_fao_open_water_rows=("overlap_e_fao_open_water_row", "sum"),
            n_negative_E_FAO_raw_rows=("negative_e_fao_raw_row", "sum"),
            min_E_FAO_raw=("E_FAO_raw", "min"),
            min_E_FAO=("E_FAO", "min"),
        )
    )
    summary["first_date"] = summary["first_date"].dt.strftime("%Y-%m-%d")
    summary["last_date"] = summary["last_date"].dt.strftime("%Y-%m-%d")
    summary["first_analysis_ready_date"] = summary["first_analysis_ready_date"].dt.strftime("%Y-%m-%d")
    summary["last_analysis_ready_date"] = summary["last_analysis_ready_date"].dt.strftime("%Y-%m-%d")
    return summary.loc[:, summary_columns].sort_values("station_id").reset_index(drop=True)


def analysis_ready_mask(wide_with_fao: pd.DataFrame) -> pd.Series:
    required_columns = [
        "E_FAO",
        "open_water_evaporation",
        "tas_mean",
        "tas_max",
        "tas_min",
        "wind_speed",
        "vapour_pressure",
        "sunshine_duration",
    ]
    return wide_with_fao.loc[:, required_columns].notna().all(axis=1)


def build_analysis_ready_table(wide_with_fao: pd.DataFrame, *, analysis_start_date: str | None) -> pd.DataFrame:
    import pandas as pd

    if wide_with_fao.empty:
        return pd.DataFrame(columns=analysis_ready_columns())

    prepared = wide_with_fao.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    mask = analysis_ready_mask(prepared)
    if analysis_start_date is not None:
        start_ts = pd.to_datetime(analysis_start_date, errors="coerce")
        mask = mask & prepared["date"].ge(start_ts)
    filtered = prepared.loc[mask, :].copy()
    filtered["date"] = filtered["date"].dt.strftime("%Y-%m-%d")
    for column in analysis_ready_columns():
        if column not in filtered.columns:
            filtered[column] = pd.NA
    return filtered.loc[:, analysis_ready_columns()].sort_values(["station_id", "date"]).reset_index(drop=True)


def analysis_ready_columns() -> list[str]:
    return [
        "station_id",
        "date",
        "tas_mean",
        "tas_max",
        "tas_min",
        "wind_speed",
        "vapour_pressure",
        "sunshine_duration",
        "pressure_observed",
        "relative_humidity",
        "open_water_evaporation",
        "E_FAO_raw",
        "E_FAO",
        "vpd_raw_kpa",
        "vpd_kpa",
        "ea_kpa",
        "es_kpa",
        "Rs_MJ_m2_day",
        "Rn_MJ_m2_day",
        "u2_m_s",
    ]


def summarize_overlap_stats(wide_with_fao: pd.DataFrame) -> dict[str, object]:
    import pandas as pd

    if wide_with_fao.empty:
        return {
            "n_open_water_evaporation_rows": 0,
            "n_e_fao_rows": 0,
            "n_overlap_e_fao_open_water_rows": 0,
            "first_overlap_date": None,
            "last_overlap_date": None,
        }

    prepared = wide_with_fao.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    open_water_mask = prepared["open_water_evaporation"].notna() if "open_water_evaporation" in prepared.columns else pd.Series(False, index=prepared.index)
    e_fao_mask = prepared["E_FAO"].notna() if "E_FAO" in prepared.columns else pd.Series(False, index=prepared.index)
    overlap_mask = open_water_mask & e_fao_mask
    overlap_dates = prepared.loc[overlap_mask, "date"]
    first_overlap = overlap_dates.min()
    last_overlap = overlap_dates.max()
    return {
        "n_open_water_evaporation_rows": int(open_water_mask.sum()),
        "n_e_fao_rows": int(e_fao_mask.sum()),
        "n_overlap_e_fao_open_water_rows": int(overlap_mask.sum()),
        "first_overlap_date": first_overlap.strftime("%Y-%m-%d") if pd.notna(first_overlap) else None,
        "last_overlap_date": last_overlap.strftime("%Y-%m-%d") if pd.notna(last_overlap) else None,
    }


def ensure_expected_wide_columns(wide: pd.DataFrame) -> pd.DataFrame:
    import pandas as pd

    expected_columns = [
        "station_id",
        "date",
        "tas_mean",
        "tas_max",
        "tas_min",
        "wind_speed",
        "vapour_pressure",
        "sunshine_duration",
        "relative_humidity",
        "pressure_observed",
        "open_water_evaporation",
    ]
    prepared = wide.copy()
    for column in expected_columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    return prepared.loc[:, expected_columns]


def report_duplicate_observations(observations: pd.DataFrame) -> None:
    import pandas as pd

    if observations.empty:
        print("Duplicate check: no observations downloaded.")
        return

    date_column = "date" if "date" in observations.columns else "observation_date"
    grouping_columns = ["station_id", date_column, "element"]
    duplicate_mask = observations.duplicated(subset=grouping_columns, keep=False)
    duplicate_rows = observations.loc[duplicate_mask].copy()
    if duplicate_rows.empty:
        print("Duplicate check: no duplicated station/date/element groups found.")
        return

    summary_rows: list[dict[str, object]] = []
    exact_duplicate_groups = 0
    conflicting_groups = 0

    for keys, group in duplicate_rows.groupby(grouping_columns, dropna=False, sort=True):
        numeric_values = pd.to_numeric(group["value"], errors="coerce")
        distinct_values = sorted({value for value in numeric_values.dropna().tolist()})
        if numeric_values.isna().any():
            distinct_values.append("NaN")
        serialized_values = ", ".join(str(value) for value in distinct_values[:5])
        if len(distinct_values) > 5:
            serialized_values += ", ..."

        row_signature_columns = [column for column in group.columns if column not in {"provider", "resolution"}]
        exact_duplicates = group.duplicated(subset=row_signature_columns, keep=False).all()
        if len(distinct_values) <= 1 and exact_duplicates:
            exact_duplicate_groups += 1
        else:
            conflicting_groups += 1

        station_id, observation_date, element = keys
        summary_rows.append(
            {
                "station_id": station_id,
                "date": observation_date,
                "element": element,
                "n_rows": len(group),
                "n_distinct_values": len(distinct_values),
                "example_values": serialized_values,
            }
        )

    summary = pd.DataFrame.from_records(
        summary_rows,
        columns=["station_id", "date", "element", "n_rows", "n_distinct_values", "example_values"],
    )
    print(
        "Duplicate check: "
        f"{len(summary)} duplicated station/date/element group(s) found; "
        f"{exact_duplicate_groups} exact-duplicate group(s), "
        f"{conflicting_groups} conflicting group(s)."
    )
    print(summary.head(20).to_string(index=False))


def ensure_no_duplicate_observation_keys(observations: pd.DataFrame) -> None:
    import pandas as pd

    if observations.empty:
        print("Duplicate check: no observations downloaded.")
        return

    date_column = "date" if "date" in observations.columns else "observation_date"
    grouping_columns = ["station_id", date_column, "element"]
    duplicate_mask = observations.duplicated(subset=grouping_columns, keep=False)
    duplicate_rows = observations.loc[duplicate_mask].copy()
    if duplicate_rows.empty:
        print("Duplicate check: no duplicated station/date/element groups found.")
        return

    summary_rows: list[dict[str, object]] = []
    for keys, group in duplicate_rows.groupby(grouping_columns, dropna=False, sort=True):
        numeric_values = pd.to_numeric(group["value"], errors="coerce")
        distinct_values = sorted({value for value in numeric_values.dropna().tolist()})
        if numeric_values.isna().any():
            distinct_values.append("NaN")
        serialized_values = ", ".join(str(value) for value in distinct_values[:5])
        if len(distinct_values) > 5:
            serialized_values += ", ..."
        station_id, observation_date, element = keys
        summary_rows.append(
            {
                "station_id": station_id,
                "date": observation_date,
                "element": element,
                "n_rows": len(group),
                "n_distinct_values": len(distinct_values),
                "example_values": serialized_values,
            }
        )

    summary = pd.DataFrame.from_records(
        summary_rows,
        columns=["station_id", "date", "element", "n_rows", "n_distinct_values", "example_values"],
    )
    print(f"Duplicate check: {len(summary)} duplicated station/date/element group(s) remain after filtering.")
    print(summary.head(20).to_string(index=False))
    raise ValueError("Duplicate station/date/element rows remain after CZ time_function filtering.")


def report_element_completeness(observations: pd.DataFrame, *, label: str) -> None:
    import pandas as pd

    if observations.empty or "element" not in observations.columns:
        print(f"{label}: no observation rows available for element completeness.")
        return

    counts = (
        observations.assign(value_numeric=pd.to_numeric(observations["value"], errors="coerce"))
        .groupby("element", dropna=False, sort=True)
        .agg(
            n_rows=("element", "size"),
            n_non_missing_values=("value_numeric", lambda values: int(values.notna().sum())),
        )
        .reset_index()
    )
    print(f"{label} by element:")
    print(counts.to_string(index=False))


def filter_cz_daily_time_functions(observations: pd.DataFrame) -> pd.DataFrame:
    filtered, _removed_rows = filter_cz_daily_time_functions_with_stats(observations)
    return filtered


def filter_cz_daily_time_functions_with_stats(observations: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    import pandas as pd

    if observations.empty or "time_function" not in observations.columns:
        return observations, 0

    element_series = observations["element"].astype("string").str.strip()
    time_function_series = observations["time_function"].astype("string").str.strip().str.upper()
    keep_mask = pd.Series(True, index=observations.index, dtype="boolean")

    for element, preferred_time_function in PREFERRED_TIME_FUNCTION_BY_ELEMENT.items():
        element_mask = element_series.eq(element)
        if bool(element_mask.any()):
            keep_mask.loc[element_mask] = time_function_series.loc[element_mask].eq(preferred_time_function)

    filtered = observations.loc[keep_mask.fillna(False)].copy()
    removed_rows = len(observations) - len(filtered)
    return filtered, removed_rows


def export_optional_parquet(table: pd.DataFrame, output_path: Path, *, label: str) -> None:
    from weatherdownload import export_table

    try:
        export_table(table, output_path, format="parquet")
    except RuntimeError as exc:
        print(f"Skipping Parquet export for {label}: {exc}")
    else:
        print(f"Parquet export written for {label}: {output_path}")


def strip_table_attrs(table: pd.DataFrame) -> pd.DataFrame:
    prepared = table.copy()
    prepared.attrs = {}
    return prepared


if __name__ == "__main__":
    raise SystemExit(main())
