from __future__ import annotations

import argparse
from pathlib import Path

COUNTRY = "CZ"
DATASET_SCOPE = "historical_csv"
RESOLUTION = "daily"
DEFAULT_OUTPUT_DIR = Path("outputs/mape2026")
DEFAULT_MAPE2026_STATION_IDS = (
    "0-20000-0-11406",  # Cheb
    "0-20000-0-11423",  # Primda
    "0-20000-0-11438",  # Tusimice
    "0-20000-0-11450",  # Plzen Mikulka
    "0-20000-0-11487",  # Kocelovice
    "0-20000-0-11502",  # Usti nad Labem Kockov
    "0-20000-0-11509",  # Doksany
    "0-20000-0-11519",  # Praha Karlov
    "0-20000-0-11520",  # Praha Libus
    "0-20000-0-11603",  # Liberec
    "0-20000-0-11628",  # Kosetice Kresin Kramolin
    "0-20000-0-11636",  # Kostelni Myslova
    "0-20000-0-11643",  # Pec pod Snezkou
    "0-20000-0-11659",  # Pribyslav Hriste
    "0-20000-0-11679",  # Usti nad Orlici
    "0-20000-0-11683",  # Svratouch
    "0-20000-0-11693",  # Dukovany
    "0-20000-0-11698",  # Kucharovice
    "0-20000-0-11710",  # Luka
    "0-20000-0-11723",  # Brno Turany
    "0-20000-0-11766",  # Cervena
    "0-20000-0-11774",  # Holesov
)
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
    parser.add_argument("--no-parquet", action="store_true", help="Skip Parquet exports.")
    parser.add_argument(
        "--min-complete-days",
        type=int,
        default=1,
        help="Reporting threshold for complete-row counts.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if (args.start_date is None) != (args.end_date is None):
        raise SystemExit("Provide both --start-date and --end-date together.")
    if args.min_complete_days < 1:
        raise SystemExit("--min-complete-days must be at least 1.")

    selected_station_ids = resolve_selected_station_ids(
        station_ids=args.station_ids,
        station_file=args.station_file,
    )
    print(f"Selected {len(selected_station_ids)} station(s) for MAPE 2026.")
    print("Station IDs: " + ", ".join(selected_station_ids))

    print("Reading station metadata...")
    from weatherdownload import read_station_metadata

    station_metadata = read_station_metadata(country=COUNTRY)
    selected_stations = select_station_metadata(station_metadata, selected_station_ids)
    print(f"Loaded metadata for {len(selected_stations)} selected station(s).")

    print("Downloading daily observations...")
    observations = download_selected_observations(
        station_ids=selected_station_ids,
        station_metadata=selected_stations,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(f"Downloaded {len(observations)} normalized long observation row(s).")

    print("Converting observations to wide daily format...")
    from weatherdownload.utils import observations_to_wide

    wide = observations_to_wide(
        observations,
        rename_elements={"pressure": "pressure_observed"},
    )
    wide = ensure_expected_wide_columns(wide)
    print(f"Prepared {len(wide)} wide daily row(s).")

    print("Building station-level summary...")
    summary = build_summary_table(wide)
    qualifying_fao = int((summary["n_complete_fao_rows"] >= args.min_complete_days).sum()) if not summary.empty else 0
    qualifying_extended = int((summary["n_complete_extended_rows"] >= args.min_complete_days).sum()) if not summary.empty else 0

    output_dir = args.output_dir
    wide_csv_path = output_dir / "fao_mape2026_daily_wide.csv"
    wide_parquet_path = output_dir / "fao_mape2026_daily_wide.parquet"
    stations_csv_path = output_dir / "fao_mape2026_stations.csv"
    stations_parquet_path = output_dir / "fao_mape2026_stations.parquet"
    summary_csv_path = output_dir / "fao_mape2026_summary.csv"

    print("Writing CSV outputs...")
    export_table(wide, wide_csv_path, format="csv")
    export_table(selected_stations, stations_csv_path, format="csv")
    export_table(summary, summary_csv_path, format="csv")

    if args.no_parquet:
        print("Skipping Parquet exports because --no-parquet was used.")
    else:
        export_optional_parquet(wide, wide_parquet_path, label="wide daily table")
        export_optional_parquet(selected_stations, stations_parquet_path, label="station metadata")

    print(f"Wide daily CSV: {wide_csv_path}")
    print(f"Station metadata CSV: {stations_csv_path}")
    print(f"Summary CSV: {summary_csv_path}")
    print(
        "Summary counts: "
        f"{qualifying_fao} station(s) with >= {args.min_complete_days} complete FAO rows, "
        f"{qualifying_extended} station(s) with >= {args.min_complete_days} complete extended rows."
    )
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


def select_station_metadata(stations: pd.DataFrame, station_ids: list[str]) -> pd.DataFrame:
    selected = stations[stations["station_id"].astype(str).isin(station_ids)].copy()
    missing_station_ids = [station_id for station_id in station_ids if station_id not in set(selected["station_id"].astype(str))]
    if missing_station_ids:
        missing = ", ".join(missing_station_ids)
        raise ValueError(f"Station metadata not found for station_id(s): {missing}")
    selected = selected.drop_duplicates(subset=["station_id"], keep="first")
    return selected.sort_values("station_id").reset_index(drop=True)


def download_selected_observations(
    *,
    station_ids: list[str],
    station_metadata: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    from weatherdownload import ObservationQuery, download_observations

    query_kwargs: dict[str, object] = {
        "country": COUNTRY,
        "dataset_scope": DATASET_SCOPE,
        "resolution": RESOLUTION,
        "station_ids": station_ids,
        "elements": list(DAILY_ELEMENTS),
    }
    if start_date is not None and end_date is not None:
        query_kwargs["start_date"] = start_date
        query_kwargs["end_date"] = end_date
    else:
        query_kwargs["all_history"] = True

    query = ObservationQuery(**query_kwargs)
    return download_observations(
        query,
        country=COUNTRY,
        station_metadata=station_metadata,
    )


def build_summary_table(wide: pd.DataFrame) -> pd.DataFrame:
    import pandas as pd

    summary_columns = [
        "station_id",
        "first_date",
        "last_date",
        "n_days",
        "n_complete_fao_rows",
        "n_complete_extended_rows",
    ]
    if wide.empty:
        return pd.DataFrame(columns=summary_columns)

    prepared = wide.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared["complete_fao_row"] = prepared.loc[:, list(FAO_REQUIRED_COLUMNS)].notna().all(axis=1)
    prepared["complete_extended_row"] = prepared.loc[:, list(EXTENDED_REQUIRED_COLUMNS)].notna().all(axis=1)

    summary = (
        prepared.groupby("station_id", as_index=False)
        .agg(
            first_date=("date", "min"),
            last_date=("date", "max"),
            n_days=("date", "size"),
            n_complete_fao_rows=("complete_fao_row", "sum"),
            n_complete_extended_rows=("complete_extended_row", "sum"),
        )
    )
    summary["first_date"] = summary["first_date"].dt.strftime("%Y-%m-%d")
    summary["last_date"] = summary["last_date"].dt.strftime("%Y-%m-%d")
    return summary.loc[:, summary_columns].sort_values("station_id").reset_index(drop=True)


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


def export_optional_parquet(table: pd.DataFrame, output_path: Path, *, label: str) -> None:
    from weatherdownload import export_table

    try:
        export_table(table, output_path, format="parquet")
    except RuntimeError as exc:
        print(f"Skipping Parquet export for {label}: {exc}")
    else:
        print(f"Parquet export written for {label}: {output_path}")


if __name__ == "__main__":
    raise SystemExit(main())
