from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from weatherdownload import (
    DEFAULT_META1_URL,
    DEFAULT_META2_URL,
    ObservationQuery,
    download_observations,
    export_table,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.utils import observations_to_wide

COUNTRY = "CZ"
PROVIDER = "historical_csv"
RESOLUTION = "daily"
OUTPUT_STEM = "cz_chmi_daily_tusimice_pm_evaporation"

TARGET_STATION_NAME = "Tušimice"
TARGET_LOCAL_ID = "U1KATU01"
TARGET_WSI = "0-20000-0-11438"
TARGET_WMO = "11438"

REQUESTED_ELEMENTS = (
    "tas_mean",
    "tas_max",
    "tas_min",
    "wind_speed",
    "relative_humidity",
    "pressure",
    "vapour_pressure",
    "sunshine_duration",
    "precipitation",
)
MEASURED_EVAPORATION_ELEMENT = "open_water_evaporation"
ALL_TRACKED_ELEMENTS = (*REQUESTED_ELEMENTS, MEASURED_EVAPORATION_ELEMENT)

CANONICAL_UNITS = {
    "tas_mean": "degC",
    "tas_max": "degC",
    "tas_min": "degC",
    "wind_speed": "m/s",
    "relative_humidity": "%",
    "pressure": "hPa",
    "vapour_pressure": "hPa",
    "sunshine_duration": "h",
    "precipitation": "mm",
    "open_water_evaporation": "mm",
}

PREFERRED_TIME_FUNCTION_BY_ELEMENT = {
    "tas_mean": "AVG",
    "tas_max": "20:00",
    "tas_min": "20:00",
    "wind_speed": "AVG",
    "relative_humidity": "AVG",
    "pressure": "AVG",
    "vapour_pressure": "AVG",
    "sunshine_duration": "00:00",
}

RAW_CODE_BY_CANONICAL = {
    "tas_mean": "T",
    "tas_max": "TMA",
    "tas_min": "TMI",
    "wind_speed": "F",
    "relative_humidity": "RH",
    "pressure": "P",
    "vapour_pressure": "E",
    "sunshine_duration": "SSV",
    "precipitation": "SRA",
    "open_water_evaporation": "VY",
}


@dataclass(frozen=True, slots=True)
class ResolvedStation:
    station_id: str
    gh_id: str
    full_name: str
    begin_date: str
    end_date: str
    latitude: float | None
    longitude: float | None
    elevation_m: float | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Download CZ/CHMI historical_csv daily observations for Tušimice "
            "for FAO-56 Penman-Monteith preprocessing, plus measured evaporation if available."
        )
    )
    parser.add_argument("--output-dir", type=Path, default=Path("outputs") / OUTPUT_STEM)
    parser.add_argument("--start-date", default=None, help="Optional YYYY-MM-DD start date.")
    parser.add_argument("--end-date", default=None, help="Optional YYYY-MM-DD end date.")
    return parser


def resolve_tusimice_station(stations: pd.DataFrame) -> ResolvedStation:
    if stations.empty:
        raise ValueError("Station metadata is empty.")

    prepared = stations.copy()
    prepared["station_id_norm"] = prepared["station_id"].astype("string").str.strip().str.upper()
    prepared["gh_id_norm"] = prepared["gh_id"].astype("string").str.strip().str.upper()
    prepared["full_name_norm"] = (
        prepared["full_name"]
        .astype("string")
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
        .str.strip()
        .str.lower()
    )

    direct_match = prepared[
        prepared["station_id_norm"].eq(TARGET_WSI)
        | prepared["gh_id_norm"].eq(TARGET_LOCAL_ID)
    ].copy()
    if direct_match.empty:
        raise ValueError(
            "Could not resolve Tušimice station from metadata using canonical station ids "
            f"{TARGET_WSI} / {TARGET_LOCAL_ID}."
        )

    direct_match["wmo_suffix"] = direct_match["station_id_norm"].str.rsplit("-", n=1).str[-1]
    direct_match["name_matches"] = direct_match["full_name_norm"].eq("tusimice")
    direct_match["local_id_matches"] = direct_match["gh_id_norm"].eq(TARGET_LOCAL_ID)
    direct_match["wsi_matches"] = direct_match["station_id_norm"].eq(TARGET_WSI)
    direct_match["wmo_matches"] = direct_match["wmo_suffix"].eq(TARGET_WMO)

    direct_match = direct_match.sort_values(
        ["local_id_matches", "wsi_matches", "wmo_matches", "name_matches", "end_date"],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)
    best = direct_match.iloc[0]

    if str(best["station_id_norm"]) != TARGET_WSI:
        raise ValueError(
            "Resolved metadata row does not match the required Tušimice WSI "
            f"{TARGET_WSI}: got {best['station_id']}."
        )
    if str(best["gh_id_norm"]) != TARGET_LOCAL_ID:
        raise ValueError(
            "Resolved metadata row does not match the preferred Tušimice local station id "
            f"{TARGET_LOCAL_ID}: got {best['gh_id']}."
        )
    if str(best["wmo_suffix"]) != TARGET_WMO:
        raise ValueError(
            "Resolved metadata row does not match the required Tušimice WMO suffix "
            f"{TARGET_WMO}: got {best['wmo_suffix']}."
        )

    return ResolvedStation(
        station_id=str(best["station_id"]),
        gh_id=str(best["gh_id"]),
        full_name=str(best["full_name"]),
        begin_date=str(best["begin_date"]),
        end_date=str(best["end_date"]),
        latitude=_optional_float(best["latitude"]),
        longitude=_optional_float(best["longitude"]),
        elevation_m=_optional_float(best["elevation_m"]),
    )


def filter_cz_daily_time_functions(observations: pd.DataFrame) -> pd.DataFrame:
    if observations.empty or "time_function" not in observations.columns:
        return observations.copy()

    prepared = observations.copy()
    element_series = prepared["element"].astype("string").str.strip()
    time_function_series = prepared["time_function"].astype("string").str.strip().str.upper()
    keep_mask = pd.Series(True, index=prepared.index, dtype="boolean")

    for element, preferred_time_function in PREFERRED_TIME_FUNCTION_BY_ELEMENT.items():
        element_mask = element_series.eq(element)
        if bool(element_mask.any()):
            keep_mask.loc[element_mask] = time_function_series.loc[element_mask].eq(preferred_time_function)

    filtered = prepared.loc[keep_mask.fillna(False)].copy()
    if filtered.duplicated(subset=["station_id", "observation_date", "element"], keep=False).any():
        raise ValueError("Duplicate station/date/element rows remain after CZ time_function filtering.")
    return filtered.reset_index(drop=True)


def build_station_element_metadata(
    observation_metadata: pd.DataFrame,
    *,
    station_id: str,
) -> pd.DataFrame:
    if observation_metadata.empty:
        return pd.DataFrame(
            columns=[
                "element",
                "metadata_begin_date",
                "metadata_end_date",
                "metadata_schedule",
                "metadata_name",
                "metadata_description",
                "metadata_height",
            ]
        )

    relevant = observation_metadata[
        observation_metadata["obs_type"].astype("string").str.upper().eq("DLY")
        & observation_metadata["station_id"].astype("string").str.upper().eq(station_id.upper())
        & observation_metadata["element"].astype("string").str.upper().isin({code.upper() for code in RAW_CODE_BY_CANONICAL.values()})
    ].copy()
    if relevant.empty:
        return pd.DataFrame(
            columns=[
                "element",
                "metadata_begin_date",
                "metadata_end_date",
                "metadata_schedule",
                "metadata_name",
                "metadata_description",
                "metadata_height",
            ]
        )

    inverse_raw_lookup = {raw_code: canonical for canonical, raw_code in RAW_CODE_BY_CANONICAL.items()}
    relevant["element"] = relevant["element"].astype("string").str.upper().map(inverse_raw_lookup)
    aggregated = (
        relevant.groupby("element", as_index=False)
        .agg(
            metadata_begin_date=("begin_date", "min"),
            metadata_end_date=("end_date", "max"),
            metadata_schedule=("schedule", lambda values: ",".join(sorted({str(value) for value in values.dropna() if str(value).strip()}))),
            metadata_name=("name", lambda values: ",".join(sorted({str(value) for value in values.dropna() if str(value).strip()}))),
            metadata_description=("description", lambda values: ",".join(sorted({str(value) for value in values.dropna() if str(value).strip()}))),
            metadata_height=("height", "min"),
        )
    )
    return aggregated.reset_index(drop=True)


def build_provenance_table(
    observations: pd.DataFrame,
    availability: pd.DataFrame,
    station_element_metadata: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    source_codes_by_element = (
        observations.groupby("element", dropna=False)["element_raw"]
        .agg(lambda values: ",".join(sorted({str(value) for value in values.dropna() if str(value).strip()})))
        .to_dict()
        if not observations.empty
        else {}
    )
    time_functions_by_element = (
        observations.groupby("element", dropna=False)["time_function"]
        .agg(lambda values: ",".join(sorted({str(value) for value in values.dropna() if str(value).strip()})))
        .to_dict()
        if not observations.empty
        else {}
    )

    metadata_lookup = (
        station_element_metadata.set_index("element").to_dict("index")
        if not station_element_metadata.empty
        else {}
    )

    for element in ALL_TRACKED_ELEMENTS:
        available_row = availability.loc[availability["element"].eq(element)].iloc[0]
        metadata_row = metadata_lookup.get(element, {})
        rows.append(
            {
                "element": element,
                "requested": element in REQUESTED_ELEMENTS,
                "is_measured_evaporation": element == MEASURED_EVAPORATION_ELEMENT,
                "downloaded": bool(available_row["downloaded"]),
                "canonical_unit": CANONICAL_UNITS.get(element, ""),
                "preferred_raw_code": RAW_CODE_BY_CANONICAL.get(element, ""),
                "source_raw_codes_seen": source_codes_by_element.get(element, ""),
                "preferred_time_function": PREFERRED_TIME_FUNCTION_BY_ELEMENT.get(element, ""),
                "time_functions_seen": time_functions_by_element.get(element, ""),
                "metadata_begin_date": metadata_row.get("metadata_begin_date", pd.NA),
                "metadata_end_date": metadata_row.get("metadata_end_date", pd.NA),
                "metadata_schedule": metadata_row.get("metadata_schedule", ""),
                "metadata_name": metadata_row.get("metadata_name", ""),
                "metadata_description": metadata_row.get("metadata_description", ""),
                "metadata_height": metadata_row.get("metadata_height", pd.NA),
                "notes": (
                    "Measured pan/open-water evaporation from CHMI VY."
                    if element == MEASURED_EVAPORATION_ELEMENT
                    else ""
                ),
            }
        )

    return pd.DataFrame.from_records(
        rows,
        columns=[
            "element",
            "requested",
            "is_measured_evaporation",
            "downloaded",
            "canonical_unit",
            "preferred_raw_code",
            "source_raw_codes_seen",
            "preferred_time_function",
            "time_functions_seen",
            "metadata_begin_date",
            "metadata_end_date",
            "metadata_schedule",
            "metadata_name",
            "metadata_description",
            "metadata_height",
            "notes",
        ],
    )


def build_availability_report(wide: pd.DataFrame) -> pd.DataFrame:
    total_rows = len(wide)
    rows: list[dict[str, object]] = []

    for element in ALL_TRACKED_ELEMENTS:
        if element not in wide.columns:
            rows.append(
                {
                    "element": element,
                    "downloaded": False,
                    "first_date": pd.NA,
                    "last_date": pd.NA,
                    "n_valid_values": 0,
                    "missing_percent": 100.0 if total_rows else pd.NA,
                }
            )
            continue

        values = pd.to_numeric(wide[element], errors="coerce")
        valid_mask = values.notna()
        valid_dates = pd.to_datetime(wide.loc[valid_mask, "date"], errors="coerce")
        missing_percent = float((~valid_mask).mean() * 100.0) if total_rows else pd.NA
        rows.append(
            {
                "element": element,
                "downloaded": bool(valid_mask.any()),
                "first_date": valid_dates.min().strftime("%Y-%m-%d") if valid_mask.any() else pd.NA,
                "last_date": valid_dates.max().strftime("%Y-%m-%d") if valid_mask.any() else pd.NA,
                "n_valid_values": int(valid_mask.sum()),
                "missing_percent": round(missing_percent, 2) if missing_percent == missing_percent else pd.NA,
            }
        )

    return pd.DataFrame.from_records(
        rows,
        columns=["element", "downloaded", "first_date", "last_date", "n_valid_values", "missing_percent"],
    )


def build_station_table(station: ResolvedStation) -> pd.DataFrame:
    return pd.DataFrame.from_records(
        [
            {
                "country": COUNTRY,
                "provider": PROVIDER,
                "resolution": RESOLUTION,
                "station_id": station.station_id,
                "gh_id": station.gh_id,
                "full_name": station.full_name,
                "target_station_name": TARGET_STATION_NAME,
                "target_local_id": TARGET_LOCAL_ID,
                "target_wsi": TARGET_WSI,
                "target_wmo": TARGET_WMO,
                "begin_date": station.begin_date,
                "end_date": station.end_date,
                "latitude": station.latitude,
                "longitude": station.longitude,
                "elevation_m": station.elevation_m,
            }
        ]
    )


def prepare_output_tables(
    observations: pd.DataFrame,
    station: ResolvedStation,
    station_element_metadata: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    filtered = filter_cz_daily_time_functions(observations)
    wide = observations_to_wide(filtered)
    wide.insert(0, "country", COUNTRY)
    wide.insert(1, "provider", PROVIDER)
    wide.insert(2, "resolution", RESOLUTION)
    wide.insert(3, "station_name", station.full_name)
    wide.insert(4, "gh_id", station.gh_id)
    ordered = [
        "country",
        "provider",
        "resolution",
        "station_id",
        "gh_id",
        "station_name",
        "date",
        *ALL_TRACKED_ELEMENTS,
    ]
    for column in ordered:
        if column not in wide.columns:
            wide[column] = pd.NA
    wide = wide.loc[:, ordered].sort_values(["station_id", "date"]).reset_index(drop=True)
    availability = build_availability_report(wide)
    provenance = build_provenance_table(filtered, availability, station_element_metadata)
    return filtered, wide, availability, provenance


def export_outputs(
    *,
    wide: pd.DataFrame,
    availability: pd.DataFrame,
    provenance: pd.DataFrame,
    station_table: pd.DataFrame,
    output_dir: Path,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    exports = {
        "wide_csv": export_table(wide, output_dir / f"{OUTPUT_STEM}.csv", format="csv"),
        "wide_xlsx": export_table(wide, output_dir / f"{OUTPUT_STEM}.xlsx", format="excel"),
        "wide_mat": export_table(wide, output_dir / f"{OUTPUT_STEM}.mat", format="mat"),
        "availability_csv": export_table(availability, output_dir / f"{OUTPUT_STEM}_availability.csv", format="csv"),
        "provenance_csv": export_table(provenance, output_dir / f"{OUTPUT_STEM}_provenance.csv", format="csv"),
        "station_csv": export_table(station_table, output_dir / f"{OUTPUT_STEM}_station.csv", format="csv"),
    }
    return exports


def print_availability_report(availability: pd.DataFrame) -> None:
    report = availability.copy()
    report["downloaded"] = report["downloaded"].map({True: "yes", False: "no"})
    print("Availability report:")
    print(report.to_string(index=False))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if (args.start_date is None) != (args.end_date is None):
        raise SystemExit("Provide both --start-date and --end-date together, or neither for all history.")

    stations = read_station_metadata(country=COUNTRY, source_url=DEFAULT_META1_URL)
    observation_metadata = read_station_observation_metadata(country=COUNTRY, source_url=DEFAULT_META2_URL)
    station = resolve_tusimice_station(stations)
    selected_station_metadata = stations.loc[stations["station_id"].astype("string").eq(station.station_id)].copy()
    station_element_metadata = build_station_element_metadata(
        observation_metadata,
        station_id=station.station_id,
    )

    query_kwargs: dict[str, object] = {
        "country": COUNTRY,
        "provider": PROVIDER,
        "resolution": RESOLUTION,
        "station_ids": [station.station_id],
        "elements": list(ALL_TRACKED_ELEMENTS),
    }
    if args.start_date is not None and args.end_date is not None:
        query_kwargs["start_date"] = args.start_date
        query_kwargs["end_date"] = args.end_date
    else:
        query_kwargs["all_history"] = True

    observations = download_observations(
        ObservationQuery(**query_kwargs),
        country=COUNTRY,
        station_metadata=selected_station_metadata,
    )
    filtered, wide, availability, provenance = prepare_output_tables(
        observations,
        station,
        station_element_metadata,
    )
    station_table = build_station_table(station)
    exports = export_outputs(
        wide=wide,
        availability=availability,
        provenance=provenance,
        station_table=station_table,
        output_dir=args.output_dir,
    )

    print(f"Resolved station: {station.station_id} / {station.gh_id} / {station.full_name}")
    print(f"Downloaded long rows: {len(observations)}")
    print(f"Filtered long rows: {len(filtered)}")
    print(f"Merged daily rows: {len(wide)}")
    print_availability_report(availability)
    for label, path in exports.items():
        print(f"{label}: {path}")
    return 0


def _optional_float(value: object) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


if __name__ == "__main__":
    raise SystemExit(main())
