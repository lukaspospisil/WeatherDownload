from pathlib import Path

import numpy as np
import pandas as pd
from scipy.io import savemat

from weatherdownload import (
    ObservationQuery,
    download_observations,
    find_stations_with_elements,
)


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

COUNTRY = "CZ"
PROVIDER = "historical_csv"
RESOLUTION = "daily"

# Canonical WeatherDownload element for measured pan / open-water evaporation.
ELEMENT = "open_water_evaporation"

# Wide interval: download everything available.
START_DATE = "1800-01-01"
END_DATE = "2100-12-31"

# Output folder relative to the WeatherDownload repository root.
OUTDIR = Path("outputs") / "cz_pan_evaporation"
OUTDIR.mkdir(parents=True, exist_ok=True)

OUT_MAT = OUTDIR / "cz_open_water_evaporation_all_available.mat"
OUT_CSV = OUTDIR / "cz_open_water_evaporation_all_available.csv"
OUT_SUMMARY_CSV = OUTDIR / "cz_open_water_evaporation_station_summary.csv"


# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def to_matlab_datenum(dates):
    """
    Convert pandas datetime values to MATLAB datenum.

    MATLAB datenum counts days from 0000-01-00.
    Python ordinal counts days from 0001-01-01.
    The offset is 366 days.
    """
    dt = pd.to_datetime(dates)

    out = []
    for d in dt:
        py_dt = d.to_pydatetime()
        value = (
            py_dt.toordinal()
            + 366
            + py_dt.hour / 24
            + py_dt.minute / 1440
            + py_dt.second / 86400
            + py_dt.microsecond / 86400e6
        )
        out.append(value)

    return np.asarray(out, dtype=float).reshape(-1, 1)


def as_column(values, dtype=float):
    """
    Convert an array-like object to a MATLAB column vector.
    """
    return np.asarray(values, dtype=dtype).reshape(-1, 1)


def as_cellstr(values):
    """
    Convert values to a MATLAB cell array of strings.
    """
    s = pd.Series(values).fillna("").astype(str)
    return np.asarray(s.to_numpy(), dtype=object).reshape(-1, 1)


def first_existing_column(df, candidates):
    """
    Return the first existing column name from a list of candidate names.
    """
    for col in candidates:
        if col in df.columns:
            return col
    return None


# ---------------------------------------------------------------------
# Find stations with measured pan / open-water evaporation
# ---------------------------------------------------------------------

print("Finding CZ stations with measured open-water / pan evaporation...")

stations = find_stations_with_elements(
    country=COUNTRY,
    provider=PROVIDER,
    resolution=RESOLUTION,
    elements=[ELEMENT],
)

if stations is None or len(stations) == 0:
    raise RuntimeError("No CZ stations with open_water_evaporation were found.")

if "station_id" not in stations.columns:
    raise RuntimeError("Station table does not contain a 'station_id' column.")

station_ids = (
    stations["station_id"]
    .dropna()
    .astype(str)
    .drop_duplicates()
    .tolist()
)

print(f"Stations with {ELEMENT}: {len(station_ids)}")


# ---------------------------------------------------------------------
# Download only measured pan / open-water evaporation
# ---------------------------------------------------------------------

frames = []
chunk_size = 50

for i in range(0, len(station_ids), chunk_size):
    chunk = station_ids[i:i + chunk_size]

    print(
        f"Downloading chunk {i // chunk_size + 1}: "
        f"stations {i + 1}--{min(i + chunk_size, len(station_ids))}"
    )

    query = ObservationQuery(
        country=COUNTRY,
        provider=PROVIDER,
        resolution=RESOLUTION,
        station_ids=chunk,
        start_date=START_DATE,
        end_date=END_DATE,
        elements=[ELEMENT],
    )

    try:
        df_chunk = download_observations(query)

        if df_chunk is not None and len(df_chunk) > 0:
            frames.append(df_chunk)

    except Exception as exc:
        print(f"  Chunk failed: {exc}")
        print("  Falling back to station-by-station download.")

        for sid in chunk:
            query_one = ObservationQuery(
                country=COUNTRY,
                provider=PROVIDER,
                resolution=RESOLUTION,
                station_ids=[sid],
                start_date=START_DATE,
                end_date=END_DATE,
                elements=[ELEMENT],
            )

            try:
                df_one = download_observations(query_one)

                if df_one is not None and len(df_one) > 0:
                    frames.append(df_one)

            except Exception as exc_one:
                print(f"    Station {sid} failed: {exc_one}")


if not frames:
    raise RuntimeError("No evaporation records were downloaded.")

df = pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------
# Normalize and clean the downloaded table
# ---------------------------------------------------------------------

# Daily WeatherDownload output uses observation_date.
# This fallback keeps the script usable if an older local branch uses date.
date_col = first_existing_column(df, ["observation_date", "date"])

if date_col is None:
    raise RuntimeError("Downloaded table contains neither 'observation_date' nor 'date'.")

if "value" not in df.columns:
    raise RuntimeError("Downloaded table does not contain a 'value' column.")

if "element" in df.columns:
    df = df[df["element"].eq(ELEMENT)].copy()

df[date_col] = pd.to_datetime(df[date_col])
df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Keep only real measured evaporation values.
df = df[df["value"].notna()].copy()

# Use one canonical date column in the exported file.
df["date"] = df[date_col].dt.date.astype(str)

# Sort for easier downstream use.
df = df.sort_values(["station_id", date_col]).reset_index(drop=True)

print(f"Downloaded records after filtering: {len(df)}")
print(f"Stations with non-missing evaporation data: {df['station_id'].nunique()}")


# ---------------------------------------------------------------------
# Attach station metadata
# ---------------------------------------------------------------------

stations_for_merge = stations.copy()
stations_for_merge["station_id"] = stations_for_merge["station_id"].astype(str)

df = df.merge(
    stations_for_merge,
    on="station_id",
    how="left",
    suffixes=("", "_station"),
)


# ---------------------------------------------------------------------
# Save CSV preview
# ---------------------------------------------------------------------

df.to_csv(OUT_CSV, index=False, encoding="utf-8")
print(f"CSV written to: {OUT_CSV}")


# ---------------------------------------------------------------------
# Save station summary
# ---------------------------------------------------------------------

summary = (
    df.groupby("station_id", as_index=False)
    .agg(
        n_obs=("value", "size"),
        first_date=("date", "min"),
        last_date=("date", "max"),
        mean_E_pan_mm=("value", "mean"),
        min_E_pan_mm=("value", "min"),
        max_E_pan_mm=("value", "max"),
    )
    .sort_values(["n_obs", "station_id"], ascending=[False, True])
)

summary.to_csv(OUT_SUMMARY_CSV, index=False, encoding="utf-8")
print(f"Summary written to: {OUT_SUMMARY_CSV}")


# ---------------------------------------------------------------------
# Build MATLAB-friendly MAT structure
# ---------------------------------------------------------------------

mat = {
    # Main observation vectors.
    "station_id": as_cellstr(df["station_id"]),
    "date": as_cellstr(df["date"]),
    "date_num": to_matlab_datenum(df[date_col]),
    "E_pan_mm": as_column(df["value"].to_numpy(dtype=float)),

    # Basic metadata.
    "country": COUNTRY,
    "provider": PROVIDER,
    "resolution": RESOLUTION,
    "element": ELEMENT,
    "units": "mm/day",
    "description": (
        "Measured open-water / pan evaporation from WeatherDownload. "
        "This is observed evaporation from an open water surface, evaporation pan, "
        "or evaporimeter. It is not FAO Penman-Monteith ET0, PET, or modeled evaporation."
    ),
}

# Optional station-name column.
name_col = first_existing_column(df, ["full_name", "station_name", "name"])
if name_col is not None:
    mat["station_name"] = as_cellstr(df[name_col])

# Optional coordinate columns.
lat_col = first_existing_column(df, ["latitude", "lat"])
lon_col = first_existing_column(df, ["longitude", "lon"])
elev_col = first_existing_column(df, ["elevation_m", "elevation", "altitude", "elev"])

if lat_col is not None:
    mat["latitude"] = as_column(pd.to_numeric(df[lat_col], errors="coerce"))

if lon_col is not None:
    mat["longitude"] = as_column(pd.to_numeric(df[lon_col], errors="coerce"))

if elev_col is not None:
    mat["elevation_m"] = as_column(pd.to_numeric(df[elev_col], errors="coerce"))


# ---------------------------------------------------------------------
# Save MAT file
# ---------------------------------------------------------------------

savemat(
    OUT_MAT,
    mat,
    do_compression=True,
    long_field_names=True,
)

print(f"MAT written to: {OUT_MAT}")
print("Done.")