# MATLAB-Oriented FAO Preparation Example

The repository includes a workflow-oriented example:

```powershell
python examples/download_fao.py
```

This example prepares a clean daily meteorological dataset from CHMI OpenData historical CSV for later FAO-related processing in MATLAB, R, or Python.

## Purpose

The example does not compute:

- FAO Penman-Monteith reference evapotranspiration
- extraterrestrial radiation `Ra`
- any other derived variables

Its purpose is only to prepare a clean station-based dataset that can later be used for:

1. `Ra` computation
2. FAO computation
3. calibration of simpler empirical models

## Data source

The workflow uses only CHMI `historical_csv` daily data together with:

- `meta1.csv` for basic station metadata
- `meta2.csv` for observation availability metadata
- daily CSV files grouped by variable type

The example currently works only with daily data.

## Required daily elements

The final exported dataset is based on these daily elements:

- `T`
- `TMA`
- `TMI`
- `F`
- `E`
- `SSV`

This is an E-based workflow, meaning vapor pressure `E` is required in the final dataset. Relative humidity `H` is not used in the final export.

## Station screening logic

The example applies a multi-stage screening workflow:

1. load station metadata from `meta1`
2. load observation metadata from `meta2`
3. keep only stations that declare all required daily elements
4. use `meta2` validity intervals for a coarse overlap pre-screen
5. verify that the required daily CSV files really exist
6. load the daily files
7. apply fixed `TIMEFUNC` selection rules
8. merge variables by calendar date
9. keep only complete E-based days
10. retain only stations with at least `3650` complete days by default

## TIMEFUNC selection

The example uses these fixed daily selection rules:

- `T` -> `AVG`
- `F` -> `AVG`
- `E` -> `AVG`
- `TMA` -> `20:00`
- `TMI` -> `20:00`
- `SSV` -> `00:00`

After that, the workflow derives calendar dates from `DT`, merges the selected variables by date, and keeps only complete days.

## Export modes

The example supports three export modes:

- `mat`: write only the MATLAB-oriented `.mat` bundle
- `parquet`: write only the portable Parquet bundle directory
- `both`: write both outputs

CLI arguments:

- `--export-format {mat,parquet,both}`
- `--output` for the `.mat` path, default `outputs/fao_daily.mat`
- `--output-dir` for the Parquet bundle directory, default `outputs/fao_daily_bundle`
- `--station-id` for restricting the workflow to selected stations
- `--min-complete-days` for changing the retention threshold
- `--timeout` for HTTP timeout control

## MAT output structure

The MATLAB-oriented `.mat` bundle contains three parts:

### `dataInfo`
Dataset-level metadata such as:

- creation timestamp
- dataset type
- source description
- required elements
- minimum required number of complete days
- number of retained stations

### `stations`
A station summary table/struct containing at least:

- `WSI`
- `FULL_NAME`
- `Latitude`
- `Longitude`
- `Elevation`
- `NumCompleteDays_E`
- `FirstCompleteDate_E`
- `LastCompleteDate_E`

### `series`
A per-station structure array where each item contains:

- `WSI`
- `FULL_NAME`
- `Latitude`
- `Longitude`
- `Elevation`
- `Date`
- `T`
- `TMA`
- `TMI`
- `F`
- `E`
- `SSV`

Each station series already contains only complete E-based days, so no missing values remain in the exported meteorological variables.

## Parquet bundle layout

The portable Parquet bundle is a directory containing:

- `data_info.json`
- `stations.parquet`
- `series.parquet`

`data_info.json` stores dataset-level metadata such as `CreatedAt`, `DatasetType`, `Source`, `Elements`, `MinCompleteDays`, and `NumStations`.

`stations.parquet` contains one row per retained station with:

- `WSI`
- `FULL_NAME`
- `Latitude`
- `Longitude`
- `Elevation`
- `NumCompleteDays_E`
- `FirstCompleteDate_E`
- `LastCompleteDate_E`

`series.parquet` is one long flat table with one row per station-date and columns:

- `WSI`
- `FULL_NAME`
- `Latitude`
- `Longitude`
- `Elevation`
- `Date`
- `T`
- `TMA`
- `TMI`
- `F`
- `E`
- `SSV`

Like the MAT export, `series.parquet` contains only complete E-based days, so no missing values remain in `T`, `TMA`, `TMI`, `F`, `E`, or `SSV`.

## Examples

Write only the MATLAB bundle:

```powershell
python examples/download_fao.py --export-format mat --output outputs/fao_daily.mat
```

Write only the portable Parquet bundle:

```powershell
python examples/download_fao.py --export-format parquet --output-dir outputs/fao_daily_bundle
```

Write both outputs in one run:

```powershell
python examples/download_fao.py --export-format both --output outputs/fao_daily.mat --output-dir outputs/fao_daily_bundle
```

For a restricted test run on selected stations:

```powershell
python examples/download_fao.py --station-id 0-20000-0-11406 --min-complete-days 3650
```

## Why this example exists

This example is intentionally kept as a workflow layer on top of the core library.

General-purpose functionality remains in the library itself, such as:

- loading `meta1`
- loading `meta2`
- accessing CHMI registry information
- downloading daily CSV files
- parsing daily CHMI CSV data
- exporting Parquet tables

The FAO-specific orchestration remains in the example, because it is a specialized downstream preparation workflow rather than a general-purpose core API feature.\n\nWhen meta1 contains duplicate rows for the same canonical station_id, the example keeps the first matching meta1 row in the existing metadata order as the representative station record. This makes the workflow stable and prevents repeated downloads for the same station.

