# MATLAB-Oriented FAO Preparation Example

The repository includes a workflow-oriented example:

python examples/download_fao.py

This example is intended for preparing a clean daily meteorological dataset from CHMI OpenData historical CSV for later processing in MATLAB.

## Purpose

The example does not compute:

- FAO Penman–Monteith reference evapotranspiration
- extraterrestrial radiation `Ra`
- any other derived variables

Its only purpose is to prepare a clean station-based dataset that can later be used in MATLAB for:

1. `Ra` computation
2. FAO computation
3. calibration of simpler empirical models

## Data source

The workflow uses only CHMI historical_csv daily data together with:

- `meta1.csv` for basic station metadata
- `meta2.csv` for observation availability metadata
- daily CSV files grouped by variable type

The example currently works only with daily data.

## Required daily elements

The final exported dataset is based on the following daily elements:

- `T`
- `TMA`
- `TMI`
- `F`
- `E`
- `SSV`

This is an E-based workflow, meaning that vapor pressure `E` is required in the final dataset. Relative humidity `H` is not used in the final export.

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

The example uses the following fixed daily selection rules:

- `T`   → `AVG`
- `F`   → `AVG`
- `E`   → `AVG`
- `TMA` → `20:00`
- `TMI` → `20:00`
- `SSV` → `00:00`

After that, the workflow derives calendar dates from `DT`, merges the selected variables by date, and keeps only complete days.

## Output structure

The example exports a single MATLAB-oriented `.mat` bundle containing three parts:

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

## Default output

By default, the example writes:

outputs/fao_daily.mat

## Optional arguments

The example supports:

- `--output` for changing the output `.mat` path
- `--station-id` for restricting the workflow to selected stations
- `--min-complete-days` for changing the retention threshold
- `--timeout` for HTTP timeout control

## Example

python examples/download_fao.py --output outputs/fao_daily.mat

For a restricted test run on selected stations:

python examples/download_fao.py --station-id 0-20000-0-11406 --min-complete-days 3650

## Why this example exists

This example is intentionally kept as a workflow layer on top of the core library.

General-purpose functionality remains in the library itself, such as:

- loading `meta1`
- loading `meta2`
- accessing CHMI registry information
- downloading daily CSV files
- parsing daily CHMI CSV data

The FAO-specific orchestration remains in the example, because it is a specialized downstream preparation workflow rather than a general-purpose core API feature.
