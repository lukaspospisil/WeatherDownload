# MATLAB-Oriented FAO Workflow

`examples/download_fao.py` is a workflow-oriented example built on top of the core WeatherDownload library.

It is intentionally specialized and currently targets:

- `CZ`
- CHMI `historical_csv`
- daily data only

## Purpose

The example prepares a clean daily meteorological dataset for later downstream work in MATLAB, R, or Python.

It does not compute:

- FAO Penman-Monteith evapotranspiration
- extraterrestrial radiation `Ra`
- derived variables

## Input Data

The workflow uses:

- `meta1.csv` for station identity/location metadata
- `meta2.csv` for observation availability metadata
- CHMI daily CSV files for:
  - `T`
  - `TMA`
  - `TMI`
  - `F`
  - `E`
  - `SSV`

This is an E-based workflow. Relative humidity is not part of the final export.

## Workflow Steps

1. load `meta1` station metadata
2. load `meta2` observation metadata
3. coarse-screen stations by required daily elements
4. apply a coarse validity-overlap pre-screen from `meta2`
5. deduplicate candidate stations to one row per canonical `station_id`
6. verify required daily CSV files
7. load daily data
8. apply fixed `TIMEFUNC` rules
9. merge by calendar date
10. keep only complete E-based days
11. retain only stations with at least `3650` complete days by default
12. export the final bundle

## Fixed `TIMEFUNC` Rules

| Element | Required `TIMEFUNC` |
| --- | --- |
| `T` | `AVG` |
| `F` | `AVG` |
| `E` | `AVG` |
| `TMA` | `20:00` |
| `TMI` | `20:00` |
| `SSV` | `00:00` |

## Execution Modes

The script supports:

- `full`
- `download`
- `build`

### `full`

- reuses cached files when present
- downloads only missing files
- builds and exports the final dataset

### `download`

- downloads and caches raw inputs only
- does not build the final dataset

### `build`

- reads only from the cache
- fails clearly if required cached files are missing

## Cache Layout

```text
<cache-dir>/
  meta1.csv
  meta2.csv
  daily/
    <WSI>/
      dly-<WSI>-T.csv
      dly-<WSI>-TMA.csv
      dly-<WSI>-TMI.csv
      dly-<WSI>-F.csv
      dly-<WSI>-E.csv
      dly-<WSI>-SSV.csv
```

Default cache directory:

- `outputs/fao_cache`

## Export Modes

Supported export formats:

- `mat`
- `parquet`
- `both`

### MAT bundle

The `.mat` export contains:

- `dataInfo`
- `stations`
- `series`

### Parquet bundle

The Parquet bundle directory contains:

- `data_info.json`
- `stations.parquet`
- `series.parquet`

`series.parquet` is long-form and contains only complete E-based days.

## Representative Station Row Rule

If `meta1` contains duplicate rows for the same canonical `station_id`, the workflow keeps the first matching row in the existing metadata order.

This makes the workflow stable and prevents repeated downloads for the same station.

## Example Commands

Cache raw inputs only:

```powershell
python examples/download_fao.py --mode download --cache-dir outputs/fao_cache
```

Build only from cache and export Parquet:

```powershell
python examples/download_fao.py --mode build --cache-dir outputs/fao_cache --export-format parquet --output-dir outputs/fao_daily_bundle
```

Run the full workflow and export both outputs:

```powershell
python examples/download_fao.py --mode full --cache-dir outputs/fao_cache --export-format both --output outputs/fao_daily.mat --output-dir outputs/fao_daily_bundle
```

## Why This Stays In `examples/`

This workflow is intentionally downstream-specific.

The reusable parts stay in the core library:

- metadata loading
- registry/discovery
- daily CSV download and parsing
- DataFrame export helpers

The FAO preparation orchestration stays in the example layer because it is a specialized workflow, not a general-purpose downloader API.
