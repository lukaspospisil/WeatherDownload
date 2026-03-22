# MATLAB-Oriented FAO Workflow

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

`examples/download_fao.py` is a workflow-oriented example built on top of the core WeatherDownload library.

It is country-aware, but only for countries whose daily FAO-prep mapping is explicitly implemented.

Currently supported:

- `CZ`
- `DE`

## CLI

The example now accepts:

```powershell
python examples/download_fao.py --country CZ
python examples/download_fao.py --country DE
```

`--country` uses ISO 3166-1 alpha-2 codes and defaults to `CZ`.

## Conceptual Target Variables

The workflow prepares the same conceptual daily meteorological variables across countries:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

These canonical names are now also the final exported variable names in the station series and Parquet bundle.

## Final Export Schema

The final per-station series expose:

- `Date`
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

This keeps the exported dataset country-independent and canonical-first.

## Provider Provenance Metadata

The workflow still preserves how the dataset was built.

`dataInfo` now includes `ProviderElementMapping`, which records for each canonical exported variable:

- the raw/provider code or codes used for the selected country
- the country-specific selection rule, if any

Example idea:

- `tas_mean -> raw_codes=['T']` and `selection_rule='AVG'` for `CZ`
- `tas_mean -> raw_codes=['TMK']` and `selection_rule=null` for `DE`

## Country-Specific Mapping

### CZ

| Canonical export name | CHMI raw code | Selection rule |
| --- | --- | --- |
| `tas_mean` | `T` | `TIMEFUNC=AVG` |
| `tas_max` | `TMA` | `TIMEFUNC=20:00` |
| `tas_min` | `TMI` | `TIMEFUNC=20:00` |
| `wind_speed` | `F` | `TIMEFUNC=AVG` |
| `vapour_pressure` | `E` | `TIMEFUNC=AVG` |
| `sunshine_duration` | `SSV` | `TIMEFUNC=00:00` |

### DE

| Canonical export name | DWD raw code | Selection rule |
| --- | --- | --- |
| `tas_mean` | `TMK` | none |
| `tas_max` | `TXK` | none |
| `tas_min` | `TNK` | none |
| `wind_speed` | `FM` | none |
| `vapour_pressure` | `VPM` | none |
| `sunshine_duration` | `SDK` | none |

DE daily files do not expose a CHMI-like `TIMEFUNC` concept, so the DE branch uses the normalized daily values directly.

## Workflow Steps

1. load station metadata for the selected country
2. load observation metadata for the selected country
3. coarse-screen stations by required daily FAO-prep variables
4. apply a coarse validity-overlap pre-screen from observation metadata
5. deduplicate candidate stations to one row per canonical `station_id`
6. download or reuse cached normalized daily observations for the selected country
7. apply country-specific daily selection rules
8. merge variables by calendar date
9. keep only complete days
10. retain only stations with at least `3650` complete days by default
11. export the final bundle

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

- downloads and caches normalized metadata and daily observations only
- does not build the final dataset

### `build`

- reads only from the cache
- fails clearly if required cached inputs are missing

## Cache Layout

The cache is now country-scoped:

```text
<cache-dir>/
  CZ/
    meta1.csv
    meta2.csv
    daily/
      <station_id>/
        daily-<station_id>.csv
  DE/
    meta1.csv
    meta2.csv
    daily/
      <station_id>/
        daily-<station_id>.csv
```

Default base cache directory:

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

`series.parquet` is long-form and contains only complete FAO-prep days with canonical variable names.

## Legacy Variable Names

The old CHMI-style export names

- `T`
- `TMA`
- `TMI`
- `F`
- `E`
- `SSV`

are no longer used in the final exported dataset for this example.

No separate legacy export mode is kept in this step.

## Representative Station Row Rule

If station metadata contain duplicate rows for the same canonical `station_id`, the workflow keeps the first matching row in the existing metadata order.

This makes the workflow stable and prevents repeated processing for the same station.

## Example Commands

Cache raw inputs only for CZ:

```powershell
python examples/download_fao.py --country CZ --mode download --cache-dir outputs/fao_cache
```

Build only from cache and export Parquet for DE:

```powershell
python examples/download_fao.py --country DE --mode build --cache-dir outputs/fao_cache --export-format parquet --output-dir outputs/fao_daily_bundle
```

Run the full workflow and export both outputs:

```powershell
python examples/download_fao.py --country CZ --mode full --cache-dir outputs/fao_cache --export-format both --output outputs/fao_daily.mat --output-dir outputs/fao_daily_bundle
```

## Why This Stays In `examples/`

This workflow is intentionally downstream-specific.

The reusable parts stay in the core library:

- provider-aware metadata loading
- canonical element handling
- country-aware daily observation downloading
- DataFrame export helpers

The FAO preparation orchestration stays in the example layer because it is a specialized downstream workflow, not a general-purpose downloader API.
