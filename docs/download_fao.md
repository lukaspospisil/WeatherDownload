# MATLAB-Oriented FAO Workflow

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

`examples/download_fao.py` is a workflow-oriented example built on top of the core WeatherDownload library.

It is country-aware, but only for countries whose daily FAO-prep mapping is explicitly implemented.

Currently supported:

- `CZ`
- `DE`
- `AT`

## CLI

The example now accepts:

```powershell
python examples/download_fao.py --country CZ
python examples/download_fao.py --country DE
python examples/download_fao.py --country AT
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

These canonical names are also the final exported variable names in the station series and Parquet bundle.

## Final Export Schema

The final per-station series expose:

- `date`
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

This keeps the exported dataset country-independent and canonical-first.

## Provider Provenance Metadata

`data_info` includes `provider_element_mapping`, which records for each canonical exported variable:

- the raw/provider code or codes used for the selected country
- the country-specific selection rule, if any
- whether the exported variable is directly observed or unavailable in the shared example layer

When country-specific assumptions matter, `data_info` also includes an `assumptions` block.

## Country-Specific Mapping

### CZ

| Canonical export name | CHMI raw code | Selection rule | Status |
| --- | --- | --- | --- |
| `tas_mean` | `T` | `TIMEFUNC=AVG` | observed |
| `tas_max` | `TMA` | `TIMEFUNC=20:00` | observed |
| `tas_min` | `TMI` | `TIMEFUNC=20:00` | observed |
| `wind_speed` | `F` | `TIMEFUNC=AVG` | observed |
| `vapour_pressure` | `E` | `TIMEFUNC=AVG` | observed |
| `sunshine_duration` | `SSV` | `TIMEFUNC=00:00` | observed |

### DE

| Canonical export name | DWD raw code | Selection rule | Status |
| --- | --- | --- | --- |
| `tas_mean` | `TMK` | none | observed |
| `tas_max` | `TXK` | none | observed |
| `tas_min` | `TNK` | none | observed |
| `wind_speed` | `FM` | none | observed |
| `vapour_pressure` | `VPM` | none | observed |
| `sunshine_duration` | `SDK` | none | observed |

### AT

| Canonical export name | GeoSphere raw code(s) | Selection rule | Status |
| --- | --- | --- | --- |
| `tas_mean` | `tl_mittel` | none | observed |
| `tas_max` | `tlmax` | none | observed |
| `tas_min` | `tlmin` | none | observed |
| `wind_speed` | `vv_mittel` | none | observed |
| `vapour_pressure` | none | none | unavailable |
| `sunshine_duration` | `so_h` | none | observed |

For Austria, the shared workflow keeps the same exported bundle shape as CZ and DE, but `vapour_pressure` stays empty because it is not directly available from the current Austria daily provider path.

## Austria Assumptions

The Austria branch keeps several assumptions explicit in `data_info`:

- `wind_height_handling`: `wind_speed` uses GeoSphere daily `vv_mittel` as delivered and is not converted to FAO 2 m wind speed
- `pressure_usage`: GeoSphere daily pressure is available in the provider but is intentionally not included in this shared bundle
- `relative_humidity_interpretation`: GeoSphere daily `rf_mittel` exists in the provider, but the shared workflow does not use it to derive new variables
- `sunshine_duration_to_radiation`: GeoSphere daily `so_h` is used as observed sunshine duration; no radiation is derived

The example still does not compute FAO-56 ET0 or any FAO intermediate physics.

## Workflow Steps

1. load station metadata for the selected country
2. load observation metadata for the selected country
3. coarse-screen stations by required daily FAO-prep inputs
4. apply a coarse validity-overlap pre-screen from observation metadata
5. deduplicate candidate stations to one row per canonical `station_id`
6. download or reuse cached normalized daily observations for the selected country
7. apply country-specific daily selection rules only
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

The cache is country-scoped:

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
  AT/
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

Default country-aware output names when you do not pass explicit paths:

- `CZ` MAT: `outputs/fao_daily.cz.mat`
- `CZ` Parquet bundle: `outputs/fao_daily.cz`
- `DE` MAT: `outputs/fao_daily.de.mat`
- `DE` Parquet bundle: `outputs/fao_daily.de`
- `AT` MAT: `outputs/fao_daily.at.mat`
- `AT` Parquet bundle: `outputs/fao_daily.at`

### MAT bundle

The `.mat` export contains:

- `data_info`
- `stations`
- `series`

### Parquet bundle

The Parquet bundle directory contains:

- `data_info.json`
- `stations.parquet`
- `series.parquet`

`series.parquet` is long-form and contains only complete FAO-prep days with canonical variable names.

`stations.parquet` uses normalized snake_case fields:

- `station_id`
- `full_name`
- `latitude`
- `longitude`
- `elevation_m`
- `num_complete_days`
- `first_complete_date`
- `last_complete_date`

`series.parquet` uses:

- `station_id`
- `full_name`
- `latitude`
- `longitude`
- `elevation_m`
- `date`
- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

## Legacy Variable Names

The old CHMI-style export names

- `T`
- `TMA`
- `TMI`
- `F`
- `E`
- `SSV`

are not used in the final exported dataset.

## Representative Station Row Rule

If station metadata contain duplicate rows for the same canonical `station_id`, the workflow keeps the first matching row in the existing metadata order.

This makes the workflow stable and prevents repeated processing for the same station.

## Example Commands

Cache raw inputs only for CZ:

```powershell
python examples/download_fao.py --country CZ --mode download --cache-dir outputs/fao_cache
```

Build only from cache and export Parquet for AT:

```powershell
python examples/download_fao.py --country AT --mode build --cache-dir outputs/fao_cache --export-format parquet
```

Run the full workflow and export both outputs for DE:

```powershell
python examples/download_fao.py --country DE --mode full --cache-dir outputs/fao_cache --export-format both
```

## Why This Stays In `examples/`

This workflow is intentionally downstream-specific.

The reusable parts stay in the core library:

- provider-aware metadata loading
- canonical element handling
- country-aware daily observation downloading
- DataFrame export helpers

The FAO preparation orchestration stays in the example layer because it is a specialized downstream workflow, not a general-purpose downloader API.

