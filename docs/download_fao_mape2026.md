# MAPE 2026 CZ FAO Workflow

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

`examples/workflows/download_fao_mape2026.py` is a conference- and project-specific workflow for preparing daily Czech station data for MAPE 2026.

It is designed for comparing computed FAO-56 reference evapotranspiration against measured open-water evaporation. It is not a generic all-country FAO downloader.

## Where To Start

- Main project entry point: [README](../README.md)
- Shared generic FAO workflow: [FAO-Oriented Daily Input Packaging Workflow](download_fao.md)
- Shared example overview: [Examples And Workflows](examples.md)
- Provider model and country limits: [Provider Model](providers.md)
- Source-specific provider notes: [Provider Notes](provider_notes/README.md)

## Purpose

This workflow:

- downloads normalized daily `CZ / historical_csv / daily` observations for a fixed MAPE 2026 station set by default
- filters Czech daily `time_function` ambiguity before pivoting to a wide daily table
- computes FAO-56 daily reference evapotranspiration from the wide table
- exports both full reproducibility tables and a narrower analysis-ready table

Important boundary:

- this is a Czech Republic workflow only
- it is aimed at the MAPE 2026 comparison task
- it prepares data for comparing FAO-56 reference evapotranspiration with measured open-water evaporation
- it is not a generic all-country FAO packaging workflow

## Fixed Station Set

By default, the workflow uses this fixed 23-station MAPE 2026 set:

- `0-20000-0-11438` Tušimice
- `0-20000-0-11502` Ústí nad Labem Kočkov
- `0-20000-0-11509` Doksany
- `0-20000-0-11520` Praha Libuš
- `0-20000-0-11450` Plzeň Mikulka
- `0-20000-0-11406` Cheb
- `0-20000-0-11603` Liberec
- `0-20000-0-11423` Přimda
- `0-20000-0-11487` Kocelovice Nový Dvůr
- `0-20000-0-11628` Košetice
- `0-20000-0-11643` Pec pod Sněžkou
- `0-20000-0-11659` Přibyslav Keřkov
- `0-20000-0-11683` Svratouch
- `0-20000-0-11636` Kostelní Myslová
- `0-20000-0-11679` Ústí nad Orlicí
- `0-20000-0-11693` Dukovany
- `0-20000-0-11698` Kuchařovice
- `0-20000-0-11710` Luká
- `0-20000-0-11723` Brno Černovice
- `0-203-0-20201031001` Nové Heřminovy
- `0-20000-0-11766` Červená
- `0-20000-0-11774` Holešov
- `0-203-0-11790` Ostrava Poruba

If you pass one or more `--station-id` values, they replace the default 23-station set.

## Downloaded Daily Elements

The workflow downloads these normalized daily elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`
- `relative_humidity`
- `pressure`
- `open_water_evaporation`

Wide-table note:

- `pressure` is renamed to `pressure_observed` in the wide output so it is not confused with FAO pressure computed from elevation

Evaporation note:

- `open_water_evaporation` is the measured open-water evaporation or evaporimeter value
- it is not an input to the FAO-56 calculation
- it is the comparison target against computed `E_FAO`

## CZ `time_function` Filtering

The Czech `historical_csv / daily` source can contain multiple rows for the same `station_id + date + element`, for example:

- intended daily `AVG` rows
- observation-hour rows such as `07:00`, `14:00`, or `21:00`

Before wide conversion, the workflow filters the intended daily `time_function` for selected elements:

- `tas_mean` -> `AVG`
- `wind_speed` -> `AVG`
- `vapour_pressure` -> `AVG`
- `relative_humidity` -> `AVG`
- `pressure` -> `AVG`

After that filter:

- `observations_to_wide()` still stays strict
- if duplicated `station/date/element` keys remain after filtering, the workflow raises an error instead of silently picking one row

This is intentional. The workflow treats the filtered long table as a strict prerequisite for the wide daily table.

## FAO-56 Computation

Reusable FAO-56 equations live in [weatherdownload/fao.py](../weatherdownload/fao.py).

The workflow computes FAO-56 daily reference evapotranspiration from the wide table with:

- `wind_measurement_height_m=10.0`
- `use_observed_pressure=False`

That means:

- observed `pressure_observed` is preserved in the exported tables
- FAO pressure is computed separately from station elevation by default
- daily soil heat flux `G` is assumed to be zero

Important output columns:

- `E_FAO_raw`: raw FAO-56 Penman-Monteith result before nonnegative clipping
- `E_FAO`: clipped nonnegative final FAO-56 reference evapotranspiration used for analysis
- `vpd_raw_kpa`: raw `es_kpa - ea_kpa`
- `vpd_kpa`: clipped nonnegative vapour-pressure deficit used in the FAO calculation
- `ea_kpa`: actual vapour pressure in `kPa`
- `es_kpa`: saturation vapour pressure in `kPa`
- `pressure_fao_kpa`: FAO atmospheric pressure estimated from elevation
- `gamma_kpa_per_c`: psychrometric constant
- `Rs_MJ_m2_day`: incoming solar radiation from sunshine duration
- `Rn_MJ_m2_day`: net radiation
- `u2_m_s`: wind speed adjusted to the FAO 2 m reference height

Unit interpretation:

- `vapour_pressure` from `CZ / historical_csv / daily` is interpreted as `hPa` for CHMI and converted internally to `kPa` for FAO
- `pressure_observed` stays as the observed daily pressure variable in the wide tables
- `pressure_fao_kpa` is the separately computed FAO pressure used by the FAO equations when `use_observed_pressure=False`

## Outputs

The workflow writes these outputs under `--output-dir`:

- `fao_mape2026_daily_wide.csv`
- `fao_mape2026_daily_wide.parquet`
- `fao_mape2026_daily_wide_with_fao.csv`
- `fao_mape2026_daily_wide_with_fao.parquet`
- `fao_mape2026_analysis_ready.csv`
- `fao_mape2026_analysis_ready.parquet`
- `fao_mape2026_stations.csv`
- `fao_mape2026_stations.parquet`
- `fao_mape2026_summary.csv`

Meaning:

- `daily_wide` is the full wide meteorological table without FAO columns
- `daily_wide_with_fao` is the full wide table plus FAO intermediates and `E_FAO`
- `analysis_ready` contains only rows where both `E_FAO` and `open_water_evaporation` are available together with the required FAO input variables
- `analysis_ready` is the recommended table for comparing computed FAO values with measured evaporimeter evaporation
- `stations` contains the selected station metadata used by the workflow
- `summary` contains station-level completeness and overlap counts

Practical note:

- the full historical CSV exports can be large
- `--no-full-csv` is often a good default for routine reruns when Parquet plus the analysis-ready CSV is enough

## CLI Examples

Basic full run:

```powershell
python examples/workflows/download_fao_mape2026.py --output-dir outputs/mape2026
```

Recommended run without huge full CSV:

```powershell
python examples/workflows/download_fao_mape2026.py --output-dir outputs/mape2026 --no-full-csv
```

Short smoke test:

```powershell
python examples/workflows/download_fao_mape2026.py --station-id 0-20000-0-11406 --start-date 2023-01-01 --end-date 2023-01-31 --output-dir outputs/mape2026_smoke
```

Useful flags:

- `--station-id` overrides the default 23-station set
- `--station-file` can provide station ids from a TXT or CSV file with a `station_id` column
- `--cache-dir` controls the per-station raw and filtered cache location
- `--force-refresh` ignores cache and redownloads or recomputes station results
- `--debug-duplicates` enables expensive pre-filter duplicate diagnostics and is not recommended for normal full runs
- `--analysis-start-date` applies only to the final analysis-ready export
- `--no-full-csv` skips the largest full reproducibility CSV files
- `--no-parquet` skips Parquet exports

## Cache Behavior

The workflow uses station-level cache files for both:

- raw downloaded long observations
- filtered long observations after CZ `time_function` cleanup

Repeated-run behavior:

- the filtered cache is preferred first
- if a filtered cache file is present, the workflow can skip both re-downloading and re-filtering for that station
- if only a raw cache file is present, the workflow can reuse it and rerun only the filtering step

This keeps repeated runs practical for large Czech historical CSV downloads.

## Summary Output

`fao_mape2026_summary.csv` contains station-level completeness and overlap fields such as:

- `n_complete_fao_rows`: rows where the required FAO input columns are all present
- `n_complete_extended_rows`: rows where the extended tracked columns are all present, including `relative_humidity`, `pressure_observed`, and `open_water_evaporation`
- `n_complete_fao_output_rows`: rows where final `E_FAO` is available
- `n_open_water_evaporation_rows`: rows where measured `open_water_evaporation` is available
- `n_e_fao_rows`: rows where `E_FAO` is available
- `n_overlap_e_fao_open_water_rows`: rows where both `E_FAO` and `open_water_evaporation` are available together
- `n_rows_analysis_ready`: rows that pass the final analysis-ready mask
- `first_analysis_ready_date`: first date present in the analysis-ready table for that station
- `last_analysis_ready_date`: last date present in the analysis-ready table for that station
- `n_negative_E_FAO_raw_rows`: rows where raw `E_FAO_raw` was negative before clipping

The summary also keeps:

- `first_date`
- `last_date`
- `n_days`
- `min_E_FAO_raw`
- `min_E_FAO`

## Warnings And Interpretation

Important interpretation boundaries:

- `E_FAO` is FAO-56 reference evapotranspiration, not measured open-water evaporation
- `open_water_evaporation` is measured evaporation from the evaporimeter or open-water evaporation record
- `E_FAO` and `open_water_evaporation` are physically related but not identical quantities
- negative raw FAO values are preserved in `E_FAO_raw`
- final `E_FAO` is clipped to zero for analysis

This workflow is therefore suited to comparison and analysis, but not to treating the two evaporation quantities as interchangeable observations.

