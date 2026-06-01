# FAO-Oriented Daily Input Packaging Workflow

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

`examples/workflows/download_fao.py` is a shared, country-aware workflow example built on top of the core WeatherDownload library.

## Where To Start

- Main project entry point: [README](../README.md)
- Shared example overview: [Examples And Workflows](examples.md)
- Provider model and country limits: [Provider Model](providers.md)
- Source-specific provider notes: [Provider Notes](provider_notes/README.md)
- Shared normalized station and observation columns: [Normalized Output Schemas](output_schema.md)

## MAPE 2026 CZ Workflow

For the separate Czech project-specific workflow that prepares the fixed MAPE 2026 station set, filters CZ daily `time_function` ambiguity, computes FAO-56 daily reference evapotranspiration, and exports analysis-ready open-water-evaporation comparison tables, see [MAPE 2026 CZ FAO Workflow](download_fao_mape2026.md).

Critical boundary:

- it does not compute FAO-56 ET0
- observed-only mode is the default
- it does not derive FAO-56 intermediate variables unless you explicitly enable `--compute-fao-intermediates`
- it only downloads, normalizes, filters, and packages observed daily meteorological inputs for later downstream FAO workflow use
- unavailable fields remain null or missing instead of being derived by default

Currently supported in the shared workflow:

- `CZ`
- `DE`
- `AT`
- `BE`
- `CH`
- `DK`
- `ES`
- `HU`
- `PL`
- `LU`
- `NL`
- `SE`

## CLI

```powershell
python examples/workflows/download_fao.py --country CZ
python examples/workflows/download_fao.py --country DE
python examples/workflows/download_fao.py --country AT
python examples/workflows/download_fao.py --country BE
python examples/workflows/download_fao.py --country CH
python examples/workflows/download_fao.py --country DK
python examples/workflows/download_fao.py --country ES
python examples/workflows/download_fao.py --country ES --fill-missing allow-derived
python examples/workflows/download_fao.py --country HU
python examples/workflows/download_fao.py --country PL
python examples/workflows/download_fao.py --country PL --fill-missing allow-hourly-aggregate
python examples/workflows/download_fao.py --country LU
python examples/workflows/download_fao.py --country LU --fill-missing allow-derived
python examples/workflows/download_fao.py --country NL
python examples/workflows/download_fao.py --country SE
python examples/workflows/download_fao.py --country NL --fill-missing allow-derived
python examples/workflows/download_fao.py --country CZ --compute-fao-intermediates
```

`--country` uses ISO 3166-1 alpha-2 codes and defaults to `CZ`.

For `ES`, use:

```powershell
python examples/workflows/download_fao.py --country ES --fill-missing allow-derived
```

The current shared workflow uses the fixed `aemet` daily provider path selected by `--country ES`; there is no separate `--provider` flag on this example CLI.

`--fill-missing` defaults to `none`. Use `--fill-missing allow-derived` or `--fill-missing allow-hourly-aggregate` only when you want the shared example layer to apply its documented opt-in fallback rules.

`--compute-fao-intermediates` defaults to off. When you enable it, the workflow keeps the standard six-field observed/prepared bundle unchanged and appends FAO-56 derived columns to each exported station series in MAT and Parquet outputs.

For `NL`, set `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` first.

For `ES`, set `WEATHERDOWNLOAD_AEMET_API_KEY` or `AEMET_API_KEY` first.

For `CH`, `HU`, and `PL`, no extra API key is required for the current provider slices.

## Fixed Export Shape

The shared example always exports the same canonical bundle columns:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

This keeps the downstream bundle shape stable across countries.

Important interpretation:

- these are packaging targets, not a promise that every country directly observes every field in the current provider path
- if a field is unavailable in the provider path, the shared example keeps it null in the default observed-only mode
- only the explicit opt-in fill policy may apply a documented fallback rule, and only for fields covered by that rule
- `Rn` is not downloaded from any provider in this workflow; it is derived only in explicit `--compute-fao-intermediates` mode by FAO-56 equations

## Optional FAO-56 Intermediate Computation

Default mode still exports only these six prepared input columns:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `vapour_pressure`
- `sunshine_duration`

With `--compute-fao-intermediates`, each completed station daily series additionally includes:

| Column | Units | Meaning |
| --- | --- | --- |
| `es` | `kPa` | mean saturation vapour pressure |
| `vpd` | `kPa` | vapour pressure deficit `es - ea` |
| `delta` | `kPa degC^-1` | slope of saturation vapour pressure curve |
| `pressure` | `kPa` | atmospheric pressure estimated from elevation |
| `gamma` | `kPa degC^-1` | psychrometric constant |
| `Ra` | `MJ m^-2 day^-1` | extraterrestrial radiation |
| `N` | `h day^-1` | maximum daylight hours |
| `Rs` | `MJ m^-2 day^-1` | incoming solar radiation from sunshine duration |
| `Rso` | `MJ m^-2 day^-1` | clear-sky solar radiation |
| `Rns` | `MJ m^-2 day^-1` | net shortwave radiation |
| `Rnl` | `MJ m^-2 day^-1` | net outgoing longwave radiation |
| `Rn` | `MJ m^-2 day^-1` | net radiation |
| `G` | `MJ m^-2 day^-1` | daily soil heat flux, fixed to `0` |
| `E_FAO` | `mm day^-1` | FAO-56 Penman-Monteith reference evapotranspiration |

These columns are not observed provider variables. They are example-layer `derived_fao56` outputs computed from the prepared daily inputs plus station metadata using standard FAO-56 daily equations. `Rn` is derived from the FAO-56 daily radiation equations, and `E_FAO` is derived from the FAO-56 Penman-Monteith equation. They are not downloaded provider observations.

Implementation notes:

- the workflow keeps the prepared six-field bundle unchanged and appends the derived columns only in opt-in mode
- the FAO-56 equations use actual vapour pressure in `kPa`; the current workflow converts the prepared bundle `vapour_pressure` values to `kPa` internally before deriving FAO-56 terms
- the longwave-radiation step clamps `Rs / Rso` into the FAO-safe range `[0.0, 1.0]` before it is used in `Rnl`
- if latitude, elevation, sunshine duration, vapour pressure, temperature inputs, or wind speed are missing, dependent derived outputs stay null instead of causing the workflow to crash

## Country Mapping Summary

### CZ

All exported fields are directly observed in the current shared path.

### DE

All exported fields are directly observed in the current shared path.

### AT

Observed inputs used:

- `tas_mean`
- `tas_max`
- `tas_min`
- `wind_speed`
- `sunshine_duration`

Unavailable in the current shared path:

- `vapour_pressure` stays null

### BE

Observed inputs used:

- `tas_mean` via `temp_avg`
- `tas_max` via `temp_max`
- `tas_min` via `temp_min`
- `wind_speed` via `wind_speed_10m`
- `sunshine_duration` via `sun_duration`

Unavailable in the current shared path:

- `vapour_pressure` stays null

The BE branch uses only the existing Belgium provider through the unified public interface. Belgium daily values come from the official provider-side `aws_1day` aggregation under the shared `resolution="daily"` path and are not recomputed from 10-minute data in this example.

### DK

Observed inputs used:

- `tas_mean` via `mean_temp`
- `tas_max` via `max_temp_w_date`
- `tas_min` via `min_temp`
- `wind_speed` via `mean_wind_speed`
- `sunshine_duration` via `bright_sunshine`

Unavailable in the current shared path:

- `vapour_pressure` stays null

The DK branch uses only the existing Denmark daily provider through the unified public interface. Denmark daily values come from the official DMI Climate Data `stationValue` path, and the workflow remains Denmark-only in this pass without broadening to Greenland or Faroe Islands support.

### ES

Observed inputs used from the `ES / aemet / daily` provider path:

- `tas_mean` via `tmed`
- `tas_max` via `tmax`
- `tas_min` via `tmin`
- `precipitation` via `prec`
- `wind_speed` via `velmedia`
- `relative_humidity` via `hrMedia`
- `sunshine_duration` via `sol`

Unavailable as an observed provider field in the current shared path:

- observed `vapour_pressure` stays null in default `fill_missing='none'` mode

Optional shared fallback in `--fill-missing allow-derived` mode:

- `vapour_pressure` may be filled from observed daily `tas_mean` plus observed daily `relative_humidity` through the existing shared example-layer fallback rule
- this is workflow-level `--fill-missing allow-derived` fallback rule compatibility only, not observed provider support
- the filled `vapour_pressure` values are marked as `derived_opt_in` in workflow provenance outputs, not observed provider data

The ES branch uses only the existing `ES / aemet / daily` provider through the unified public interface. It requires an AEMET OpenData API key via `WEATHERDOWNLOAD_AEMET_API_KEY` or `AEMET_API_KEY`. It does not add observed `vapour_pressure`, does not add hourly or 10-minute support, and does not move derivation logic into the provider.

Practical interpretation:

- `ES / aemet / daily` is usable by the FAO preparation workflow only when you explicitly enable `--fill-missing allow-derived`
- that compatibility is workflow-level only and does not make the Spain AEMET provider slice provider-level observed FAO-ready

### CH

Observed inputs used:

- `tas_mean` via `tre200d0`
- `tas_max` via `tre200dx`
- `tas_min` via `tre200dn`
- `wind_speed` via `fkl010d0`
- `vapour_pressure` via `pva200d0`
- `sunshine_duration` via `sre000d0`

Observed `vapour_pressure` is available in the current shared path and is exported directly when present.

Optional shared fallback in `--fill-missing allow-derived` mode:

- if observed `vapour_pressure` is missing on some rows, the existing shared example-layer fallback may fill it from observed daily `tas_mean` plus observed daily `relative_humidity`

The CH branch uses only the existing MeteoSwiss A1 daily provider slice through the unified public interface. This workflow prepares a clean observed daily input bundle for later FAO-oriented processing, does not compute FAO-56 ET0, does not derive radiation terms, and does not reinterpret MeteoSwiss provider-defined daily precipitation semantics.

### HU

Observed inputs used:

- `tas_mean` via `t`
- `tas_max` via `tx`
- `tas_min` via `tn`
- `wind_speed` via `fs`
- `sunshine_duration` via `f`

Unavailable in the current shared path:

- observed `vapour_pressure` stays null in default mode

Optional shared fallback in `--fill-missing allow-derived` mode:

- `vapour_pressure` may be filled from observed daily `tas_mean` plus observed daily `relative_humidity` through the existing shared example-layer fallback rule

The HU branch uses only the existing HungaroMet provider through the unified public interface. No new derivation rule is added, no ET0 computation is added, and no derivation logic is moved into the provider.

### LU

Observed inputs used from the separate `LU / asta / daily` provider path:

- `tas_mean` via `avg_ta200`
- `tas_max` via `max_ta200max`
- `tas_min` via `min_ta200min`
- `wind_speed` via `avg_wv200`
- `sunshine_duration` via `sum_ssd`

Observed helper input available in this provider path:

- `relative_humidity` via `avg_rh200`

Unavailable as an observed provider field in the current shared path:

- observed `vapour_pressure` stays null in default mode

Optional shared fallback in `--fill-missing allow-derived` mode:

- `vapour_pressure` may be filled from observed daily `tas_mean` plus observed daily `relative_humidity` through the existing shared example-layer fallback rule
- this is workflow-level `--fill-missing allow-derived` fallback rule compatibility only, not observed provider support
- the filled `vapour_pressure` values are marked as `derived_opt_in` in workflow provenance outputs, not observed provider data

The LU branch uses only the separate `LU / asta / daily` provider through the unified public interface. It does not merge or reinterpret the distinct `LU / meteolux / daily` Findel-only provider, does not add observed `vapour_pressure`, and does not move derivation logic into the provider.

Practical interpretation:

- `LU / asta / daily` is usable by the FAO preparation workflow only when you explicitly enable `--fill-missing allow-derived`
- that compatibility is workflow-level only and does not make the Luxembourg ASTA provider slice provider-level observed FAO-ready

### PL

Observed inputs used:

- `tas_mean` via `STD`
- `tas_max` via `TMAX`
- `tas_min` via `TMIN`
- `sunshine_duration` via `USL`

Unavailable in the current shared path:

- `wind_speed` stays null
- `vapour_pressure` stays null

Optional shared fallback in `--fill-missing allow-derived` mode:

- no additional PL field is filled in the current synop-backed slice, because observed daily `relative_humidity` is not exposed in that provider path

Optional hourly supplementation in `--fill-missing allow-hourly-aggregate` mode:

- `wind_speed` may be filled from official IMGW `historical / 1hour` `wind_speed`
- `vapour_pressure` may be filled from official IMGW `historical / 1hour` `vapour_pressure`
- both are aggregated as arithmetic means over the UTC calendar day
- both require at least 18 hourly observations for that day
- if that threshold is not met, the daily field stays missing
- supplemented values are labeled explicitly as `aggregated_hourly_opt_in` in workflow provenance outputs

The PL branch uses only the existing IMGW-PIB synop-backed daily provider slice by default and may optionally supplement it from the official IMGW synop-backed hourly slice through the unified public interface. It prepares a daily meteorological input bundle for later FAO-oriented processing, does not compute FAO-56 ET0, does not derive radiation terms, keeps `wind_speed` empty in default mode because the official synop daily fields `FF10` and `FF15` are duration-of-threshold wind indicators rather than wind-speed observations, keeps `vapour_pressure` empty in default mode because the implemented daily IMGW families do not publish daily relative humidity or vapour pressure for the shared fallback path, and keeps station coordinates and elevation missing because the implemented official IMGW station list does not provide clean source-backed values for those fields.

### NL

Observed inputs used:

- `tas_mean` via `TG`
- `tas_max` via `TX`
- `tas_min` via `TN`
- `wind_speed` via `FG`
- `sunshine_duration` via `SQ`

Unavailable in the current shared path:

- `vapour_pressure` stays null

The NL branch uses only the existing KNMI provider through the unified public interface.

### SE

Observed inputs used:

- `tas_mean` via `2`
- `tas_max` via `20`
- `tas_min` via `19`

Unavailable in the current shared path:

- `wind_speed` stays null
- `vapour_pressure` stays null
- `sunshine_duration` stays null

The SE branch uses only the existing SMHI daily provider through the unified public interface. It uses the official corrected-archive daily CSV path and packages observed daily temperature inputs only in this pass; even in optional fill mode, missing wind_speed and sunshine_duration remain missing because this shared example does not invent replacement observations.

## Fill Policy

Default behavior:

- `--fill-missing none`
- observed-only mode
- missing unavailable fields stay null or missing
- no ET0 or meteorological derivation happens

Optional behavior:

- `--fill-missing allow-derived`
- still no ET0 computation
- derivation is opt-in and stays in the shared example layer only, never in providers
- the current explicit fallback rule is limited to `vapour_pressure`
- `vapour_pressure` may be derived from observed daily `tas_mean` plus observed daily `relative_humidity` using the Magnus saturation-vapour-pressure formula in hPa
- when that fallback is used, the resulting `vapour_pressure` values are marked as `derived_opt_in` in workflow provenance outputs rather than observed provider data
- if the helper observations needed for that rule are unavailable, the field stays missing and the sidecar file records that outcome

- `--fill-missing allow-hourly-aggregate`
- still no ET0 computation
- hourly supplementation is opt-in and stays in the shared example layer only, never in providers
- the current explicit hourly aggregation path is limited to `PL`
- only `wind_speed` and `vapour_pressure` may be filled this way
- both are arithmetic means of official hourly observations over the UTC calendar day
- both require at least 18 hourly observations for that day
- supplemented values are marked explicitly as `aggregated_hourly_opt_in` in workflow provenance outputs

## Sidecar Info Files

Every export writes a matching plain-text UTF-8 `.info` sidecar.

Naming rule:

- take the export path
- remove its final extension if it has one
- append `.info`

Examples:

- `outputs/fao_daily.cz.mat` -> `outputs/fao_daily.cz.info`
- `outputs/fao_daily.cz` -> `outputs/fao_daily.info`

The sidecar is the export-level provenance record for the workflow. It records the selected fill policy, whether opt-in hourly aggregation and/or derived values were allowed, field-by-field observed/aggregated/derived/missing counts, the rule used for each field, and an explicit note that the workflow does not compute ET0.

When `--compute-fao-intermediates` is enabled, the sidecar also adds a `derived_fao56` block listing the appended derived columns and units and explicitly stating that `Rn` and `E_FAO` are derived workflow outputs, not downloaded provider observations. The existing observed/aggregated/derived/missing summaries for the original six prepared input fields remain unchanged.

## What The Example Does

1. load station metadata for the selected country
2. load station observation metadata for the selected country
3. screen stations by required observed daily inputs
4. estimate overlap from observation metadata
5. cache normalized daily observations through the shared provider interface
6. when explicitly requested by the selected fill policy, cache the documented optional hourly supplement inputs through the shared provider interface
7. keep only complete observed-input days for the configured required fields, leaving unavailable fields null rather than deriving them by default
8. apply only the documented opt-in fill rules that match the selected fill policy
9. only when explicitly requested by `--compute-fao-intermediates`, compute FAO-56 intermediate variables and `E_FAO` from the prepared daily inputs plus station metadata
10. package the result into a stable MAT or Parquet bundle shape
11. write a matching `.info` sidecar that records observed-versus-hourly-aggregated-versus-derived provenance for the original six fields and, when enabled, the appended `derived_fao56` fields

## What The Example Explicitly Does Not Do In Default Mode

- no ET0 computation
- no vapour-pressure derivation by default
- no RH-based derivation by default
- no net-radiation derivation
- no extraterrestrial-radiation derivation
- no psychrometric-constant computation
- no sunshine-to-radiation estimation
- no hidden meteorological estimation
- no derivation beyond the explicitly enabled and documented `--fill-missing allow-derived` fallback rule
- no hidden hourly-to-daily aggregation; `PL` hourly supplementation is available only through the explicit `--fill-missing allow-hourly-aggregate` mode

If you explicitly enable `--compute-fao-intermediates`, the example does compute FAO-56 intermediate radiation, psychrometric, and ET0 terms in the workflow layer only. Those appended columns are derived outputs, not downloaded provider observations.

## Metadata In `data_info`

`data_info` includes:

- `provider_element_mapping`
- `country`
- `source`
- `dataset_type`
- `elements`
- `min_complete_days`
- `num_stations`

If a country has important limitations, `data_info` also includes an `assumptions` block.

For `CH`, that assumptions block explicitly states that:

- the branch packages observed MeteoSwiss A1 daily inputs for later FAO-oriented processing only
- observed `vapour_pressure` is available in the current provider path and is used directly when present
- the shared optional fill mode may reuse the existing `tas_mean` plus `relative_humidity` fallback only when observed `vapour_pressure` is missing on some rows
- the workflow does not compute FAO-56 ET0 and does not reinterpret MeteoSwiss provider-defined daily precipitation semantics

For `BE`, that assumptions block explicitly states that:

- the branch packages observed inputs only
- `vapour_pressure` is unavailable in the current provider path and remains null
- Belgium daily values come from the provider-side `aws_1day` aggregation and are not recomputed in this example
- the example does not derive radiation or other meteorological variables

For `DK`, that assumptions block explicitly states that:

- the branch packages observed inputs only
- the workflow stays Denmark-only and does not broaden to Greenland or Faroe Islands differences in this pass
- `vapour_pressure` is unavailable in the current provider path and remains null
- the example does not derive radiation or other meteorological variables

For `HU`, that assumptions block explicitly states that:

- the branch packages observed inputs only by default
- observed `vapour_pressure` is unavailable in the current provider path and remains null in default mode
- observed Hungary daily `relative_humidity` may support only the existing opt-in shared `--fill-missing allow-derived` fallback rule for `vapour_pressure`
- the example does not derive radiation or other meteorological variables

For `ES`, that assumptions block explicitly states that:

- the branch packages observed AEMET daily inputs only by default
- observed `vapour_pressure` is unavailable in the current provider path and remains null in default mode
- observed Spain AEMET daily `relative_humidity` may support only the existing opt-in shared `--fill-missing allow-derived` fallback rule for `vapour_pressure`
- the example does not derive radiation or other meteorological variables

For `LU`, that assumptions block explicitly states that:

- the branch packages observed ASTA daily inputs only by default
- the branch uses only the separate `LU / asta / daily` provider path and does not merge MeteoLux Findel values into this workflow slice
- observed `vapour_pressure` is unavailable in the current provider path and remains null in default mode
- observed Luxembourg ASTA daily `relative_humidity` may support only the existing opt-in shared `--fill-missing allow-derived` fallback rule for `vapour_pressure`
- the example does not derive radiation or other meteorological variables unless you explicitly enable `--compute-fao-intermediates`

For `NL`, that assumptions block explicitly states that:

- the branch packages observed inputs only
- `vapour_pressure` is unavailable in the current provider path and remains null
- the example does not derive radiation or other meteorological variables

For `SE`, that assumptions block explicitly states that:

- the branch packages observed inputs only
- the current provider path uses the official SMHI corrected-archive daily CSV source, which excludes the latest three months by source design
- `wind_speed`, `vapour_pressure`, and `sunshine_duration` are unavailable in the current daily provider path and remain null
- the example does not derive radiation or other meteorological variables

## Cache Layout

The cache is country-scoped under the base cache directory, for example:

```text
<cache-dir>/
  CZ/
  DE/
  AT/
  BE/
  CH/
  DK/
  ES/
  HU/
  PL/
  LU/
  NL/
  SE/
```

Each country directory stores:

- `meta1.csv`
- `meta2.csv`
- `daily/<station_id>/daily-<station_id>.csv`

## Default Outputs

Default country-aware output names when you do not pass explicit paths:

- `CZ` MAT: `outputs/fao_daily.cz.mat`
- `DE` MAT: `outputs/fao_daily.de.mat`
- `AT` MAT: `outputs/fao_daily.at.mat`
- `BE` MAT: `outputs/fao_daily.be.mat`
- `CH` MAT: `outputs/fao_daily.ch.mat`
- `DK` MAT: `outputs/fao_daily.dk.mat`
- `ES` MAT: `outputs/fao_daily.es.mat`
- `HU` MAT: `outputs/fao_daily.hu.mat`
- `PL` MAT: `outputs/fao_daily.pl.mat`
- `LU` MAT: `outputs/fao_daily.lu.mat`
- `NL` MAT: `outputs/fao_daily.nl.mat`
- `SE` MAT: `outputs/fao_daily.se.mat`
- `CZ` Parquet bundle: `outputs/fao_daily.cz`
- `DE` Parquet bundle: `outputs/fao_daily.de`
- `AT` Parquet bundle: `outputs/fao_daily.at`
- `BE` Parquet bundle: `outputs/fao_daily.be`
- `CH` Parquet bundle: `outputs/fao_daily.ch`
- `DK` Parquet bundle: `outputs/fao_daily.dk`
- `ES` Parquet bundle: `outputs/fao_daily.es`
- `HU` Parquet bundle: `outputs/fao_daily.hu`
- `PL` Parquet bundle: `outputs/fao_daily.pl`
- `LU` Parquet bundle: `outputs/fao_daily.lu`
- `NL` Parquet bundle: `outputs/fao_daily.nl`
- `SE` Parquet bundle: `outputs/fao_daily.se`

## Why This Stays In `examples/workflows/`

The reusable parts stay in the core library:

- provider-aware metadata loading
- canonical element handling
- country-aware daily observation downloading
- export helpers

The orchestration stays in `examples/workflows/` because it is a downstream packaging workflow, not part of the public provider API.





