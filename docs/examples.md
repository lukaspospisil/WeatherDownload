# Examples And Workflows

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page helps you find the example scripts quickly.

## Layout

- `examples/basic/`: short user-facing library examples
- `examples/workflows/`: higher-level user-facing workflows built on the shared library API
- `scripts/dev/`: maintainer and debugging helpers that are useful for development, but not part of the normal example tour

## Where To Start

- Station metadata: [`examples/basic/read_metadata.py`](../examples/basic/read_metadata.py) and [Normalized Output Schemas](output_schema.md)
- Daily downloads: [`examples/basic/download_daily.py`](../examples/basic/download_daily.py)
- Subdaily downloads: [`examples/basic/download_hourly.py`](../examples/basic/download_hourly.py) and [`examples/basic/download_tenmin.py`](../examples/basic/download_tenmin.py)
- FAO-oriented daily input bundle: [`examples/workflows/download_fao.py`](../examples/workflows/download_fao.py) and [FAO-Oriented Daily Input Packaging Workflow](download_fao.md)
- Provider coverage and country-specific limits: [Provider Model And Coverage](providers.md)

## Basic Examples

### `examples/basic/read_metadata.py`

Shows how to:

- load station metadata
- apply a simple filter
- inspect the normalized station table

Run:

```powershell
python examples/basic/read_metadata.py
```

### `examples/basic/download_daily.py`

Shows how to:

- build a daily observation query
- switch countries through the same public API
- use canonical element names
- download normalized daily observations

Run:

```powershell
python examples/basic/download_daily.py
python examples/basic/download_daily.py --country AT
python examples/basic/download_daily.py --country BE
python examples/basic/download_daily.py --country CH
python examples/basic/download_daily.py --country CZ
python examples/basic/download_daily.py --country DE
python examples/basic/download_daily.py --country DK
python examples/basic/download_daily.py --country HU
python examples/basic/download_daily.py --country NL
python examples/basic/download_daily.py --country PL
python examples/basic/download_daily.py --country SE
python examples/basic/download_daily.py --country US
```

BE notes:

- `BE` uses the shared daily example path through the official RMI/KMI `aws_1day` daily layer
- daily values are official provider-side daily aggregates from 10-minute data
- the documented daily grouping window is from `00:10` on day `D` to `00:00` on day `D+1`
- WeatherDownload does not recompute those aggregates in this pass
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null
- `BE` currently supports `historical / daily`, `historical / 1hour`, and `historical / 10min` through the same shared examples and public resolution tokens

CH notes:

- `CH` uses the shared daily example path through the official MeteoSwiss A1 station product
- station metadata and discovery use the official MeteoSwiss A1 metadata tables and STAC station assets
- daily observations stay source-backed only and preserve the provider-defined MeteoSwiss daily interval semantics behind the provider layer
- `gh_id` is populated from the official MeteoSwiss `station_wigos_id`
- raw `flag` and normalized `quality` both remain null on the implemented A1 slice
DK notes:

- `DK` uses the shared daily example path through the official DMI Climate Data `stationValue` collection
- station discovery uses the official DMI Climate Data `station` collection filtered to Denmark stations only
- the mapped daily parameters are documented by DMI as local-day Denmark values, and the example keeps that provider-defined meaning behind the provider layer
- raw source `qcStatus` and `validity` are preserved in `flag` and normalized `quality` stays null
- Greenland and Faroe Islands differences are intentionally out of scope for this pass

HU notes:

- `HU` uses the shared daily example path through the official HungaroMet open-data tree on `odp.met.hu`
- station metadata use the official `climate/observations_hungary/meta/station_meta_auto.csv` file
- daily observations use the official `climate/observations_hungary/daily/historical/` archives and the official `daily/recent/` current-year archives when needed by the requested date range
- raw HungaroMet `Q_<field>` values are preserved in `flag` and normalized `quality` stays null
- `HU` currently supports `historical / daily`, `historical / 1hour`, and `historical / 10min` through the shared examples
PL notes:

- `PL` uses the shared daily example path through the official IMGW-PIB public `historical / daily` synop slice by default; the separate `historical_klimat / daily` scope is documented in the provider notes
- station metadata use the official `dane_meteorologiczne/wykaz_stacji.csv` station list, with the 5-character IMGW station code as canonical `station_id`
- daily observations use deterministic yearly station archives for completed years and current-year monthly all-station archives when the requested range reaches the current year
- raw IMGW daily status codes such as `WSTD`, `WSMDB`, and `WUSL` are preserved in `flag`, while normalized `quality` stays null
- `PL` currently supports `historical / daily` and `historical / 1hour` in the shared example defaults, while the separate `historical_klimat / daily` scope is available through the same public API when requested explicitly

NL notes:

- set `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` first
- `daily` and `1hour` are implemented through the official KNMI validated datasets
- `10min` is implemented through the official KNMI near-real-time Open Data path and is not documented as validated in the same way
- KNMI EDR remains out of scope in this pass

SE notes:

- `SE` uses the shared daily example path through the official SMHI Meteorological Observations corrected-archive daily CSV path
- station discovery merges the supported daily and hourly parameter station listings used by this provider
- `observation_date` comes from the published `Representativt dygn` field, and provider-defined interval windows stay behind the provider layer
- raw SMHI `Kvalitet` codes are preserved in `flag` and normalized `quality` stays null
- corrected-archive excludes the latest three months by source design

US notes:

- `US` uses the shared daily example path through the official NOAA NCEI GHCN-Daily station files
- the current `US` slice is intentionally limited to `dataset_scope="ghcnd"` and `resolution="daily"`
- `open_water_evaporation` maps to official NOAA raw `EVAP`
- NOAA documents `EVAP` as evaporation of water from an evaporation pan
- NOAA raw `EVAP` values are in tenths of `mm`; WeatherDownload normalizes output `value` to `mm`
- multiday `MDEV`, ET0, PET, FAO reference evaporation, and modeled evaporation remain unsupported on this slice

### `examples/basic/download_hourly.py`

Shows how to:

- build a `1hour` query through the shared hourly example path
- use timestamp-based semantics with the shared `resolution="1hour"` provider path
- download normalized 1-hour observations

Subdaily interpretation note:

- if a country does not implement a requested `1hour` field, that field is unavailable for that provider slice rather than backfilled
- if a returned `1hour` row has no usable source value, `value` stays null for that row
- raw provider QC/status may appear in `flag`; normalized `quality` may still remain null

Run:

```powershell
python examples/basic/download_hourly.py
python examples/basic/download_hourly.py --country AT
python examples/basic/download_hourly.py --country BE
python examples/basic/download_hourly.py --country CH
python examples/basic/download_hourly.py --country DE
python examples/basic/download_hourly.py --country DK
python examples/basic/download_hourly.py --country HU
python examples/basic/download_hourly.py --country NL
python examples/basic/download_hourly.py --country PL
python examples/basic/download_hourly.py --country SE
```

AT hourly notes:

- `AT` uses the shared hourly example path through the official GeoSphere Austria `klima-v2-1h` station dataset and the public `resolution="1hour"` token
- station discovery still uses the official GeoSphere station metadata path used by the Austria provider
- the example preserves the published GeoSphere hourly `time` value as the normalized UTC `timestamp`
- raw GeoSphere hourly `<parameter>_flag` values are preserved in `flag` and normalized `quality` stays null
- Austria `10min` support is implemented separately through the shared `examples/basic/download_tenmin.py` path

BE hourly notes:

- `BE` uses the shared hourly example path through the official RMI/KMI `aws_1hour` layer and the public `resolution="1hour"` token
- hourly values are official provider-side aggregates from 10-minute data
- the documented hourly grouping window is from `(H-1):10` to `H:00` for hour `H`
- the example preserves the published hourly timestamps and does not recompute hourly values from 10-minute data
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null

CH hourly notes:

- `CH` uses the shared hourly example path through the official MeteoSwiss A1 station assets
- the example preserves the published MeteoSwiss UTC reference timestamp as the normalized `timestamp`
- mapped fields stay source-backed only; WeatherDownload does not recompute hourly values from any other Swiss product
- `gh_id` is populated from the official MeteoSwiss `station_wigos_id`
- raw `flag` and normalized `quality` both remain null on the implemented A1 slice
DK hourly notes:

- `DK` uses the shared hourly example path through the official DMI Climate Data `stationValue` collection with `timeResolution=hour`
- station discovery still uses the official DMI Climate Data `station` collection filtered to Denmark stations only
- the source hourly path is UTC and exposes `from` and `to` interval bounds; the example preserves that provider-defined hourly meaning behind the provider layer
- raw source `qcStatus` and `validity` are preserved in `flag` and normalized `quality` stays null
- Greenland and Faroe Islands differences are intentionally out of scope for this pass

HU hourly notes:

- `HU` uses the shared hourly example path through the official HungaroMet `climate/observations_hungary/hourly/` tree
- station discovery still uses the official `meta/station_meta_auto.csv` metadata file used by the Hungary provider
- the example preserves the published HungaroMet hourly `Time` value as the normalized UTC `timestamp`
- raw HungaroMet `Q_<field>` values are preserved in `flag` and normalized `quality` stays null
- mapped fields stay source-backed only; WeatherDownload does not recompute hourly values from any other Hungary source path

PL hourly notes:

- `PL` uses the shared hourly example path through the official IMGW-PIB `dane_meteorologiczne/terminowe/synop` archive
- station discovery still uses the official `dane_meteorologiczne/wykaz_stacji.csv` station list used by the Poland provider
- the implemented hourly `timestamp` is built from the published `ROK`, `MC`, `DZ`, and `GG` fields and treated as UTC in this first slice
- raw IMGW hourly status codes such as `WTEMP`, `WFWR`, `WPORW`, `WWLGW`, `WCPW`, and `WPPPS` are preserved in `flag`, while normalized `quality` stays null
- this slice adds official subdaily observations only; it does not aggregate them into daily FAO inputs and does not compute FAO-56 ET0

NL hourly notes:

- `NL` uses the shared hourly example path through the official KNMI Open Data API `hourly-in-situ-meteorological-observations-validated` dataset
- set `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` first
- the example preserves the published KNMI hourly file timestamp as the normalized UTC `timestamp`
- provider-defined hourly element semantics stay behind the provider layer; mapped fields stay source-backed only
- raw `flag` and normalized `quality` both remain null in this slice

SE hourly notes:

- `SE` uses the shared hourly example path through the official SMHI Meteorological Observations corrected-archive CSV path
- station discovery still uses the source-backed parameter station listings merged across the implemented Sweden parameters
- the example preserves the published `Datum` + `Tid (UTC)` hour timestamp as the normalized UTC `timestamp`
- raw SMHI `Kvalitet` codes are preserved in `flag` and normalized `quality` stays null
- corrected-archive excludes the latest three months by source design

### `examples/basic/download_tenmin.py`

Shows how to:

- build a 10-minute query
- use timestamp-based semantics with the shared `resolution="10min"` provider path
- download normalized 10-minute observations

Subdaily interpretation note:

- if a country does not implement a requested `10min` field, that field is unavailable for that provider slice rather than backfilled
- if a returned `10min` row has no usable source value, `value` stays null for that row
- raw provider QC/status may appear in `flag`; normalized `quality` may still remain null

Run:

```powershell
python examples/basic/download_tenmin.py
python examples/basic/download_tenmin.py --country AT
python examples/basic/download_tenmin.py --country BE
python examples/basic/download_tenmin.py --country CH
python examples/basic/download_tenmin.py --country DE
python examples/basic/download_tenmin.py --country DK
python examples/basic/download_tenmin.py --country HU
python examples/basic/download_tenmin.py --country NL
```

AT 10-minute notes:

- `AT` uses the shared 10-minute example path through the official GeoSphere Austria `klima-v2-10min` station dataset and the public `resolution="10min"` token
- station discovery still uses the official GeoSphere station metadata path used by the Austria provider
- the example preserves the published GeoSphere `time` value as the normalized UTC `timestamp`
- raw GeoSphere 10-minute `<parameter>_flag` values are preserved in `flag` and normalized `quality` stays null
- mapped fields stay source-backed only; no hourly or daily values are recomputed from Austria 10-minute data

BE 10-minute notes:

- `BE` uses the shared 10-minute example path through the official RMI/KMI `aws_10min` layer
- the example preserves the published provider timestamps and does not reinterpret them into a different meteorological meaning
- mapped fields stay source-backed only; no hourly or daily aggregates are recomputed from Belgium 10-minute data
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null

CH 10-minute notes:

- `CH` uses the shared 10-minute example path through the official MeteoSwiss A1 station assets
- the example preserves the published MeteoSwiss UTC reference timestamp as the normalized `timestamp`
- mapped fields stay source-backed only; no hourly or daily values are recomputed from Swiss 10-minute data
- `gh_id` is populated from the official MeteoSwiss `station_wigos_id`
- raw `flag` and normalized `quality` both remain null on the implemented A1 slice
DK 10-minute notes:

- `DK` uses the shared 10-minute example path through the official DMI Meteorological Observation API `observation` collection
- station discovery still uses the official DMI Climate Data `station` collection filtered to Denmark stations only
- the source `observed` timestamp is preserved as the normalized `timestamp` in UTC, with provider-defined 10-minute field semantics left intact behind the provider layer
- the implemented metObs path is raw observation data; no hourly or daily values are recomputed from it
- source QC/status fields are not exposed on the implemented `10min` path, so `flag` remains null and normalized `quality` stays null
- Greenland and Faroe Islands differences are intentionally out of scope for this pass


HU 10-minute notes:

- `HU` uses the shared 10-minute example path through the official HungaroMet `climate/observations_hungary/10_minutes/` tree
- station discovery still uses the official `meta/station_meta_auto.csv` metadata file used by the Hungary provider
- the example preserves the published HungaroMet 10-minute `Time` value as the normalized UTC `timestamp`
- raw HungaroMet `Q_<field>` values are preserved in `flag` and normalized `quality` stays null
- the separate HungaroMet `10_minutes_wind` product is exposed through the distinct `dataset_scope="historical_wind"` capability in the library, but it is intentionally not merged into the shared default `10min` example path
- for an explicit HU / historical_wind / 10min Python download example and CLI discovery example, see [HungaroMet Hungary Provider Notes](providers_hu_hungaromet.md#wind-only-10-minute-example)

NL 10-minute notes:

- `NL` uses the shared 10-minute example path through the official KNMI Open Data API `10-minute-in-situ-meteorological-observations` dataset
- set `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` first
- the example preserves the published KNMI 10-minute file timestamp as the normalized UTC `timestamp`
- this KNMI path is official and source-backed, but it is a near-real-time dataset and is not documented as validated in the same way as the KNMI daily and hourly validated datasets
- mapped fields stay source-backed only; this first slice does not derive missing variables or recompute hourly or daily values from 10-minute data
- raw `flag` and normalized `quality` both remain null in this slice

## Workflow Examples

### `examples/workflows/station_availability.py`

Shows how to:

- inspect implemented station paths
- inspect supported canonical elements
- check station support before downloading

Run:

```powershell
python examples/workflows/station_availability.py
```

## Maintainer Utilities

### `scripts/dev/inspect_file.py`

Shows how to:

- inspect WeatherDownload output files from the terminal
- detect `.parquet`, `.csv`, `.mat`, `.json`, or a bundle directory automatically
- view file size, modification time, schema, missing values, and a small preview

Run:

```powershell
python scripts/dev/inspect_file.py outputs/some_file.parquet
python scripts/dev/inspect_file.py outputs/fao_daily.cz.mat
python scripts/dev/inspect_file.py outputs/fao_daily.cz
```

## Maintainer And Experimental Helpers

### `scripts/dev/probe_shmu_sk.py`

Shows how to:

- probe the experimental SHMU Slovakia `recent / daily` observation feed
- cache the raw metadata JSON and one monthly daily JSON sample
- inspect the current feed summary offline via `probe_summary.csv`
- normalize one selected station/date slice into the library's canonical daily observation schema

Run:

```powershell
python scripts/dev/probe_shmu_sk.py
```

## Workflow Examples

### `examples/workflows/download_fao.py`

This is a workflow-oriented example rather than a generic library quickstart.

It prepares a clean daily FAO-prep bundle for later MATLAB, R, or Python processing.

What it does:

- supports `CZ`, `DE`, `AT`, `BE`, `CH`, `DK`, `HU`, `PL`, `NL`, and `SE`
- downloads and caches normalized country-aware observed daily inputs
- screens candidate stations
- applies country-specific observed-input selection rules
- keeps only complete observed-input days
- exports a MATLAB-oriented bundle, a Parquet bundle, or both

Important boundary:

- `AT`, `BE`, `CH`, `CZ`, `DE`, `DK`, `HU`, `PL`, `NL`, and `SE` all use the same shared country-parameterized workflow shape
- the workflow downloads, normalizes, filters, and packages observed daily inputs only by default
- it does not compute FAO-56 ET0, and derivation is only available through the explicit example-layer `--fill-missing allow-derived` mode
- if a field is unavailable in the current provider path, it remains null or missing in the default observed-only mode
- `BE` daily values come from the official provider-side `aws_1day` aggregation and are not recomputed from 10-minute data in the workflow
- `DK` is included through the shared workflow using only observed Denmark daily inputs from the existing provider; Denmark daily values come from the DMI Climate Data `stationValue` path and the workflow remains Denmark-only in this pass
- `HU` is included through the shared workflow using only observed Hungary daily inputs from the existing provider; observed `vapour_pressure` is not exposed by the current Hungary provider slice and may be filled only through the existing opt-in shared `--fill-missing allow-derived` fallback rule when observed `tas_mean` and `relative_humidity` are available
- `PL` is included through the shared workflow using observed IMGW synop daily inputs by default; `wind_speed` and `vapour_pressure` remain null in the default slice, but the explicit `--fill-missing allow-hourly-aggregate` mode may supplement them from official IMGW `historical / 1hour` observations with provenance labeled as `aggregated_hourly_opt_in`
- `NL` is included through the shared workflow using only observed KNMI daily inputs, and `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` is required
- `SE` is included through the shared workflow using only observed SMHI daily inputs from the corrected-archive daily path; wind_speed, vapour_pressure, and sunshine_duration remain null when they are unavailable in the current provider path
- every shared FAO export writes a matching human-readable `.info` sidecar that records the selected fill policy and field-level observed/derived/missing counts

Detailed workflow behavior, fill policies, and sidecar provenance are documented in [FAO-Oriented Daily Input Packaging Workflow](download_fao.md).

## Recommended Reading Order

1. start with the root [README](../README.md)
2. check [Provider Model And Coverage](providers.md)
3. check [Canonical Elements](canonical_elements.md)
4. check [Normalized Output Schemas](output_schema.md)
5. then use the example scripts from this page





























