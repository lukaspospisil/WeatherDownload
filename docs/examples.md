# Examples And Workflows

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page helps you find the example scripts quickly.

## Library Examples

### `examples/read_metadata.py`

Shows how to:

- load station metadata
- apply a simple filter
- inspect the normalized station table

Run:

```powershell
python examples/read_metadata.py
```

### `examples/download_daily.py`

Shows how to:

- build a daily observation query
- switch countries through the same public API
- use canonical element names
- download normalized daily observations

Run:

```powershell
python examples/download_daily.py
python examples/download_daily.py --country AT
python examples/download_daily.py --country BE
python examples/download_daily.py --country CZ
python examples/download_daily.py --country DE
python examples/download_daily.py --country DK
python examples/download_daily.py --country NL
python examples/download_daily.py --country SE
```

BE notes:

- `BE` uses the shared daily example path through the official RMI/KMI `aws_1day` daily layer
- daily values are official provider-side daily aggregates from 10-minute data
- the documented daily grouping window is from `00:10` on day `D` to `00:00` on day `D+1`
- WeatherDownload does not recompute those aggregates in this pass
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null
- `BE` currently supports `historical / daily`, `historical / 1hour`, and `historical / 10min` through the same shared examples and public resolution tokens

DK notes:

- `DK` uses the shared daily example path through the official DMI Climate Data `stationValue` collection
- station discovery uses the official DMI Climate Data `station` collection filtered to Denmark stations only
- the mapped daily parameters are documented by DMI as local-day Denmark values, and the example keeps that provider-defined meaning behind the provider layer
- raw source `qcStatus` and `validity` are preserved in `flag` and normalized `quality` stays null
- Greenland and Faroe Islands differences are intentionally out of scope for this pass

NL notes:

- set `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` first
- only `historical / daily` is implemented
- hourly and EDR are intentionally out of scope for this pass

SE notes:

- `SE` uses the shared daily example path through the official SMHI Meteorological Observations corrected-archive daily CSV path
- station discovery merges the supported daily and hourly parameter station listings used by this provider
- `observation_date` comes from the published `Representativt dygn` field, and provider-defined interval windows stay behind the provider layer
- raw SMHI `Kvalitet` codes are preserved in `flag` and normalized `quality` stays null
- corrected-archive excludes the latest three months by source design

### `examples/download_hourly.py`

Shows how to:

- build a `1hour` query through the shared hourly example path
- use timestamp-based semantics with the shared `resolution="1hour"` provider path
- download normalized 1-hour observations

Run:

```powershell
python examples/download_hourly.py
python examples/download_hourly.py --country BE
python examples/download_hourly.py --country DE
python examples/download_hourly.py --country DK
python examples/download_hourly.py --country SE
```

BE hourly notes:

- `BE` uses the shared hourly example path through the official RMI/KMI `aws_1hour` layer and the public `resolution="1hour"` token
- hourly values are official provider-side aggregates from 10-minute data
- the documented hourly grouping window is from `(H-1):10` to `H:00` for hour `H`
- the example preserves the published hourly timestamps and does not recompute hourly values from 10-minute data
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null

DK hourly notes:

- `DK` uses the shared hourly example path through the official DMI Climate Data `stationValue` collection with `timeResolution=hour`
- station discovery still uses the official DMI Climate Data `station` collection filtered to Denmark stations only
- the source hourly path is UTC and exposes `from` and `to` interval bounds; the example preserves that provider-defined hourly meaning behind the provider layer
- raw source `qcStatus` and `validity` are preserved in `flag` and normalized `quality` stays null
- Greenland and Faroe Islands differences are intentionally out of scope for this pass

SE hourly notes:

- `SE` uses the shared hourly example path through the official SMHI Meteorological Observations corrected-archive CSV path
- station discovery still uses the source-backed parameter station listings merged across the implemented Sweden parameters
- the example preserves the published `Datum` + `Tid (UTC)` hour timestamp as the normalized UTC `timestamp`
- raw SMHI `Kvalitet` codes are preserved in `flag` and normalized `quality` stays null
- corrected-archive excludes the latest three months by source design

### `examples/download_tenmin.py`

Shows how to:

- build a 10-minute query
- use timestamp-based semantics with the shared `resolution="10min"` provider path
- download normalized 10-minute observations

Run:

```powershell
python examples/download_tenmin.py
python examples/download_tenmin.py --country BE
python examples/download_tenmin.py --country DE
python examples/download_tenmin.py --country DK
```

BE 10-minute notes:

- `BE` uses the shared 10-minute example path through the official RMI/KMI `aws_10min` layer
- the example preserves the published provider timestamps and does not reinterpret them into a different meteorological meaning
- mapped fields stay source-backed only; no hourly or daily aggregates are recomputed from Belgium 10-minute data
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null

DK 10-minute notes:

- `DK` uses the shared 10-minute example path through the official DMI Meteorological Observation API `observation` collection
- station discovery still uses the official DMI Climate Data `station` collection filtered to Denmark stations only
- the source `observed` timestamp is preserved as the normalized `timestamp` in UTC, with provider-defined 10-minute field semantics left intact behind the provider layer
- the implemented metObs path is raw observation data; no hourly or daily values are recomputed from it
- source QC/status fields are not exposed on the implemented `10min` path, so `flag` remains null and normalized `quality` stays null
- Greenland and Faroe Islands differences are intentionally out of scope for this pass

### `examples/station_availability.py`

Shows how to:

- inspect implemented station paths
- inspect supported canonical elements
- check station support before downloading

Run:

```powershell
python examples/station_availability.py
```

### `examples/inspect_file.py`

Shows how to:

- inspect WeatherDownload output files from the terminal
- detect `.parquet`, `.csv`, `.mat`, `.json`, or a bundle directory automatically
- view file size, modification time, schema, missing values, and a small preview

Run:

```powershell
python examples/inspect_file.py outputs/some_file.parquet
python examples/inspect_file.py outputs/fao_daily.cz.mat
python examples/inspect_file.py outputs/fao_daily.cz
```

## Experimental Provider Example

### `examples/probe_shmu_sk.py`

Shows how to:

- probe the experimental SHMU Slovakia `recent / daily` observation feed
- cache the raw metadata JSON and one monthly daily JSON sample
- inspect the current feed summary offline via `probe_summary.csv`
- normalize one selected station/date slice into the library's canonical daily observation schema

## Workflow Example

### `examples/download_fao.py`

This is a workflow-oriented example rather than a generic library quickstart.

It prepares a clean daily FAO-prep bundle for later MATLAB, R, or Python processing.

What it does:

- supports `CZ`, `DE`, `AT`, `BE`, `DK`, `NL`, and `SE`
- downloads and caches normalized country-aware observed daily inputs
- screens candidate stations
- applies country-specific observed-input selection rules
- keeps only complete observed-input days
- exports a MATLAB-oriented bundle, a Parquet bundle, or both

Important boundary:

- `AT`, `BE`, `CZ`, `DE`, `DK`, `NL`, and `SE` all use the same shared country-parameterized workflow shape
- the workflow downloads, normalizes, filters, and packages observed daily inputs only by default
- it does not compute FAO-56 ET0, and derivation is only available through the explicit example-layer `--fill-missing allow-derived` mode
- if a field is unavailable in the current provider path, it remains null or missing in the default observed-only mode
- `BE` daily values come from the official provider-side `aws_1day` aggregation and are not recomputed from 10-minute data in the workflow
- `DK` is included through the shared workflow using only observed Denmark daily inputs from the existing provider; Denmark daily values come from the DMI Climate Data `stationValue` path and the workflow remains Denmark-only in this pass
- `NL` is included through the shared workflow using only observed KNMI daily inputs, and `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` is required
- `SE` is included through the shared workflow using only observed SMHI daily inputs from the corrected-archive daily path; wind_speed, vapour_pressure, and sunshine_duration remain null when they are unavailable in the current provider path

## Recommended Reading Order

1. start with the root [README](../README.md)
2. check [Provider Model And Coverage](providers.md)
3. check [Canonical Elements](canonical_elements.md)
4. check [Normalized Output Schemas](output_schema.md)
5. then use the example scripts from this page
