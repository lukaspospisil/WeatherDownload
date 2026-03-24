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
python examples/download_daily.py --country NL
```

BE notes:

- `BE` uses the shared daily example path through the official RMI/KMI `aws_1day` daily layer
- daily values are official provider-side daily aggregates from 10-minute data
- the documented daily grouping window is from `00:10` on day `D` to `00:00` on day `D+1`
- WeatherDownload does not recompute those aggregates in this pass
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null
- `BE` currently supports `historical / daily` and `historical / 10min`; hourly remains out of scope

NL notes:

- set `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY` first
- only `historical / daily` is implemented
- hourly and EDR are intentionally out of scope for this pass

### `examples/download_hourly.py`

Shows how to:

- build an hourly query
- use timestamp-based semantics
- download normalized hourly observations

Run:

```powershell
python examples/download_hourly.py
```

### `examples/download_tenmin.py`

Shows how to:

- build a 10-minute query
- use timestamp-based semantics
- download normalized 10-minute observations

Run:

```powershell
python examples/download_tenmin.py
python examples/download_tenmin.py --country BE
python examples/download_tenmin.py --country DE
```

BE 10-minute notes:

- `BE` uses the shared 10-minute example path through the official RMI/KMI `aws_10min` layer
- the example preserves the published provider timestamps and does not reinterpret them into a different meteorological meaning
- mapped fields stay source-backed only; no hourly or daily aggregates are recomputed from Belgium 10-minute data
- raw `qc_flags` are preserved in `flag` and normalized `quality` stays null

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

It prepares a clean daily FAO-prep dataset for later MATLAB, R, or Python processing.

What it does:

- supports `CZ`, `DE`, `AT`, `BE`, and `NL`
- caches normalized country-aware daily inputs
- screens candidate stations
- applies country-specific daily selection rules
- keeps only complete days
- exports a MATLAB-oriented bundle, a Parquet bundle, or both

Important boundary:

- `BE` is included through the shared country-parameterized workflow using only observed Belgium daily inputs from the existing provider
- Belgium daily values come from the official provider-side `aws_1day` aggregation and are not recomputed from 10-minute data in the workflow
- `vapour_pressure` remains null for `BE` because the current provider path does not expose it directly, and the workflow does not derive it
- `NL` is included through the shared country-parameterized workflow using only observed KNMI daily inputs
- `vapour_pressure` remains null for `NL` because the current provider path does not expose it directly, and the workflow does not derive it

## Recommended Reading Order

1. start with the root [README](../README.md)
2. check [Provider Model And Coverage](providers.md)
3. check [Canonical Elements](canonical_elements.md)
4. check [Normalized Output Schemas](output_schema.md)
5. then use the example scripts from this page

