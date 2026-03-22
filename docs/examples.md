# Examples And Workflows

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This page helps you find the example scripts quickly.

## Library Examples

These examples are small and focused on the public library API.

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
- use canonical element names
- download normalized daily observations

Run:

```powershell
python examples/download_daily.py
```

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
```

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
python examples/inspect_file.py outputs/fao_daily.mat
python examples/inspect_file.py outputs/fao_daily_bundle
```

## Workflow Example

### `examples/download_fao.py`

This is a workflow-oriented example rather than a generic library quickstart.

It prepares a clean daily FAO-prep dataset for later MATLAB, R, or Python processing.

What it does:

- supports `CZ` and `DE`
- caches normalized country-aware daily inputs
- screens candidate stations
- applies country-specific daily selection rules
- keeps only complete days
- exports a MATLAB-oriented bundle, a Parquet bundle, or both
- writes canonical exported variables:
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `wind_speed`
  - `vapour_pressure`
  - `sunshine_duration`

Run:

```powershell
python examples/download_fao.py --country CZ --mode full --cache-dir outputs/fao_cache --export-format both --output outputs/fao_daily.mat --output-dir outputs/fao_daily_bundle
```

Detailed workflow notes:

- [MATLAB-Oriented FAO Workflow](download_fao.md)

## Recommended Reading Order

For a new user:

1. start with the root [README](../README.md)
2. check [Provider Model And Coverage](providers.md)
3. check [Canonical Elements](canonical_elements.md)
4. check [Normalized Output Schemas](output_schema.md)
5. then use the example scripts from this page
