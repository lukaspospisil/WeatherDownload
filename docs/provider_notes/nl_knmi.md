# KNMI Netherlands

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative KNMI slice used for the Netherlands. The public API stays standard while KNMI-specific authentication and dataset-validation boundaries stay explicit.

## Provider identifiers

- country: `NL`
- provider: `historical`
- backward-compatible `dataset_scope`: `historical`
- resolution(s): `daily`, `1hour`, `10min`

## Source

- official source: KNMI Open Data API
- daily dataset: `daily-in-situ-meteorological-observations-validated`
- hourly dataset: `hourly-in-situ-meteorological-observations-validated`
- 10-minute dataset: `10-minute-in-situ-meteorological-observations`
- version: `1.0`

## Station identifiers

- `station_id` is the official KNMI station identifier from the source-backed station metadata CSV
- `gh_id` remains null on this path
- station metadata are retrieved through the KNMI Open Data API

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity`
- `1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`
- `10min`: `tas_mean`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official KNMI values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- KNMI access requires `WEATHERDOWNLOAD_KNMI_API_KEY` or `KNMI_API_KEY`
- `daily` and `1hour` use validated KNMI datasets
- `10min` uses the official near-real-time KNMI dataset and is not documented as validated in the same way
- daily timestamps are converted from the published end timestamp back to `observation_date`
- hourly and 10-minute timestamps preserve the published UTC timestamp
- `quality` and `flag` remain null unless future source-backed semantics are added
- KNMI EDR paths remain out of scope

## Examples

```powershell
weatherdownload stations elements --country NL --provider historical --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
