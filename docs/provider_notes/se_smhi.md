# SMHI Sweden

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative Sweden slice built from official SMHI meteorological observations. The provider keeps SMHI's parameter and period model behind the shared WeatherDownload interface.

## Provider identifiers

- country: `SE`
- provider: `historical`
- backward-compatible `dataset_scope`: `historical`
- resolution(s): `daily`, `1hour`

## Source

- official source: SMHI Meteorological Observations open-data API
- parameter metadata: `https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{parameter_id}.json`
- observations: `https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv`

The implemented historical paths use only `period="corrected-archive"`.

## Station identifiers

- `station_id` is the official SMHI station ID from the parameter station listings
- `gh_id` remains null on this path
- station metadata come from the supported-parameter station listings

## Supported data

Current source-backed mapping includes:

- `daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`
- `1hour`: `tas_mean`, `wind_speed`, `relative_humidity`, `precipitation`, `pressure`

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official SMHI values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- corrected-archive excludes the latest three months by source design
- daily `observation_date` comes from the published `Representativt dygn` column
- hourly `timestamp` is taken directly from the published `Datum` and `Tid (UTC)` columns
- raw SMHI `Kvalitet` codes are preserved in `flag`
- `quality` remains null
- `10min` is intentionally unsupported; official hourly outputs derived from 10-minute sampling and official 15-minute parameters are not treated as true `resolution="10min"` observations

## Examples

```powershell
weatherdownload stations elements --country SE --provider historical --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
