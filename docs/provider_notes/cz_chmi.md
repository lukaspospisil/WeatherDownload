# CHMI Czech Republic

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note covers the existing Czech Hydrometeorological Institute provider family for `CZ`. It is separate from the Czech NOAA GHCN-Daily wrapper because the two source families use different station identifiers and different source contracts.

## Provider identifiers

- country: `CZ`
- implemented provider: `historical_csv`
- additional registered provider tokens: `historical`, `recent`, `now`
- `provider`: same token values as `provider`
- implemented resolution(s): `daily`, `1hour`, `10min`

## Source

- official source: Czech Hydrometeorological Institute OpenData
- metadata: `https://opendata.chmi.cz/meteorology/climate/historical_csv/metadata/meta1.csv`
- observation metadata: `https://opendata.chmi.cz/meteorology/climate/historical_csv/metadata/meta2.csv`
- data product: historical CSV climate observations

## Station identifiers

- `station_id` is the CHMI WSI station identifier on this provider family
- `gh_id` preserves the CHMI station code where available
- these station identifiers are different from the raw GHCN station ids used on `CZ / ghcnd / daily`

## Supported data

- the implemented public path is currently `CZ / historical_csv` for `daily`, `1hour`, and `10min`
- daily data includes measured `open_water_evaporation` via raw `VY`
- hourly and 10-minute paths expose the current conservative subsets documented in the capability matrix

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- WeatherDownload uses CHMI source units and parser rules directly for this family
- source-specific raw-to-canonical mappings depend on resolution and stay provider-specific

## Limitations and caveats

- `provider` is the public selector for the concrete source path
- `historical`, `recent`, and `now` remain registered provider tokens, but the currently implemented CHMI observation download path is `historical_csv`
- measured `open_water_evaporation` for Czech stations is supported here through raw `VY`, not through the Czech GHCN wrapper

## Examples

```powershell
weatherdownload observations daily --country CZ --provider historical_csv --station-id 0-20000-0-11406 --element open_water_evaporation --start-date 2024-01-01 --end-date 2024-01-10
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [NOAA GHCN-Daily Czech Republic](cz_noaa_ghcnd.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
