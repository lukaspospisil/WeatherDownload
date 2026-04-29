# NOAA GHCN-Daily Direct-Prefix Wrappers

This note covers the shared NOAA GHCN-Daily wrapper pattern used for the current `FI`, `FR`, `IT`, `NO`, and `NZ` country adapters. These notes stay intentionally short because the runtime logic lives in the shared helper under `weatherdownload/providers/ghcnd/`.

## Provider identifiers

- country: `FI`, `FR`, `IT`, `NO`, `NZ`
- provider: `ghcnd`
- backward-compatible `dataset_scope`: `ghcnd`
- resolution(s): `daily`

## Source

- official source: NOAA NCEI GHCN-Daily
- station metadata: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- observations: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

## Station identifiers

- `station_id` is the raw NOAA GHCN-Daily station ID
- country filtering stays explicit in the wrapper configuration
- station metadata and station elements are inventory-driven from `ghcnd-inventory.txt`

These wrappers are grouped together because the GHCN country prefix matches the WeatherDownload country code directly:

- `FI -> FI`
- `FR -> FR`
- `IT -> IT`
- `NO -> NO`
- `NZ -> NZ`

Mapped-prefix wrappers such as `CZ -> EZ` and `DE -> GM` are documented separately in [NOAA GHCN-Daily Mapped-Prefix Wrappers](ghcnd_mapped_prefix_wrappers.md). `CA` and `MX` also keep short country-specific notes even though their current station-id pattern already matches their wrapper code.

## Supported data

All wrappers in this note expose the same conservative `daily` core:

- `TAVG` -> `tas_mean`
- `TMAX` -> `tas_max`
- `TMIN` -> `tas_min`
- `PRCP` -> `precipitation`
- `SNWD` -> `snow_depth`

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- `TAVG`, `TMAX`, and `TMIN`: tenths of degrees C -> degrees C
- `PRCP`: tenths of mm -> mm
- `SNWD`: mm -> mm
- NOAA missing code `-9999` is treated as missing

## Limitations and caveats

- these wrappers are intentionally thin and share parser, metadata, inventory, and observation logic
- the shared helper now supports both direct-prefix wrappers (`FI -> FI`) and mapped-prefix wrappers (`CZ -> EZ`) using the same configuration pattern
- station-level availability is inventory-driven and can differ by station
- `tas_mean` comes only from raw NOAA `TAVG`; this wrapper does not derive a mean from `TMAX` and `TMIN`
- `snowfall` is intentionally unsupported because there is no existing canonical snowfall element wired for GHCN in this pass
- `open_water_evaporation` is intentionally unsupported on these wrappers
- the shared wrapper audit did not justify exposing raw `EVAP` for this group

## Examples

```powershell
weatherdownload stations elements --country FI --provider ghcnd --resolution daily --include-mapping
```

```powershell
weatherdownload observations daily --country NZ --provider ghcnd --station-id NZ000093844 --start-date 2024-01-01 --end-date 2024-01-03 --element precipitation
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
