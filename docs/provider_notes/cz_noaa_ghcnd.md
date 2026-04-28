# NOAA GHCN-Daily Czech Republic

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note covers the thin `CZ / ghcnd / daily` wrapper around the shared NOAA GHCN-Daily implementation. Unlike the Czech CHMI provider family, this wrapper uses raw GHCN station identifiers and a mapped GHCN country prefix.

## Provider identifiers

- country: `CZ`
- provider: `ghcnd`
- backward-compatible `dataset_scope`: `ghcnd`
- resolution(s): `daily`
- GHCN country prefix used by the wrapper: `EZ`

## Source

- official source: NOAA NCEI GHCN-Daily
- country codes reference: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt`
- station metadata: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- observations: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

## Station identifiers

- `station_id` is the raw NOAA GHCN-Daily station ID
- Czech stations on this wrapper use the GHCN prefix `EZ`, for example `EZM00011406`
- these IDs are different from the CHMI WSI station ids used on `CZ / historical_csv`
- station metadata and station elements are inventory-driven from `ghcnd-inventory.txt`

## Supported data

Current raw-to-canonical mapping:

- `TMAX` -> `tas_max`
- `TMIN` -> `tas_min`
- `PRCP` -> `precipitation`

`open_water_evaporation` is intentionally unsupported on this wrapper. Measured Czech evaporation remains available through `CZ / historical_csv / daily` via raw `VY`, not through GHCN.

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- `TMAX` and `TMIN`: tenths of degrees C -> degrees C
- `PRCP`: tenths of mm -> mm
- NOAA missing code `-9999` is treated as missing

## Limitations and caveats

- the wrapper is intentionally thin and shares parser, metadata, inventory, and observation logic with the common GHCN helper under `weatherdownload/providers/ghcnd/`
- station-level availability is inventory-driven and can differ by station
- only `TMAX`, `TMIN`, and `PRCP` are exposed on `CZ / ghcnd / daily`
- `open_water_evaporation` is not advertised on this wrapper even if unrelated NOAA elements exist elsewhere in GHCN

## Examples

```powershell
weatherdownload stations elements --country CZ --provider ghcnd --station-id EZM00011406 --resolution daily --include-mapping
```

```powershell
weatherdownload observations daily --country CZ --provider ghcnd --station-id EZM00011406 --start-date 2024-01-01 --end-date 2024-01-03 --element tas_max
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [CHMI Czech Republic](cz_chmi.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
