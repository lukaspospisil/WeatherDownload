# NOAA GHCN-Daily Canada

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note covers the thin `CA / ghcnd / daily` wrapper around the shared NOAA GHCN-Daily implementation. The shared GHCN wrapper behavior is documented in more detail in [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md).

## Provider identifiers

- country: `CA`
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
- only Canadian stations are exposed on this wrapper
- station metadata and station elements are inventory-driven

## Supported data

Current raw-to-canonical mapping:

- `TMAX` -> `tas_max`
- `TMIN` -> `tas_min`
- `PRCP` -> `precipitation`

`open_water_evaporation` is intentionally unsupported on this wrapper. The current inventory audit did not justify advertising raw `EVAP` for Canada.

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- `TMAX` and `TMIN`: tenths of degrees C -> degrees C
- `PRCP`: tenths of mm -> mm
- NOAA missing code `-9999` is treated as missing

## Limitations and caveats

- the wrapper is intentionally thin and shares parser, metadata, and observation logic
- station-level availability is inventory-driven and can differ by station
- `quality` carries NOAA `QFLAG`, while `flag` preserves provider details from `MFLAG` and `SFLAG`
- `open_water_evaporation` is not advertised for `CA / ghcnd / daily`

## Examples

```powershell
weatherdownload stations elements --country CA --provider ghcnd --resolution daily --include-mapping
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md)
- [Canonical Elements](../canonical_elements.md)
