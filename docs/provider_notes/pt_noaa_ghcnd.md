# NOAA GHCN-Daily Portugal

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note covers the thin `PT / ghcnd / daily` wrapper around the shared NOAA GHCN-Daily implementation. The public WeatherDownload country code is `PT`, while the GHCN-Daily station prefix used internally by the wrapper is `PO`.

## Provider identifiers

- country: `PT`
- provider: `ghcnd`
- resolution(s): `daily`
- GHCN country prefix used by the wrapper: `PO`

## Source

- official source: NOAA NCEI GHCN-Daily
- country codes reference: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt`
- station metadata: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- observations: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

## Station identifiers

- `station_id` is the raw NOAA GHCN-Daily station ID
- Portuguese stations on this wrapper use the GHCN prefix `PO`, for example `PO000000001`
- station metadata and station elements are inventory-driven from `ghcnd-inventory.txt`

## Supported data

Current raw-to-canonical mapping:

- `TAVG` -> `tas_mean`
- `TMAX` -> `tas_max`
- `TMIN` -> `tas_min`
- `PRCP` -> `precipitation`
- `SNWD` -> `snow_depth`

`open_water_evaporation` is intentionally unsupported on this wrapper.

This wrapper is observed-only and does not add derived values.

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Limitations and caveats

- the wrapper is intentionally thin and shares parser, metadata, inventory, and observation logic with the common GHCN helper under `weatherdownload/providers/ghcnd/`
- station-level availability is inventory-driven and can differ by station
- `open_water_evaporation` is not advertised on `PT / ghcnd / daily`
- the wrapper is observed-only and does not make FAO-ready or derived-data claims

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [NOAA GHCN-Daily Mapped-Prefix Wrappers](ghcnd_mapped_prefix_wrappers.md)
