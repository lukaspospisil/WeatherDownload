# NOAA GHCN-Daily United Kingdom

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note covers the thin `GB / ghcnd / daily` wrapper around the shared NOAA GHCN-Daily implementation. The shared wrapper behavior is documented in more detail in [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md).

## Provider identifiers

- country: `GB`
- accepted public alias: `UK -> GB`
- provider: `ghcnd`
- resolution(s): `daily`

## Source

- official source: NOAA NCEI GHCN-Daily
- station metadata: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- observations: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

## Station identifiers

- `station_id` is the raw NOAA GHCN-Daily station ID
- only United Kingdom stations are exposed on this wrapper
- station metadata and station elements are inventory-driven

## Supported data

Current raw-to-canonical mapping:

- `TAVG` -> `tas_mean`
- `TMAX` -> `tas_max`
- `TMIN` -> `tas_min`
- `PRCP` -> `precipitation`
- `SNWD` -> `snow_depth`

`open_water_evaporation` is intentionally unsupported on this wrapper. `EVAP` is exposed only when a wrapper explicitly advertises it, which `GB / ghcnd / daily` does not.

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- `TAVG`, `TMAX`, and `TMIN`: tenths of degrees C -> degrees C
- `PRCP`: tenths of mm -> mm
- `SNWD`: mm -> mm
- NOAA missing code `-9999` is treated as missing

## Limitations and caveats

- the wrapper is intentionally thin and shares parser, metadata, and observation logic
- station-level availability is inventory-driven and can differ by station
- `tas_mean` comes only from raw NOAA `TAVG`; it is not derived from `TMAX` and `TMIN`
- `quality` carries NOAA `QFLAG`, while `flag` preserves provider details from `MFLAG` and `SFLAG`
- `GB / ghcnd / daily` is observed-only and not FAO-ready

## Examples

```powershell
weatherdownload stations elements --country GB --provider ghcnd --resolution daily --include-mapping
```

```powershell
weatherdownload observations daily --country UK --provider ghcnd --station-id GB000000001 --start-date 2020-02-01 --end-date 2020-02-02 --element precipitation
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md)
- [Canonical Elements](../canonical_elements.md)
