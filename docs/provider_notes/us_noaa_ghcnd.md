# NOAA GHCN-Daily United States

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current `US / ghcnd / daily` wrapper around the shared NOAA GHCN-Daily implementation. It stays separate from the other GHCN wrappers because it is the only current GHCN path that exposes measured pan evaporation as `open_water_evaporation`.

## Provider identifiers

- country: `US`
- provider: `ghcnd`
- backward-compatible `dataset_scope`: `ghcnd`
- resolution(s): `daily`

## Source

- official source: NOAA NCEI GHCN-Daily
- station metadata: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- observations: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

The official NOAA documentation describes `.dly` files as station-month records and documents `EVAP` as evaporation from an evaporation pan in tenths of millimeters.

## Station identifiers

- `station_id` is the raw NOAA GHCN-Daily station ID
- country stays implicit through `country="US"` and the GHCN station prefix
- station metadata and station elements are inventory-driven from `ghcnd-inventory.txt`

## Supported data

Current raw-to-canonical mapping:

- `TMAX` -> `tas_max`
- `TMIN` -> `tas_min`
- `PRCP` -> `precipitation`
- `EVAP` -> `open_water_evaporation`

`open_water_evaporation` is supported here only because NOAA explicitly documents `EVAP` as measured evaporation from an evaporation pan. PET, ET0, reference evaporation, modeled evaporation, and multiday `MDEV` totals remain intentionally unsupported.

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

- `TMAX` and `TMIN`: tenths of degrees C -> degrees C
- `PRCP` and `EVAP`: tenths of mm -> mm
- NOAA missing code `-9999` is treated as missing
- normalized output preserves raw NOAA `element_raw`

## Limitations and caveats

- only `US / ghcnd / daily` is implemented
- the shared parser expands fixed-width `.dly` station-month rows into daily observations
- station-level availability is inventory-driven and can differ by station
- `open_water_evaporation` appears only where the official inventory advertises raw `EVAP`
- `quality` carries NOAA `QFLAG`, while `flag` preserves provider details from `MFLAG` and `SFLAG`
- the wrapper stays intentionally thin; shared logic lives under `weatherdownload/providers/ghcnd/`

## Examples

```powershell
weatherdownload stations elements --country US --provider ghcnd --resolution daily --include-mapping
```

```powershell
weatherdownload observations daily --country US --provider ghcnd --station-id USC00010008 --start-date 2024-01-01 --end-date 2024-01-03 --element open_water_evaporation
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
