# NOAA GHCN-Daily Mapped-Prefix Wrappers

This note covers the shared NOAA GHCN-Daily wrapper pattern used for the current mapped-prefix country adapters. These wrappers stay intentionally thin because the runtime logic lives in the shared helper under `weatherdownload/providers/ghcnd/`.

## Provider identifiers

- country: `AT`, `BA`, `BY`, `CH`, `CZ`, `DE`, `DK`, `ME`, `PT`, `RS`, `SE`, `SK`, `TR`, `UA`
- provider: `ghcnd`
- `provider`: `ghcnd`
- resolution(s): `daily`

## Source

- official source: NOAA NCEI GHCN-Daily
- country codes reference: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt`
- station metadata: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- observations: `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

## Station identifiers

- `station_id` is the raw NOAA GHCN-Daily station ID
- country filtering stays explicit in the wrapper configuration
- station metadata and station elements are inventory-driven from `ghcnd-inventory.txt`

These wrappers are grouped together because the WeatherDownload country code and the GHCN country prefix differ:

- `BA -> BK`
- `BY -> BO`
- `AT -> AU`
- `CH -> SZ`
- `CZ -> EZ`
- `DE -> GM`
- `DK -> DA`
- `ME -> MJ`
- `PT -> PO`
- `RS -> RI`
- `SE -> SW`
- `SK -> LO`
- `TR -> TU`
- `UA -> UP`

The `AT -> AU` mapping is especially worth keeping explicit because GHCN `AU` means Austria here, not WeatherDownload country `AU`. The `BA -> BK` mapping is important because GHCN `BA` means Bahrain, while Bosnia and Herzegovina uses GHCN prefix `BK`. The `BY -> BO` mapping is important because GHCN `BY` means Burundi, while Belarus uses GHCN prefix `BO`. The `ME -> MJ` mapping is important because Montenegro uses GHCN prefix `MJ`, not direct prefix `ME`. The `RS -> RI` mapping is equally important because GHCN `RI` means Serbia, while GHCN `RS` means Russia and must not be used for Serbia. The `SK -> LO` mapping is important because Slovakia uses GHCN prefix `LO`, not direct prefix `SK`. The `TR -> TU` and `UA -> UP` mappings are important because Turkey and Ukraine use GHCN prefixes `TU` and `UP`, not direct prefixes `TR` and `UA`.

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
- the shared helper supports both direct-prefix wrappers (`FI -> FI`) and mapped-prefix wrappers (`DE -> GM`) using the same configuration pattern
- station-level availability is inventory-driven and can differ by station
- `tas_mean` comes only from raw NOAA `TAVG`; this wrapper does not derive a mean from `TMAX` and `TMIN`
- Bosnia and Herzegovina uses WeatherDownload country code `BA` but GHCN station prefix `BK`; GHCN prefix `BA` belongs to Bahrain
- Belarus uses WeatherDownload country code `BY` but GHCN station prefix `BO`; GHCN prefix `BY` belongs to Burundi
- Montenegro uses WeatherDownload country code `ME` but GHCN station prefix `MJ`; do not use direct prefix `ME` for Montenegro
- Serbia uses WeatherDownload country code `RS` but GHCN station prefix `RI`; GHCN prefix `RS` belongs to Russia
- Slovakia uses WeatherDownload country code `SK` but GHCN station prefix `LO`; do not use direct prefix `SK` for Slovakia
- Turkey uses WeatherDownload country code `TR` but GHCN station prefix `TU`; do not use direct prefix `TR` for Turkey
- Ukraine uses WeatherDownload country code `UA` but GHCN station prefix `UP`; do not use direct prefix `UA` for Ukraine
- `SK / ghcnd / daily` is a GHCN-Daily fallback path, not a national Slovakia provider; the separate `SK / recent / daily` SHMU path remains experimental and recent-only
- `snowfall` is intentionally unsupported because there is no existing canonical snowfall element wired for GHCN in this pass
- `open_water_evaporation` is intentionally unsupported on these wrappers
- national providers remain the place for country-specific station IDs and extra national-only elements such as `CZ / historical_csv / daily` raw `VY` or `SK / recent / daily` raw `voda_vypar`

## Examples

```powershell
weatherdownload stations elements --country DE --provider ghcnd --station-id GM000001153 --resolution daily --include-mapping
```

```powershell
weatherdownload observations daily --country SK --provider ghcnd --station-id LO000011934 --start-date 1951-01-01 --end-date 1951-01-03 --element precipitation
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
