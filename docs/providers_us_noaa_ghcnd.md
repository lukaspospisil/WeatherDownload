# NOAA / GHCN-Daily Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

## Current Scope

This first NOAA slice is intentionally conservative:

- country: `US`
- dataset scope: `ghcnd`
- resolution: `daily`
- canonical element support: `open_water_evaporation` only

The implementation uses the official NOAA NCEI GHCN-Daily station files and does not attempt a global provider refactor in this pass.

## Official Source Contract

The implementation uses these official GHCN-Daily files:

- station metadata:
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory:
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- station observation files:
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

The official NOAA readme documents:

- GHCN-Daily as an integrated database of daily climate summaries from land surface stations
- `.dly` records as one station-month per line
- `EVAP` as `Evaporation of water from evaporation pan (tenths of mm)`

## Canonical Mapping

Supported mapping on this path:

- `open_water_evaporation` -> `EVAP`

Semantic boundary:

- `EVAP` is accepted because NOAA documents it as measured evaporation of water from an evaporation pan
- `MDEV` is intentionally excluded in this pass because it is a multiday total with different semantics
- PET, ET0, reference evaporation, and modeled evaporation are intentionally unsupported here

## Units And Missing Values

Raw NOAA unit:

- `EVAP`: tenths of `mm`

WeatherDownload output:

- `value` in `mm`
- conversion rule: `value_mm = raw_value / 10`

Missing raw values:

- NOAA missing code `-9999`
- WeatherDownload treats these as missing and does not emit a numeric value for those day rows

## Station Metadata Behavior

The normalized station table keeps:

- `station_id` as the raw GHCN-Daily station ID
- `full_name`
- `latitude`
- `longitude`
- `elevation_m`
- `country`

This first slice keeps discovery intentionally narrow:

- only `US` stations are exposed
- only stations with `EVAP` availability in `ghcnd-inventory.txt` are surfaced by this provider slice

That keeps station element discovery truthful without introducing broader per-station element machinery across the repository.

## Daily Parsing Behavior

Each `.dly` line is expanded into daily rows using the official fixed-width layout:

- `ID`
- `YEAR`
- `MONTH`
- `ELEMENT`
- `VALUE1..VALUE31`
- `MFLAG1..MFLAG31`
- `QFLAG1..QFLAG31`
- `SFLAG1..SFLAG31`

WeatherDownload behavior on this path:

- filters to `EVAP`
- expands station-month records into daily observations
- applies requested date filtering
- canonicalizes `element` to `open_water_evaporation`
- preserves the raw NOAA element as `element_raw = EVAP`

## Flags

WeatherDownload does not silently discard values just because NOAA flags are present.

Current normalized handling:

- `quality` carries NOAA `QFLAG`
- `flag` stores provider flag details for `MFLAG` and `SFLAG`

## Limitations

- only `US / ghcnd / daily` is implemented in this pass
- only `open_water_evaporation` is mapped
- station discovery is intentionally filtered to U.S. stations with `EVAP` in the official inventory
- this pass does not implement broader GHCN-Daily variables or a country-agnostic global provider abstraction
