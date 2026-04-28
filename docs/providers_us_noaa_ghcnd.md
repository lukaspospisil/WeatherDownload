# NOAA / GHCN-Daily Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

## Current Scope

This first NOAA slice is intentionally conservative:

- country: `US`
- dataset scope: `ghcnd`
- resolution: `daily`
- canonical element support:
  - `tas_max` -> `TMAX`
  - `tas_min` -> `TMIN`
  - `precipitation` -> `PRCP`
  - `open_water_evaporation` -> `EVAP`

The implementation uses the official NOAA NCEI GHCN-Daily station files through a shared source-local helper under `weatherdownload/providers/ghcnd/`, with the `US` provider kept as a thin country wrapper.

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

- `tas_max` -> `TMAX`
- `tas_min` -> `TMIN`
- `precipitation` -> `PRCP`
- `open_water_evaporation` -> `EVAP`

Semantic boundary:

- `EVAP` is accepted because NOAA documents it as measured evaporation of water from an evaporation pan
- `TMAX`, `TMIN`, and `PRCP` are accepted because NOAA documents them as standard daily maximum temperature, minimum temperature, and precipitation totals on this source
- `MDEV` is intentionally excluded in this pass because it is a multiday total with different semantics
- PET, ET0, reference evaporation, and modeled evaporation are intentionally unsupported here

## Units And Missing Values

Raw NOAA unit:

- `TMAX`: tenths of degrees C
- `TMIN`: tenths of degrees C
- `PRCP`: tenths of `mm`
- `EVAP`: tenths of `mm`

WeatherDownload output:

- `tas_max`, `tas_min` in degrees C
- `value` in `mm`
- conversion rule for `PRCP` and `EVAP`: `value_mm = raw_value / 10`
- conversion rule for `TMAX` and `TMIN`: `value_c = raw_value / 10`

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

The shared normalized station metadata schema does not add a separate `country` column on this path. Country stays implicit through:

- `country="US"` in the provider selection
- the GHCN-Daily station-id prefix, e.g. `USC...` or `USW...`

This first slice keeps discovery intentionally narrow:

- only `US` stations are exposed
- station metadata include U.S. stations that have at least one currently supported GHCN-Daily element in `ghcnd-inventory.txt`
- station elements are inventory-driven and can differ by station
- `open_water_evaporation` appears only for stations whose official inventory includes raw `EVAP`

That keeps station element discovery truthful while keeping the reusable GHCN-Daily logic in the shared source-local helper rather than duplicating parser or downloader code in the `US` wrapper.

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

- filters to the requested supported GHCN-Daily elements
- expands station-month records into daily observations
- applies requested date filtering
- canonicalizes `element` to the supported WeatherDownload names
- preserves the raw NOAA element in `element_raw`

## Flags

WeatherDownload does not silently discard values just because NOAA flags are present.

Current normalized handling:

- `quality` carries NOAA `QFLAG`
- `flag` stores provider flag details for `MFLAG` and `SFLAG`

## Limitations

- only `US / ghcnd / daily` is implemented in this pass
- the reusable NOAA GHCN-Daily parser, metadata, and observation logic live in `weatherdownload/providers/ghcnd/`, while `weatherdownload/providers/us/` supplies only the country-specific wrapper configuration
- station metadata are still limited to U.S. stations with at least one currently supported GHCN-Daily element in the official inventory
- the first slice still does not expose all GHCN-Daily daily elements
- this pass does not implement broader GHCN-Daily variables or a country-agnostic global provider abstraction
