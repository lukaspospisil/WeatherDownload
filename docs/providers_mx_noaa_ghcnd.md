# NOAA / GHCN-Daily Mexico Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

## Current Scope

This first Mexico slice is intentionally conservative:

- country: `MX`
- dataset scope: `ghcnd`
- resolution: `daily`
- canonical element support:
  - `tas_max` -> `TMAX`
  - `tas_min` -> `TMIN`
  - `precipitation` -> `PRCP`

The implementation uses the official NOAA NCEI GHCN-Daily station files through the shared source-local helper under `weatherdownload/providers/ghcnd/`, with the `MX` provider kept as a thin country wrapper.

## Official Source Contract

The implementation uses these official GHCN-Daily files:

- station metadata:
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- station/element inventory:
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`
- station observation files:
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{GHCN_STATION_ID}.dly`

## Canonical Mapping

Supported mapping on this path:

- `tas_max` -> `TMAX`
- `tas_min` -> `TMIN`
- `precipitation` -> `PRCP`

Not supported on this Mexico slice:

- `open_water_evaporation`

Reason:

- the current GHCN-Daily inventory audit found zero Mexican stations advertising raw `EVAP`
- WeatherDownload therefore does not advertise `open_water_evaporation` for `MX / ghcnd / daily` in this pass

## Units And Missing Values

Raw NOAA units:

- `TMAX`: tenths of degrees C
- `TMIN`: tenths of degrees C
- `PRCP`: tenths of `mm`

WeatherDownload output:

- `tas_max`, `tas_min` in degrees C
- `precipitation` in `mm`

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

Discovery behavior on this path:

- only `MX` stations are exposed
- station metadata include Mexican stations that have at least one currently supported GHCN-Daily element in `ghcnd-inventory.txt`
- station elements are inventory-driven and can differ by station

The GHCN prefix `MX` directly matches the WeatherDownload country code `MX` on this slice.

## Daily Parsing Behavior

Each `.dly` line is expanded into daily rows using the official fixed-width layout.

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

- only `MX / ghcnd / daily` is implemented in this pass
- the reusable NOAA GHCN-Daily parser, metadata, and observation logic live in `weatherdownload/providers/ghcnd/`, while `weatherdownload/providers/mx/` supplies only the country-specific wrapper configuration
- only `TMAX`, `TMIN`, and `PRCP` are advertised for Mexico
- `EVAP` is intentionally excluded for Mexico until official inventory evidence justifies adding it
- this pass does not implement broader GHCN-Daily variables or a country-agnostic global provider abstraction
