# NOAA / GHCN-Daily Direct-Prefix Country Wrapper Notes

This note covers the direct-prefix NOAA GHCN-Daily country wrappers implemented through the shared source-local helper under `weatherdownload/providers/ghcnd/`.

Implemented countries in this slice:

- `FI` / Finland
- `FR` / France
- `IT` / Italy
- `NO` / Norway
- `NZ` / New Zealand

Shared wrapper model:

- country-specific selection still uses `country + provider + resolution`
- `provider="ghcnd"` is the preferred public selector
- `dataset_scope="ghcnd"` remains accepted as a backward-compatible alias
- output tables still expose `dataset_scope` for compatibility
- `station_id` is the raw NOAA GHCN-Daily station id
- station metadata and station elements are inventory-driven from `ghcnd-inventory.txt`
- the shared implementation lives in `weatherdownload/providers/ghcnd/`, while country wrappers remain thin and explicit

Supported elements on all countries in this note:

- `TMAX` -> `tas_max`
- `TMIN` -> `tas_min`
- `PRCP` -> `precipitation`

Intentionally unsupported on these wrappers in this pass:

- `EVAP` -> `open_water_evaporation`

Reason:

- the implementation is intentionally conservative
- the direct-prefix audit did not verify meaningful `EVAP` coverage for these countries
- WeatherDownload therefore keeps these wrappers aligned on the common `TMAX/TMIN/PRCP` core

Direct-prefix note:

- these countries are included here because the GHCN country prefix directly matches the WeatherDownload country code:
  - `FI -> FI`
  - `FR -> FR`
  - `IT -> IT`
  - `NO -> NO`
  - `NZ -> NZ`

That makes them suitable for thin wrappers without adding a separate prefix-mapping layer in this pass.

