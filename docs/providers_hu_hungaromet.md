# HungaroMet Hungary Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This provider adds Hungary as a standard WeatherDownload country adapter without changing the shared public API.

## Implemented Slice

- country code: `HU`
- dataset scope: `historical`
- resolutions: `daily`, `1hour`, `10min`
- station metadata: yes
- station observation metadata: yes
- daily downloads: yes
- hourly downloads: yes
- 10-minute downloads: yes, via the generic `10_minutes` path only

## Official Source Paths Used

- `https://odp.met.hu/climate/observations_hungary/meta/station_meta_auto.csv`
- `https://odp.met.hu/climate/observations_hungary/daily/historical/`
- `https://odp.met.hu/climate/observations_hungary/daily/recent/`
- `https://odp.met.hu/climate/observations_hungary/hourly/historical/`
- `https://odp.met.hu/climate/observations_hungary/hourly/recent/`
- `https://odp.met.hu/climate/observations_hungary/10_minutes/historical/`
- `https://odp.met.hu/climate/observations_hungary/10_minutes/recent/`

The current implementation uses the official historical archive listing plus the official current-year `*_akt.zip` path for daily, hourly, and generic 10-minute downloads when the requested range reaches the current year.

## Canonical Mapping

Supported canonical daily elements in this pass:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `sunshine_duration`

Supported canonical hourly elements in this pass:

- `tas_mean` -> `ta`
- `precipitation` -> `r`
- `wind_speed` -> `f`
- `relative_humidity` -> `u`
- `pressure` -> `p`

Supported canonical 10-minute elements in this pass:

- `tas_mean` -> `ta`
- `precipitation` -> `r`
- `wind_speed` -> `fs`
- `relative_humidity` -> `u`
- `pressure` -> `p`

Unsupported or ambiguous source fields remain unsupported rather than being guessed or derived.

## Station Identifier And Output Semantics

- `station_id` is the official HungaroMet `StationNumber`, normalized as string
- `gh_id` remains null because this source path does not expose an equivalent field in the implemented slice
- daily queries stay date-based through `start_date` and `end_date`
- hourly and 10-minute queries stay timestamp-based through `start` and `end`
- normalized daily and subdaily outputs keep the shared WeatherDownload schemas
- raw HungaroMet `Q_<field>` values stay in `flag`
- normalized `quality` stays null in this pass
- hourly and generic 10-minute `Time` are treated as UTC because the official HungaroMet dataset descriptions publish them as `YYYYMMDDHHmm` in UTC

## Current Limits

- only the official `station_meta_auto.csv` station metadata file is used in this pass
- `10_minutes_wind` is not integrated in this pass because it is a separate wind-only product with different station coverage and would need dedicated capability handling
- no synthetic station ids are created
- no derived variables are added to fill coverage gaps
- no hourly data are recomputed from 10-minute files
- no daily aggregates are recomputed from subdaily source files

## Next Safe Extension

The next low-risk extension would be to evaluate whether the separate `10_minutes_wind` product can be exposed cleanly as additional source-backed station coverage without merging it into the generic `10min` path through provider-specific assumptions.
