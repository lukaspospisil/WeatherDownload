# HungaroMet Hungary Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This provider adds Hungary as a standard WeatherDownload country adapter without changing the shared public API.

## Implemented Slice

- country code: `HU`
- dataset scope: `historical`
- resolution: `daily`
- station metadata: yes
- station observation metadata: yes
- daily downloads: yes
- `1hour`: no
- `10min`: no

## Official Source Paths Used

- `https://odp.met.hu/climate/observations_hungary/meta/station_meta_auto.csv`
- `https://odp.met.hu/climate/observations_hungary/daily/historical/`
- `https://odp.met.hu/climate/observations_hungary/daily/recent/`

The current implementation uses the official historical archive listing plus the official current-year `HABP_1D_<station>_akt.zip` path when the requested date range reaches the current year.

## Canonical Mapping

Supported canonical daily elements in this pass:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `sunshine_duration`

Mapped raw HungaroMet fields in this pass:

- `t`
- `tx`
- `tn`
- `rau`
- `fs`
- `u`
- `f`

Unsupported or ambiguous source fields remain unsupported rather than being guessed or derived.

## Station Identifier And Output Semantics

- `station_id` is the official HungaroMet `StationNumber`, normalized as string
- `gh_id` remains null because this source path does not expose an equivalent field in the implemented slice
- daily queries stay date-based through `start_date` and `end_date`
- normalized daily outputs keep the shared WeatherDownload schema
- raw HungaroMet `Q_<field>` values stay in `flag`
- normalized `quality` stays null in this pass

## Current Limits

- only the official Hungary daily station-observation slice is implemented
- no `1hour` support is exposed yet
- no `10min` support is exposed yet
- no synthetic station ids are created
- no derived variables are added to fill coverage gaps
- no daily aggregates are recomputed from subdaily source files

## Next Safe Extension

The next low-risk extension would be to inspect whether official HungaroMet `1hour` station files expose a stable metadata and archive pattern that can be mapped to existing canonical elements without changing shared library semantics.
