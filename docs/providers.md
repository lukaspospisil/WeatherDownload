# Provider Model And Coverage

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload uses a provider layer so the public API can stay stable while provider-specific logic remains internal.

## Country Selection

Use ISO 3166-1 alpha-2 country codes:

- `AT`
- `CZ`
- `DE`
- `SK` (experimental, limited to `recent / daily`)

Examples:

```python
from weatherdownload import read_station_metadata, list_supported_elements

at_stations = read_station_metadata(country="AT")
cz_stations = read_station_metadata(country="CZ")
de_stations = read_station_metadata(country="DE")
sk_stations = read_station_metadata(country="SK")

sk_daily_elements = list_supported_elements(
    country="SK",
    dataset_scope="recent",
    resolution="daily",
)
```

```powershell
weatherdownload stations metadata --country AT
weatherdownload stations metadata --country CZ
weatherdownload stations metadata --country DE
weatherdownload stations metadata --country SK
```

## Stable Public Model

The same public API shape is used across countries:

- `read_station_metadata(country=...)`
- `read_station_observation_metadata(country=...)`
- `list_dataset_scopes(country=...)`
- `list_resolutions(country=..., dataset_scope=...)`
- `list_supported_elements(country=..., dataset_scope=..., resolution=...)`
- `download_observations(...)`

What the library normalizes across providers:

- canonical `station_id`
- canonical element names
- normalized observation columns
- DataFrame-first return values

What stays provider-specific internally:

- source URLs
- file layouts
- raw element codes
- quality/flag conventions
- timestamp parsing rules
- metadata completeness

## Canonical Station Identifier

| Country | Normalized `station_id` |
| --- | --- |
| `AT` | GeoSphere Klima station id as string |
| `CZ` | CHMI `WSI` |
| `DE` | zero-padded DWD `Stations_id` |
| `SK` | SHMU `ind_kli` as string |

`gh_id` remains an optional secondary field and is nullable when a provider does not expose an equivalent identifier.

## Implemented Support vs Discovery Support

WeatherDownload distinguishes between:

- discovery support: the provider registry knows that a dataset path exists
- implemented support: the library can actually download and normalize that path

This matters because discovery can be broader than the current downloader coverage.

## Capability Matrix

| Country | Status | Supported dataset scopes | Implemented resolutions | Supported canonical elements | Station metadata quality |
| --- | --- | --- | --- | --- | --- |
| `AT` | Stable | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity` | Official GeoSphere metadata endpoint with station name, coordinates, elevation, and validity range |
| `CZ` | Stable | `now`, `recent`, `historical`, `historical_csv` | `daily`, `1hour`, `10min` under `historical_csv` | Daily: `tas_mean`, `tas_max`, `tas_min`, `wind_speed`, `vapour_pressure`, `sunshine_duration`, `precipitation`, `pressure`, `relative_humidity`<br>Hourly: `vapour_pressure`, `pressure`, `cloud_cover`, `past_weather_1`, `past_weather_2`, `sunshine_duration`<br>10min: `tas_mean`, `tas_max`, `tas_min`, `tas_period_max`, `soil_temperature_10cm`, `soil_temperature_100cm`, `sunshine_duration` | Official CHMI station metadata with canonical `station_id` (`WSI`) and secondary `gh_id` |
| `DE` | Stable | `historical` | `daily`, `1hour`, `10min` | Daily: `tas_mean`, `tas_max`, `tas_min`, `wind_speed`, `wind_speed_max`, `vapour_pressure`, `sunshine_duration`, `precipitation`, `pressure`, `relative_humidity`, `cloud_cover`, `snow_depth`, `ground_temperature_min`, `precipitation_indicator`<br>`1hour`: `tas_mean`, `relative_humidity`, `wind_speed`<br>`10min`: `tas_mean`, `relative_humidity`, `wind_speed` | Official DWD station descriptions normalized to canonical `station_id`; `gh_id` remains null |
| `SK` | Experimental | `recent` | `daily` | `tas_max`, `tas_min`, `sunshine_duration`, `precipitation` | Minimal probe-derived discovery from the current SHMU recent daily payload; station name and coordinates remain null |

Interpretation notes:

- “Supported canonical elements” means the currently implemented downloader slice, not the full upstream provider vocabulary.
- `SK` `begin_date` and `end_date` describe only coverage visible in the sampled recent payload, not authoritative historical station coverage.
- Discovery support can be broader than implemented download support, especially for `CZ`.

## Current Narrow Slices

The following implemented paths are intentionally conservative:

### AT `historical / daily`

Supported canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `sunshine_duration`
- `wind_speed`
- `pressure`
- `relative_humidity`

Detailed notes:

- [GeoSphere Austria Provider Notes](providers_at_geosphere.md)

### DE `historical / 1hour`

Supported canonical elements:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

### DE `historical / 10min`

Supported canonical elements:

- `tas_mean`
- `relative_humidity`
- `wind_speed`

### SK `recent / daily`

Supported canonical elements:

- `tas_max`
- `tas_min`
- `sunshine_duration`
- `precipitation`

Important current limitations:

- `SK` support is experimental
- only `SK / recent / daily` is implemented
- `SK` metadata are probe-derived and do not yet include authoritative station names or coordinates
- `SK` `begin_date` / `end_date` describe only coverage visible in the sampled recent payload, not authoritative historical station coverage

Hard capability boundary:

- only `SK / recent / daily`
- only `tas_max`, `tas_min`, `sunshine_duration`, `precipitation`
- unsupported requests fail early by design

Detailed notes:

- [Experimental Slovakia Provider Notes](providers_sk_experimental.md)

## Query Semantics

Daily paths are date-based:

- use `start_date` and `end_date`
- output uses `observation_date`

Subdaily paths are timestamp-based:

- use `start` and `end`
- output uses `timestamp`

For DWD subdaily paths:

- before `2000-01-01`, source timestamps are localized to `Europe/Berlin` and then converted to UTC
- from `2000-01-01` onward, source timestamps are treated as UTC directly

## CLI Notes

The CLI mirrors the provider model:

- `--country` defaults to `CZ`
- the same command shape works across countries where the provider path is implemented
- unsupported combinations fail with a clear error

Examples:

```powershell
weatherdownload observations daily --country AT --station-id 1 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations hourly --country DE --station-id 00044 --element tas_mean --element wind_speed --start 1999-12-31T22:00:00Z --end 2000-01-01T00:00:00Z
weatherdownload observations daily --country SK --station-id 11800 --element tas_max --start-date 2025-01-01 --end-date 2025-01-02
```

