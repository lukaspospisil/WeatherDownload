# Provider Model And Coverage

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload uses a provider layer so the public API can stay stable while provider-specific logic remains internal.

## Country Selection

Use ISO 3166-1 alpha-2 country codes:

- `AT`
- `BE`
- `CZ`
- `DE`
- `NL`
- `SK` (experimental, limited to `recent / daily`)

Examples:

```python
from weatherdownload import read_station_metadata, list_supported_elements

at_stations = read_station_metadata(country="AT")
be_stations = read_station_metadata(country="BE")
cz_stations = read_station_metadata(country="CZ")
de_stations = read_station_metadata(country="DE")
nl_stations = read_station_metadata(country="NL")
sk_stations = read_station_metadata(country="SK")

nl_daily_elements = list_supported_elements(
    country="NL",
    dataset_scope="historical",
    resolution="daily",
)
```

```powershell
weatherdownload stations metadata --country AT
weatherdownload stations metadata --country BE
weatherdownload stations metadata --country CZ
weatherdownload stations metadata --country DE
weatherdownload stations metadata --country NL
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
- authentication requirements
- provider-side aggregation semantics

## Canonical Station Identifier

| Country | Normalized `station_id` |
| --- | --- |
| `AT` | GeoSphere Klima station id as string |
| `BE` | official RMI/KMI AWS station code from the `aws_station` layer |
| `CZ` | CHMI `WSI` |
| `DE` | zero-padded DWD `Stations_id` |
| `NL` | official KNMI station identifier from the station metadata CSV used by this provider |
| `SK` | SHMU `ind_kli` as string |

`gh_id` remains an optional secondary field and is nullable when a provider does not expose an equivalent identifier.

## Capability Matrix

| Country | Status | Supported dataset scopes | Implemented resolutions | Supported canonical elements | Station metadata quality |
| --- | --- | --- | --- | --- | --- |
| `AT` | Stable | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity` | Official GeoSphere metadata endpoint with station name, coordinates, elevation, and validity range |
| `BE` | Stable | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | Official RMI/KMI `aws_station` metadata layer with station code, name, coordinates, altitude, and validity range |
| `CZ` | Stable | `now`, `recent`, `historical`, `historical_csv` | `daily`, `1hour`, `10min` under `historical_csv` | Daily: `tas_mean`, `tas_max`, `tas_min`, `wind_speed`, `vapour_pressure`, `sunshine_duration`, `precipitation`, `pressure`, `relative_humidity` |
| `DE` | Stable | `historical` | `daily`, `1hour`, `10min` | Daily: `tas_mean`, `tas_max`, `tas_min`, `wind_speed`, `wind_speed_max`, `vapour_pressure`, `sunshine_duration`, `precipitation`, `pressure`, `relative_humidity`, `cloud_cover`, `snow_depth`, `ground_temperature_min`, `precipitation_indicator` |
| `NL` | Stable | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity` | Official KNMI metadata file retrieved through the Open Data API; API key required |
| `SK` | Experimental | `recent` | `daily` | `tas_max`, `tas_min`, `sunshine_duration`, `precipitation` | Minimal probe-derived discovery from the current SHMU recent daily payload |

## Current Narrow Slices

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

### BE `historical / daily`

Supported canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `pressure`
- `sunshine_duration`

Important current limitations:

- the implemented path uses the official RMI/KMI open-data platform only
- only `BE / historical / daily` is implemented
- daily values are the official provider-side `aws_1day` aggregates from 10-minute data
- WeatherDownload does not recompute daily aggregates from 10-minute data in this pass
- `flag` carries the raw `qc_flags` JSON string when present
- `quality` remains null because this pass does not speculate on provider QC semantics
- no FAO computation and no derived meteorological variables are added

Detailed notes:

- [RMI/KMI Belgium Provider Notes](providers_be_rmi.md)

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

### NL `historical / daily`

Supported canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `sunshine_duration`
- `wind_speed`
- `pressure`
- `relative_humidity`

Important current limitations:

- KNMI access requires an API key
- only `NL / historical / daily` is implemented
- the implemented path uses the official KNMI Open Data API only
- hourly and EDR are intentionally out of scope for this pass
- no FAO computation and no FAO-related meteorological derivations are added
- `quality` and `flag` remain null because this pass does not speculate on KNMI quality semantics beyond the validated dataset boundary

Detailed notes:

- [KNMI Netherlands Provider Notes](providers_nl_knmi.md)

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

## Query Semantics

Daily paths are date-based:

- use `start_date` and `end_date`
- output uses `observation_date`

Subdaily paths are timestamp-based:

- use `start` and `end`
- output uses `timestamp`

For RMI/KMI Belgium daily files:

- station discovery uses the `aws_station` layer
- daily observations use the `aws_1day` layer
- observation_date follows the source daily timestamp date
- provider-defined day grouping stays behind the provider layer

For KNMI daily files:

- files are handled through the Open Data API
- the NetCDF timestamp marks the end of the daily interval in UTC
- WeatherDownload normalizes that end timestamp back to the represented observation date

## CLI Notes

The CLI mirrors the provider model:

- `--country` defaults to `CZ`
- the same command shape works across countries where the provider path is implemented
- unsupported combinations fail with a clear error

Examples:

```powershell
weatherdownload observations daily --country AT --station-id 1 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country BE --station-id 6414 --element tas_mean --element precipitation --element sunshine_duration --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country NL --station-id 0-20000-0-06260 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country SK --station-id 11800 --element tas_max --start-date 2025-01-01 --end-date 2025-01-02
```
