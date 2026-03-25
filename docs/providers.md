# Provider Model And Coverage

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

WeatherDownload uses a provider layer so the public API can stay stable while provider-specific logic remains internal.

## Where To Start

- Station metadata and identifiers: [Canonical Station Identifier](#canonical-station-identifier)
- Daily, `1hour`, and `10min` country coverage: [Capability Matrix](#capability-matrix)
- Shared examples for each workflow: [Examples And Workflows](examples.md)
- FAO-oriented daily input packaging example: [FAO-Oriented Daily Input Packaging Workflow](download_fao.md)
- Shared station and observation columns: [Normalized Output Schemas](output_schema.md)

## Country Selection

Use ISO 3166-1 alpha-2 country codes:

- `AT`
- `BE`
- `CZ`
- `DE`
- `DK`
- `NL`
- `SE`
- `SK` (experimental, limited to `recent / daily`)

Examples:

```python
from weatherdownload import read_station_metadata, list_supported_elements

at_stations = read_station_metadata(country="AT")
be_stations = read_station_metadata(country="BE")
cz_stations = read_station_metadata(country="CZ")
de_stations = read_station_metadata(country="DE")
dk_stations = read_station_metadata(country="DK")
nl_stations = read_station_metadata(country="NL")
se_stations = read_station_metadata(country="SE")
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
weatherdownload stations metadata --country DK
weatherdownload stations metadata --country NL
weatherdownload stations metadata --country SE
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

Subdaily variability is expected across providers:

- some countries implement only `daily`
- some implement `daily` plus `1hour` but not `10min`
- some expose fewer canonical subdaily fields than others
- some expose raw QC/status fields in subdaily paths while others do not
- the shared normalized contract stays the same; unsupported subdaily fields remain unavailable for that slice, while row-level missing values remain null

## Canonical Station Identifier

| Country | Normalized `station_id` |
| --- | --- |
| `AT` | GeoSphere Klima station id as string |
| `BE` | official RMI/KMI AWS station code from the `aws_station` layer |
| `CZ` | CHMI `WSI` |
| `DE` | zero-padded DWD `Stations_id` |
| `DK` | official DMI `stationId` from the Climate Data `station` collection |
| `NL` | official KNMI station identifier from the station metadata CSV used by this provider |
| `SE` | official SMHI station id from the parameter station listings used by this provider |
| `SK` | SHMU `ind_kli` as string |

`gh_id` remains an optional secondary field and is nullable when a provider does not expose an equivalent identifier.

## Capability Matrix

| Country | Status | Supported dataset scopes | Implemented resolutions | Supported canonical elements | Station metadata quality |
| --- | --- | --- | --- | --- | --- |
| `AT` | Stable | `historical` | `daily`, `1hour` | Daily: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity`; 1hour: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | Official GeoSphere metadata endpoint with station name, coordinates, elevation, and validity range |
| `BE` | Stable | `historical` | `daily`, `1hour`, `10min` | Daily: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`; 1hour: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`; 10min: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | Official RMI/KMI `aws_station` metadata layer with station code, name, geometry-backed coordinates, altitude, and validity timestamps |
| `CZ` | Stable | `now`, `recent`, `historical`, `historical_csv` | `daily`, `1hour`, `10min` under `historical_csv` | Daily: `tas_mean`, `tas_max`, `tas_min`, `wind_speed`, `vapour_pressure`, `sunshine_duration`, `precipitation`, `pressure`, `relative_humidity` | CHMI station metadata with official identifiers, names, coordinates, elevation, and validity fields where exposed by the implemented paths |
| `DE` | Stable | `historical` | `daily`, `1hour`, `10min` | Daily: `tas_mean`, `tas_max`, `tas_min`, `wind_speed`, `wind_speed_max`, `vapour_pressure`, `sunshine_duration`, `precipitation`, `pressure`, `relative_humidity`, `cloud_cover`, `snow_depth`, `ground_temperature_min`, `precipitation_indicator` | Official DWD station metadata with names, coordinates, elevation, state, and validity range |
| `DK` | Stable | `historical` | `daily`, `1hour`, `10min` | Daily: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`; 1hour: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`; 10min: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration` | Official DMI Climate Data `station` collection filtered to Denmark stations, with source-backed name, coordinates, station height, and validity range |
| `NL` | Stable | `historical` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `sunshine_duration`, `wind_speed`, `pressure`, `relative_humidity` | Official KNMI metadata file retrieved through the Open Data API; API key required |
| `SE` | Stable | `historical` | `daily`, `1hour` | Daily: `tas_mean`, `tas_max`, `tas_min`, `precipitation`; 1hour: `tas_mean`, `wind_speed`, `relative_humidity`, `precipitation`, `pressure` | Official SMHI parameter station listings merged across the supported daily and hourly parameters, with source-backed name, coordinates, elevation, and validity range |
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

### AT `historical / 1hour`

Supported canonical elements:

- `tas_mean`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `pressure`
- `sunshine_duration`

Important current limitations:

- the implemented path uses the official GeoSphere Austria station datasets only
- only `AT / historical / daily` and `AT / historical / 1hour` are implemented
- hourly observations use the official `klima-v2-1h` station path
- the normalized `timestamp` preserves the published GeoSphere hourly `time` value in UTC
- raw GeoSphere hourly `<parameter>_flag` values stay in `flag`; normalized `quality` stays null
- no FAO computation and no derived meteorological variables are added

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

### BE `historical / 1hour`

Supported canonical elements:

- `tas_mean`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `pressure`
- `sunshine_duration`

### BE `historical / 10min`

Supported canonical elements:

- `tas_mean`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `pressure`
- `sunshine_duration`

Important current limitations:

- the implemented path uses the official RMI/KMI open-data platform only
- only `BE / historical / daily`, `BE / historical / 1hour`, and `BE / historical / 10min` are implemented
- daily values are the official provider-side `aws_1day` aggregates from 10-minute data
- the documented daily grouping window is from `00:10` on day `D` to `00:00` on day `D+1`
- hourly values are the official provider-side `aws_1hour` aggregates from 10-minute data
- the documented hourly grouping window is from `(H-1):10` to `H:00` for hour `H`
- the source documents hourly `pressure` as the provider-side average of the 10-minute `pressure` field
- 10-minute values come directly from `aws_10min` and WeatherDownload preserves the published timestamps
- the source documents most mapped 10-minute fields over the last 10 minutes, while `pressure` is a last-minute average on the same path
- WeatherDownload does not recompute daily or hourly aggregates from Belgium 10-minute data in this pass
- `flag` carries the raw `qc_flags` source text when present
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

### DK `historical / daily`

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

- the implemented path uses the official DMI Climate Data `station` and `stationValue` collections only
- only `DK / historical / daily`, `DK / historical / 1hour`, and `DK / historical / 10min` are implemented
- the current slice is Denmark only; Greenland and Faroe Islands differences are intentionally out of scope for this pass
- mapped daily parameters are source-backed local-day Denmark values; WeatherDownload does not recompute or derive meteorological variables
- `flag` carries raw source-backed `qcStatus` and `validity` as JSON text; normalized `quality` remains null

Detailed notes:

- [DMI Denmark Provider Notes](providers_dk_dmi.md)

### DK `historical / 1hour`

Supported canonical elements:

- `tas_mean`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `pressure`
- `sunshine_duration`

Important current limitations:

- the implemented path uses the official DMI Climate Data `station` and `stationValue` collections only
- only `DK / historical / daily`, `DK / historical / 1hour`, and `DK / historical / 10min` are implemented
- the current slice is Denmark only; Greenland and Faroe Islands differences are intentionally out of scope for this pass
- hourly observations come from the official DMI Climate Data `stationValue` collection with `timeResolution=hour`
- WeatherDownload preserves the provider-defined hourly interval semantics and does not recompute hourly values from any other Denmark source path
- `flag` carries raw source-backed `qcStatus` and `validity` as JSON text; normalized `quality` remains null

Detailed notes:

- [DMI Denmark Provider Notes](providers_dk_dmi.md)

### DK `historical / 10min`

Supported canonical elements:

- `tas_mean`
- `precipitation`
- `wind_speed`
- `relative_humidity`
- `pressure`
- `sunshine_duration`

Important current limitations:

- station discovery still uses the official DMI Climate Data `station` collection filtered to Denmark stations
- 10-minute observations come from the official DMI Meteorological Observation API `observation` collection
- the source `observed` timestamp is preserved as the normalized `timestamp` in UTC
- mapped fields keep the provider-defined 10-minute meanings published by DMI; WeatherDownload does not recompute hourly or daily values from the 10-minute path
- source QC/status fields are not exposed on the implemented `10min` path, so `flag` remains null and normalized `quality` remains null

Detailed notes:

- [DMI Denmark Provider Notes](providers_dk_dmi.md)

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

### SE `historical / daily`

Supported canonical elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`

Important current limitations:

- the implemented path uses the official SMHI Meteorological Observations API only
- only `SE / historical / daily` and `SE / historical / 1hour` are implemented
- station discovery merges the supported daily and hourly parameter station listings used by this provider
- daily observations use the official `parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv` path
- `observation_date` is normalized from the published `Representativt dygn` column, while provider-defined interval windows stay behind the provider layer
- corrected-archive excludes the latest three months by source design
- raw `Kvalitet` stays in `flag`; normalized `quality` remains null
- no FAO computation and no derived meteorological variables are added

### SE `historical / 1hour`

Supported canonical elements:

- `tas_mean`
- `wind_speed`
- `relative_humidity`
- `precipitation`
- `pressure`

Important current limitations:

- the implemented path uses the official SMHI Meteorological Observations API only
- only `SE / historical / daily` and `SE / historical / 1hour` are implemented
- station discovery merges the supported daily and hourly parameter station listings used by this provider
- hourly observations use the official `parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv` path
- `timestamp` is taken directly from the published `Datum` + `Tid (UTC)` columns and preserved in UTC
- provider-defined hourly field semantics stay behind the provider layer
- corrected-archive excludes the latest three months by source design
- raw `Kvalitet` stays in `flag`; normalized `quality` remains null
- no FAO computation and no derived meteorological variables are added

Detailed notes:

- [SMHI Sweden Provider Notes](providers_se_smhi.md)

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

For GeoSphere Austria hourly files:

- station discovery uses the official GeoSphere metadata endpoint shared by the Austria provider
- hourly observations use the official `station/historical/klima-v2-1h` path
- `timestamp` in the normalized hourly output preserves the published GeoSphere `time` value in UTC
- provider-defined hourly field semantics stay behind the provider layer
- raw GeoSphere hourly `<parameter>_flag` values stay in `flag`; normalized `quality` stays null

For RMI/KMI Belgium files:

- station discovery uses the `aws_station` layer
- daily observations use the `aws_1day` layer
- hourly observations use the `aws_1hour` layer
- 10-minute observations use the `aws_10min` layer
- `observation_date` follows the source daily timestamp date for `aws_1day`
- the source-defined daily grouping window is from `00:10` on day `D` to `00:00` on day `D+1`
- `timestamp` in the normalized hourly output preserves the published `aws_1hour` timestamp
- the source-defined hourly grouping window is from `(H-1):10` to `H:00` for hour `H`
- `timestamp` in the normalized 10-minute output preserves the published `aws_10min` timestamp
- the source documents most mapped 10-minute fields over the last 10 minutes, while 10-minute `pressure` is a last-minute average and hourly `pressure` is the provider-side average of that field
- provider-defined grouping and timestamp semantics stay behind the provider layer
- raw `qc_flags` stay in `flag`; normalized `quality` stays null

For DMI Denmark daily files:

- station discovery uses the Climate Data `station` collection filtered to `country = DNK`
- daily observations use the Climate Data `stationValue` collection with `timeResolution=day`
- WeatherDownload normalizes `observation_date` from the source interval start in `Europe/Copenhagen` because the mapped Denmark daily parameters are documented as local-day values
- raw source `qcStatus` and `validity` are preserved in `flag`; normalized `quality` stays null

For DMI Denmark hourly files:

- station discovery uses the Climate Data `station` collection filtered to `country = DNK`
- hourly observations use the Climate Data `stationValue` collection with `timeResolution=hour`
- the official DMI hourly source path uses UTC timestamps and exposes `from` and `to` interval bounds
- WeatherDownload normalizes subdaily `timestamp` from the source `to` timestamp so the public interface keeps the provider-defined hourly interval meaning
- raw source `qcStatus` and `validity` are preserved in `flag`; normalized `quality` stays null

For DMI Denmark 10-minute files:

- station discovery still uses the Climate Data `station` collection filtered to `country = DNK`
- 10-minute observations use the Meteorological Observation API `observation` collection
- the official metObs source path exposes the published `observed` timestamp for each raw observation
- WeatherDownload preserves that `observed` timestamp as the normalized `timestamp` in UTC and does not reinterpret it into a different meteorological meaning
- source QC/status fields are not exposed on the implemented `10min` path, so `flag` and normalized `quality` remain null

For SMHI Sweden daily files:

- station discovery merges the official station lists from the supported daily and hourly parameter endpoints
- daily observations use the official `parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv` path
- `observation_date` is taken from the published `Representativt dygn` column
- provider-defined interval windows differ by parameter and stay behind the provider layer
- corrected-archive excludes the latest three months while SMHI quality control is still in progress
- raw `Kvalitet` stays in `flag`; normalized `quality` stays null

For SMHI Sweden hourly files:

- station discovery merges the official station lists from the supported daily and hourly parameter endpoints
- hourly observations use the official `parameter/{parameter_id}/station/{station_id}/period/corrected-archive/data.csv` path
- `timestamp` is taken directly from the published `Datum` + `Tid (UTC)` columns and preserved in UTC
- provider-defined hourly field semantics stay behind the provider layer
- corrected-archive excludes the latest three months while SMHI quality control is still in progress
- raw `Kvalitet` stays in `flag`; normalized `quality` stays null

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
weatherdownload observations hourly --country AT --station-id 1 --element tas_mean --element pressure --start 2024-01-01T00:00:00Z --end 2024-01-01T02:00:00Z
weatherdownload observations hourly --country BE --station-id 6414 --element tas_mean --element pressure --start 2024-01-01T01:00:00Z --end 2024-01-01T02:00:00Z
weatherdownload observations 10min --country BE --station-id 6414 --element tas_mean --element pressure --start 2024-01-01T00:10:00Z --end 2024-01-01T00:20:00Z
weatherdownload observations daily --country CZ --station-id 0-20000-0-11406 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DE --station-id 00044 --element tas_mean --start-date 2024-01-01 --end-date 2024-01-10
weatherdownload observations daily --country DK --station-id 06180 --element tas_mean --element precipitation --element sunshine_duration --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations hourly --country DK --station-id 06180 --element tas_mean --element pressure --start 2024-01-01T01:00:00Z --end 2024-01-01T02:00:00Z
weatherdownload observations 10min --country DK --station-id 06180 --element tas_mean --element pressure --start 2024-01-01T00:10:00Z --end 2024-01-01T00:20:00Z
weatherdownload observations daily --country NL --station-id 0-20000-0-06260 --element tas_mean --element precipitation --start-date 2024-01-01 --end-date 2024-01-03
weatherdownload observations daily --country SE --station-id 98230 --element tas_mean --element tas_max --element precipitation --start-date 1996-10-01 --end-date 1996-10-03
weatherdownload observations hourly --country SE --station-id 98230 --element tas_mean --element pressure --start 2012-11-29T11:00:00Z --end 2012-11-29T13:00:00Z
weatherdownload observations daily --country SK --station-id 11800 --element tas_max --start-date 2025-01-01 --end-date 2025-01-02
```



