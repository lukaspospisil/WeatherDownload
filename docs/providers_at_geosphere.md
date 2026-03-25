# GeoSphere Austria Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

## Scope

The Austria provider is currently implemented as a conservative GeoSphere Austria station-observation integration.

Implemented scope:

- country: `AT`
- provider: GeoSphere Austria Dataset API
- dataset scope: `historical`
- resolutions: `daily`, `1hour`
- source datasets: `klima-v2-1d`, `klima-v2-1h`

No Austria FAO logic is implemented in the provider itself. The FAO-preparation workflow lives separately in the shared example layer:

- [MATLAB-Oriented FAO Workflow](download_fao.md)

The general shared download examples use the same unified public interface for Austria:

- `python examples/download_daily.py --country AT`
- `python examples/download_hourly.py --country AT`

## Official Source

Dataset and API references used by this provider:

- dataset list:
  - `https://dataset.api.hub.geosphere.at/v1/datasets`
- historical daily metadata:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d/metadata`
- historical daily data:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d`
- historical hourly metadata:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1h/metadata`
- historical hourly data:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1h`

## Implemented Paths

Implemented public paths in this pass:

- `country="AT"`, `dataset_scope="historical"`, `resolution="daily"`
- `country="AT"`, `dataset_scope="historical"`, `resolution="1hour"`

## Supported Resolutions

- `daily`
- `1hour`

No Austria `10min` path is implemented in this pass.

## Station Metadata

Station discovery is derived from the official GeoSphere metadata endpoint.

Normalized fields:

- `station_id`: GeoSphere Klima station id as string
- `gh_id`: null
- `begin_date`: normalized from `valid_from`
- `end_date`: normalized from `valid_to`
- `full_name`: station `name`
- `longitude`: `lon`
- `latitude`: `lat`
- `elevation_m`: `altitude`

The provider does not guess any secondary identifier analogous to `gh_id`.

## Canonical Elements

Current canonical-to-raw mappings for `AT / historical / daily`:

- `tas_mean` -> `tl_mittel`
- `tas_max` -> `tlmax`
- `tas_min` -> `tlmin`
- `precipitation` -> `rr`
- `sunshine_duration` -> `so_h`
- `wind_speed` -> `vv_mittel`
- `pressure` -> `p_mittel`
- `relative_humidity` -> `rf_mittel`

Current canonical-to-raw mappings for `AT / historical / 1hour`:

- `tas_mean` -> `tl`
- `precipitation` -> `rr`
- `wind_speed` -> `ff`
- `relative_humidity` -> `rf`
- `pressure` -> `p`
- `sunshine_duration` -> `so`

The public API prefers the canonical names above. Raw GeoSphere parameter names remain accepted for backward compatibility.

## Daily Semantics

The GeoSphere daily endpoint returns timestamps at `00:00+00:00` for each daily record.

WeatherDownload normalizes this daily path as date-based data:

- output field: `observation_date`
- `time_function`: null

The raw GeoSphere timestamp is used only to derive the calendar date.

## Hourly Semantics

The GeoSphere hourly endpoint publishes a `time` field for each hourly record.

WeatherDownload normalizes this hourly path as timestamp-based data:

- output field: `timestamp`
- the normalized `timestamp` preserves the published GeoSphere hourly `time` value in UTC
- provider-defined hourly field semantics stay behind the provider layer

## QC / Flags

GeoSphere daily metadata documents parameter quality via companion `_flag` parameters and the `q21` code list in the metadata response.

Current daily normalization:

- `quality`: numeric value from `<raw_parameter>_flag`
- `flag`: null

Current hourly normalization:

- `quality`: null
- `flag`: raw value from `<raw_parameter>_flag`

The downloader requests both the raw parameter and its `_flag` companion for each selected element.

## Shared Interface Example

Use the normal shared interface:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="AT",
    dataset_scope="historical",
    resolution="daily",
    station_ids=["1"],
    start_date="2024-01-01",
    end_date="2024-01-03",
    elements=["tas_mean", "precipitation"],
)

daily = download_observations(query)
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="AT",
    dataset_scope="historical",
    resolution="1hour",
    station_ids=["1"],
    start="2024-01-01T00:00:00Z",
    end="2024-01-01T02:00:00Z",
    elements=["tas_mean", "pressure"],
)

hourly = download_observations(query)
```

Source request shapes:

- daily example:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d?station_ids=1&parameters=tl_mittel&parameters=tl_mittel_flag&start=2024-01-01&end=2024-01-03&output_format=csv`
- hourly example:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1h?station_ids=1&parameters=tl&parameters=tl_flag&start=2024-01-01T00:00:00Z&end=2024-01-01T02:00:00Z&output_format=csv`

Example CSV columns from the official API:

- `time`
- `station`
- `<parameter>`
- `<parameter>_flag`
- `substation`

The current provider ignores `substation` in the normalized public output.

## Known Limitations

- only `AT / historical / daily` and `AT / historical / 1hour` are implemented
- no Austria `10min` downloader yet
- no Austria-specific provider-side FAO workflow logic
- no derived meteorological variables are added
- `gh_id` remains null because GeoSphere does not expose a direct equivalent in these implemented paths
