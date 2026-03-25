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
- resolution: `daily`
- source dataset: `klima-v2-1d`

No Austria FAO logic is implemented in the provider itself. The daily FAO-preparation workflow now lives separately in the example layer:

- [MATLAB-Oriented FAO Workflow](download_fao.md)

The general daily download example also uses the same unified public interface for Austria:

- `python examples/download_daily.py --country AT`

## Official Source

Dataset page:

- `https://data.hub.geosphere.at/dataset/klima-v2-1d`

Official API endpoints used:

- dataset list:
  - `https://dataset.api.hub.geosphere.at/v1/datasets`
- historical daily metadata:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d/metadata`
- historical daily data:
  - `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d`

## Implemented Paths

Implemented public path in this pass:

- `country="AT"`, `dataset_scope="historical"`, `resolution="daily"`

## Station Metadata

Station discovery is derived from the official metadata endpoint.

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

## Supported Canonical Elements

Current canonical-to-raw mappings for `AT / historical / daily`:

- `tas_mean` -> `tl_mittel`
- `tas_max` -> `tlmax`
- `tas_min` -> `tlmin`
- `precipitation` -> `rr`
- `sunshine_duration` -> `so_h`
- `wind_speed` -> `vv_mittel`
- `pressure` -> `p_mittel`
- `relative_humidity` -> `rf_mittel`

The public API prefers the canonical names above. Raw GeoSphere parameter names remain accepted for backward compatibility.

## Daily Semantics

The GeoSphere daily endpoint returns timestamps at `00:00+00:00` for each daily record.

WeatherDownload normalizes this daily path as date-based data:

- output field: `observation_date`
- `time_function`: null

The raw GeoSphere timestamp is used only to derive the calendar date.

## Quality And Flags

GeoSphere documents daily parameter quality via companion `_flag` parameters and the `q21` code list in the metadata response.

Current normalization:

- `quality`: numeric value from `<raw_parameter>_flag`
- `flag`: null

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

observations = download_observations(query)
```

Source request shape:

Example request:

- `https://dataset.api.hub.geosphere.at/v1/station/historical/klima-v2-1d?station_ids=1&parameters=tl_mittel&parameters=tl_mittel_flag&start=2024-01-01&end=2024-01-03&output_format=csv`

Example CSV columns from the official API:

- `time`
- `station`
- `<parameter>`
- `<parameter>_flag`
- `substation`

The current provider ignores `substation` in the normalized public output.

## Known Limitations

- only `AT / historical / daily` is implemented
- no Austria hourly downloader yet
- no Austria 10-minute downloader yet
- no Austria-specific provider-side FAO workflow logic
- `gh_id` remains null because GeoSphere does not expose a direct equivalent in this dataset

