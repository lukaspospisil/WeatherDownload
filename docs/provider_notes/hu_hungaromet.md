# HungaroMet Hungary

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current HungaroMet public-source integration. It includes the standard `historical` slice and the separate wind-only `historical_wind / 10min` slice without changing the shared WeatherDownload provider model.

## Provider identifiers

- country: `HU`
- provider: `historical`, `historical_wind`
- `provider`: `historical`, `historical_wind`
- resolution(s):
  - `historical`: `daily`, `1hour`, `10min`
  - `historical_wind`: `10min`

## Source

- official metadata: `https://odp.met.hu/climate/observations_hungary/meta/station_meta_auto.csv`
- historical daily archive: `https://odp.met.hu/climate/observations_hungary/daily/historical/`
- historical hourly archive: `https://odp.met.hu/climate/observations_hungary/hourly/historical/`
- historical 10-minute archive: `https://odp.met.hu/climate/observations_hungary/10_minutes/historical/`
- wind metadata: `https://odp.met.hu/climate/observations_hungary/10_minutes_wind/station_meta_auto_wind.csv`
- wind archive: `https://odp.met.hu/climate/observations_hungary/10_minutes_wind/historical/`

The implementation also uses the official current-year `*_akt.zip` paths when the requested range reaches the current year.

## Station identifiers

- `station_id` is the official HungaroMet `StationNumber`, normalized as a string
- `gh_id` remains null on the implemented paths
- hourly and 10-minute `Time` values are treated as UTC, matching the published source descriptions

## Supported data

Current source-backed mapping includes:

- `historical / daily`: `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration`
- `historical / 1hour`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`
- `historical / 10min`: `tas_mean`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`
- `historical_wind / 10min`: `wind_speed`, `wind_speed_max`

`historical_wind / 10min` is a separate source product, not a hidden extension of the generic `historical / 10min` slice. For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Units and conversions

This provider uses the official HungaroMet values directly and does not apply a special unit-conversion layer beyond the shared WeatherDownload normalization.

## Limitations and caveats

- `flag` preserves raw HungaroMet `Q_<field>` values
- `quality` remains null in this slice
- the `10_minutes_wind` product is intentionally conservative and currently maps only clearly equivalent wind fields
- WeatherDownload does not silently backfill generic `historical / 10min` wind requests from `historical_wind / 10min`
- `open_water_evaporation` is intentionally unsupported because no clearly documented measured open-water or pan evaporation field was verified on the implemented public paths

## Examples

```powershell
weatherdownload stations elements --country HU --provider historical_wind --resolution 10min --station-id 26327 --include-mapping
```

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country="HU",
    provider="historical_wind",
    resolution="10min",
    station_ids=["26327"],
    start="2025-12-31T23:50:00Z",
    end="2026-01-01T00:10:00Z",
    elements=["wind_speed", "wind_speed_max"],
)

observations = download_observations(query)
```

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
