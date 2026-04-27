# HungaroMet Hungary Provider Notes

<p align="right">
  <img src="images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This provider adds Hungary as a standard WeatherDownload country adapter without changing the shared public API.

## Implemented Slice

- country code: `HU`
- dataset scopes: `historical`, `historical_wind`
- resolutions:
  - `historical`: `daily`, `1hour`, `10min`
  - `historical_wind`: `10min`
- station metadata: yes
- station observation metadata: yes
- daily downloads: yes
- hourly downloads: yes
- generic 10-minute downloads: yes, via the official `10_minutes` path
- separate wind-only 10-minute downloads: yes, via the official `10_minutes_wind` path and the explicit `historical_wind / 10min` capability

## Official Source Paths Used

Generic HungaroMet station observations:

- `https://odp.met.hu/climate/observations_hungary/meta/station_meta_auto.csv`
- `https://odp.met.hu/climate/observations_hungary/daily/historical/`
- `https://odp.met.hu/climate/observations_hungary/daily/recent/`
- `https://odp.met.hu/climate/observations_hungary/hourly/historical/`
- `https://odp.met.hu/climate/observations_hungary/hourly/recent/`
- `https://odp.met.hu/climate/observations_hungary/10_minutes/historical/`
- `https://odp.met.hu/climate/observations_hungary/10_minutes/recent/`

Separate Balaton wind product:

- `https://odp.met.hu/climate/observations_hungary/10_minutes_wind/station_meta_auto_wind.csv`
- `https://odp.met.hu/climate/observations_hungary/10_minutes_wind/historical/`
- `https://odp.met.hu/climate/observations_hungary/10_minutes_wind/recent/`

The current implementation uses the official historical archive listings plus the official current-year `*_akt.zip` paths when the requested range reaches the current year.

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

Supported canonical generic 10-minute elements in this pass:

- `tas_mean` -> `ta`
- `precipitation` -> `r`
- `wind_speed` -> `fs`
- `relative_humidity` -> `u`
- `pressure` -> `p`

Supported canonical `historical_wind / 10min` elements in this pass:

- `wind_speed` -> `fs`
- `wind_speed_max` -> `fx`

Unsupported or ambiguous source fields remain unsupported rather than being guessed or derived.

`open_water_evaporation` is intentionally unsupported on the implemented HungaroMet public source. The official `odp.met.hu/climate/observations_hungary` files used by this provider expose the current HABP daily, hourly, and 10-minute station products, but no clearly documented measured open-water, pan, or evaporimeter evaporation variable was verified on those implemented paths. Literature references to separate Class A pan series are not treated as sufficient unless that variable is exposed on the current public source used by WeatherDownload.

## Station Identifier And Output Semantics

- `station_id` is the official HungaroMet `StationNumber`, normalized as string
- `gh_id` remains null because these source paths do not expose an equivalent field in the implemented slice
- daily queries stay date-based through `start_date` and `end_date`
- hourly and 10-minute queries stay timestamp-based through `start` and `end`
- normalized daily and subdaily outputs keep the shared WeatherDownload schemas
- raw HungaroMet `Q_<field>` values stay in `flag`
- normalized `quality` stays null in this pass
- hourly, generic 10-minute, and `historical_wind / 10min` `Time` values are treated as UTC because the official HungaroMet dataset descriptions publish them as `YYYYMMDDHHmm` in UTC

## `historical_wind / 10min` Scope Boundary

The official `10_minutes_wind` product is exposed as a separate HU-specific dataset scope because it is not the same source product as the generic `10_minutes` tree.

What stays explicit:

- `historical / 10min` continues to mean the generic HungaroMet `10_minutes` product
- `historical_wind / 10min` means the separate HungaroMet `10_minutes_wind` product
- WeatherDownload does not mix rows from those two products inside one query
- WeatherDownload does not silently backfill generic `historical / 10min` wind requests from `10_minutes_wind`
- station coverage differs across those products and discovery reflects that difference through the dataset scope

## Wind-Only 10-Minute Example

Use the explicit `historical_wind / 10min` capability when you want the separate HungaroMet wind-only product.

Python download example:

```python
from weatherdownload import ObservationQuery, download_observations

query = ObservationQuery(
    country='HU',
    dataset_scope='historical_wind',
    resolution='10min',
    station_ids=['26327'],
    start='2025-12-31T23:50:00Z',
    end='2026-01-01T00:10:00Z',
    elements=['wind_speed', 'wind_speed_max'],
)

observations = download_observations(query)
print(observations.head().to_string(index=False))
```

CLI discovery example:

```powershell
weatherdownload stations elements --country HU --station-id 26327 --dataset-scope historical_wind --resolution 10min --include-mapping
```

Important notes:

- this is the separate HungaroMet `10_minutes_wind` product, not the generic `historical / 10min` product
- only `wind_speed` and `wind_speed_max` are currently mapped on this scope
- the simplified `weatherdownload observations 10min` CLI keeps using each country's default dataset scope, so the explicit wind-only download path is currently clearest through the Python API

## Current Limits

- only the official `station_meta_auto.csv` and `station_meta_auto_wind.csv` station metadata files are used in this pass
- the `10_minutes_wind` implementation is intentionally conservative and maps only clearly equivalent wind fields
- ambiguous direction fields from the wind-only product remain unsupported in this pass
- no synthetic station ids are created
- no derived variables are added to fill coverage gaps
- no hourly data are recomputed from 10-minute files
- no daily aggregates are recomputed from subdaily source files
- the simplified shared hourly and 10-minute example scripts keep using the default `historical` scope and do not switch to `historical_wind`

## Next Safe Extension

The next low-risk extension would be to inspect the published HungaroMet wind-only documentation PDFs in detail and determine whether any additional wind-direction field from `10_minutes_wind` can be mapped cleanly to an existing canonical element without broadening the public model.
