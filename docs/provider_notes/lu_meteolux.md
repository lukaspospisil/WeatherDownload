# MeteoLux Luxembourg

This note documents the initial Luxembourg provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `LU`
- provider: `meteolux`
- resolution: `daily`

## Source

- official dataset family: MeteoLux / Luxembourg Open Data Portal INSPIRE Annex III meteorological geographical features
- dataset page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-daily-weather-measurements-at-luxembourg-findel-airport/`
- WFS base: `https://wms.inspire.geoportail.lu/geoserver/mf/wfs`

WeatherDownload uses the structured WFS `GetFeature` JSON path for the daily Findel Airport layers rather than scraping a human-oriented web page.

According to the dataset metadata, the daily historical record spans 1947-present for this station-focused product.

The current implementation also uses the official MeteoLux daily CSV resource for observed daily sunshine duration:

- daily CSV dataset page: `https://data.public.lu/en/datasets/daily-meteorological-parameters-luxembourg-findel-airport-wmo-id-06590/`
- stable resource URL family: `https://data.public.lu/en/datasets/r/a67bd8c0-b036-4761-b161-bdab272302e5`

The current implementation uses the official source type names directly:

- `MF.PointTimeSeriesObservation_Daily_FindelAirport_maxtemperature`
- `MF.PointTimeSeriesObservation_Daily_FindelAirport_mintemperature`
- `MF.PointTimeSeriesObservation_Daily_FindelAirport_totalprecipitation`

The daily sunshine-duration value comes from the MeteoLux CSV column:

- `DINS`

## Station scope

The current implementation intentionally supports one official station:

- `station_id`: `0-20000-0-06590`
- `full_name`: `Luxembourg/Findel Airport`

Coordinates and elevation are exposed from the documented station metadata currently used by the workflow:

- latitude: `49.63265182`
- longitude: `6.232928668`
- elevation_m: `376.1`

## Supported observed elements

Raw-to-canonical mapping:

| Raw | Canonical | Unit |
| --- | --- | --- |
| `maxtemperature` | `tas_max` | `degC` |
| `mintemperature` | `tas_min` | `degC` |
| `totalprecipitation` | `precipitation` | `mm` |
| `DINS` | `sunshine_duration` | `hours` |

Notes:

- this first pass is daily only
- this first pass is observed-only provider data and does not derive extra variables
- `tas_max`, `tas_min`, and `precipitation` come from the official MeteoLux INSPIRE WFS layers
- `sunshine_duration` comes from the official MeteoLux daily CSV resource
- `tas_mean` is intentionally not exposed because it is not part of the current observed daily slice
- `Rn/net radiation is not downloaded`

## Temporal semantics

- WeatherDownload returns normalized daily rows keyed by `observation_date`
- the current WFS JSON response exposes both `day` and `datetime`, with `datetime` currently serialized as `00:00:00Z`
- the current MeteoLux daily CSV uses a `DATE` column and the `DINS` sunshine-duration column in hours
- the MeteoLux precipitation metadata describe the observational precipitation day as `06:00 UTC` to `06:00 UTC` of the following day
- this implementation preserves the source date exactly and does not silently shift precipitation or temperature dates

## Limitations

- this is still not FAO-ready because the current Luxembourg slice does not expose `tas_mean`, `wind_speed`, or `vapour_pressure`
- no hourly or 10-minute Luxembourg implementation is included in this pass
- no derived FAO variables are introduced here

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
