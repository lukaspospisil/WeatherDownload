# ASTA Luxembourg

This note documents the initial ASTA provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `LU`
- provider: `asta`
- resolution: `daily`

This provider is separate from the existing `LU / meteolux / daily` Findel-only slice.

## Source

- official dataset page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-pointtimeseriesobservation-daily-weather-measurements-of-asta-1/`
- official station metadata page: `https://data.public.lu/en/datasets/inspire-annex-iii-meteorological-geographical-features-spatial-sampling-features-location-of-weather-stations-managed-by-asta/`
- WFS base: `https://wms.inspire.geoportail.lu/geoserver/mf/wfs`

WeatherDownload uses the official INSPIRE WFS `GetFeature` JSON path for ASTA daily observations and station discovery.

## Station network

The ASTA source is a multi-station Luxembourg agrometeorological network rather than a single-station product.

WeatherDownload currently uses the official ASTA spatial-sampling metadata layer:

- `MF.SpatialSamplingFeature_ASTA`

Stable public station identifiers are exposed from:

- `inspireid_identifier_localid`

Example station ids:

- `AGM_022` (`Arsdorf`)
- `AGM_012` (`Grevenmacher`)

Station metadata include:

- `full_name`
- `latitude`
- `longitude`
- `elevation_m`

## Supported observed elements

This first ASTA slice is intentionally conservative and exposes only the clearly identified 2 m daily temperature layers:

| Raw | Canonical | Unit |
| --- | --- | --- |
| `avg_ta200` | `tas_mean` | `degC` |
| `max_ta200max` | `tas_max` | `degC` |
| `min_ta200min` | `tas_min` | `degC` |

Official WFS layers used:

- `MF.PointTimeSeriesObservation_Daily_ASTA_avg_ta200`
- `MF.PointTimeSeriesObservation_Daily_ASTA_max_ta200max`
- `MF.PointTimeSeriesObservation_Daily_ASTA_min_ta200min`

## Date semantics

- WeatherDownload returns normalized daily rows keyed by `observation_date`
- the ASTA daily WFS JSON responses expose both `day` and `datetime`
- this implementation preserves the source-assigned date exactly and does not shift dates

## Not exposed in this first slice

The ASTA daily source appears to contain additional daily variables such as precipitation, relative humidity, pressure, sunshine duration, radiation, and wind-related measurements, but they are intentionally not advertised in this first implementation because their units and exact canonical semantics were not fully verified in code and tests yet.

## Limitations

- daily only
- observed-only
- no derived values are introduced
- not FAO-ready
- no hourly ASTA support in this pass
- no AGE support in this pass
- no 10-minute Luxembourg support is introduced here

## Relationship to MeteoLux

- `LU / meteolux / daily` remains the Findel-only national MeteoLux path
- `LU / asta / daily` is a separate ASTA station-network path
- the two Luxembourg providers intentionally stay separate so their station ids, source semantics, and future extension paths do not get mixed

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Canonical Elements](../canonical_elements.md)
- [Normalized Output Schemas](../output_schema.md)
