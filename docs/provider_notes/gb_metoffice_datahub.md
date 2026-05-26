# Met Office Weather DataHub Great Britain

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current conservative official Met Office Weather DataHub Land Observations slice implemented in WeatherDownload.

## Provider identifiers

- country: `GB`
- provider: `metoffice_datahub`
- resolution(s): `1hour`

## Source

- product: official Met Office Weather DataHub Land Observations
- auth: `apikey` header, as documented by Met Office Weather DataHub
- docs: `https://datahub.metoffice.gov.uk/docs/o/category/observations/overview`
- sample metadata JSON: `https://datahub.metoffice.gov.uk/sample-model-data/observations/download/land-observations-nearest-geohash-sample`
- sample observation JSON: `https://datahub.metoffice.gov.uk/sample-model-data/observations/download/land-observations-data-with-nearest-geohash-sample`

The Met Office documentation describes Land Observations as hourly JSON observations for individual station locations, covering the past 48 hours only.

## Station identifiers

- `station_id` is the Met Office location geohash used by this integration
- requests are made for one station location at a time
- `gh_id` remains null on this path
- station metadata parsing is currently conservative and geohash-driven

## Supported data

Current source-backed mapping for `GB / metoffice_datahub / 1hour`:

- `temperature` -> `tas_mean`
- `humidity` -> `relative_humidity`
- `wind_speed` -> `wind_speed`
- `mslp` -> `pressure`

Unsupported documented sample fields stay unsupported in this first slice, including `wind_direction`, `wind_gust`, `visibility`, `pressure_tendency`, and `weather_code`.

For the authoritative current matrix, see [Supported Capabilities](../supported_capabilities.md).

## Time and missing-data handling

- recent observations only
- hourly only
- requests outside the last 48 hours are rejected explicitly
- no daily or 10-minute Met Office DataHub path is implemented here
- only explicitly mapped observed fields are exposed
- no provider-side derivation is added

## Limitations and caveats

- live use requires `WEATHERDOWNLOAD_METOFFICE_DATAHUB_API_KEY` or `METOFFICE_DATAHUB_API_KEY`
- the current integration is observed-only
- precipitation is not exposed in this first slice because it is not verified from the checked-in official sample payload used here
- `wind_direction` is not exposed because there is no established canonical mapping for it in the current project surface
- this provider is not FAO-ready

## Related documentation

- [Provider Model](../providers.md)
- [Supported Capabilities](../supported_capabilities.md)
- [Normalized Output Schemas](../output_schema.md)
