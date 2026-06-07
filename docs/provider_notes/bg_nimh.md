# NIMH Bulgaria

`BG / nimh / daily` is an official National Institute of Meteorology and Hydrology Bulgaria open-data path built from the public `info.meteo.bg/openData` daily archives. This is a national daily provider, while `BG / ghcnd / daily` remains available separately as the shared NOAA fallback.

## Source Scope

- rain archive page: `https://info.meteo.bg/openData/rain`
- snow archive page: `https://info.meteo.bg/openData/snow`
- monthly daily CSVs: `mosv_prec_YYYYMM.csv` and `mosv_snow_YYYYMM.csv`

## Access Method

- public open-data HTML archive pages expose month-specific CSV links
- the downloader discovers those monthly CSV links from the official rain and snow archive pages
- no login, token, or request-order workflow is used for this provider path

The implementation is intentionally conservative. It only exposes daily elements that are directly evidenced by the public station-level daily archives.

## Supported Daily Elements

- `precipitation` from the rain monthly CSV archive
- `snow_depth` from the snow monthly CSV archive, exposed from the provider raw field `snow_cover_depth`

Not exposed here:

- `tas_mean`, `tas_max`, `tas_min`
- `wind_speed`, `wind_speed_max`, `wind_direction`
- `relative_humidity`
- `pressure`
- `vapour_pressure`
- `sunshine_duration`
- `solar_radiation`

Those broader meteorological fields were not added to `BG / nimh / daily` because the audited public Bulgaria open-data path used here did not provide a clean station-level daily contract for them.

## Unit Handling

- `precipitation` is exposed as millimeters from the official rain CSV values
- `snow_depth` is exposed as centimeters from the official snow archive cell payload
- no unit conversion is currently required for the two implemented elements

## Parsing Notes

- Rain CSV blanks inside already reported days are treated as `0.0` precipitation.
- Rain CSV blanks after the latest reported day in a month are treated as future or unreported days and skipped.
- Snow CSV cells are parsed conservatively from the pipe-delimited daily cell payload.
- The provider uses the snow cover depth token only; it does not derive other daily fields from the snow archive payload.

## Missing-Value Handling

- rain cell `n.a.` is treated as missing
- rain blank cells on already reported days are treated as observed zero precipitation
- rain blank cells after the latest reported day in a month are skipped as future or unreported
- snow cells with `n.a`, `err`, or unsupported legend-like payloads are treated as missing
- snow cells with an empty snow-depth token inside an otherwise reported payload are treated as zero snow depth

## Metadata Notes

- Station identifiers come from the public monthly CSV station tables.
- This provider does not currently expose station coordinates or elevation from the audited open-data daily path, so those metadata fields remain null.
- This provider is not a FAO-readiness claim. It is only a narrow official-source daily downloader for the two directly supported elements above.
