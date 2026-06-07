# Vedur Iceland

This note documents the official `IS / vedur / daily` path built on the Icelandic Meteorological Office Weather API at `api.vedur.is`. The shared `IS / ghcnd / daily` wrapper remains available separately as the fallback NOAA GHCN-Daily path.

## Source

- station metadata: `https://api.vedur.is/weather/stations`
- parameter descriptions: `https://api.vedur.is/weather/parameters`
- synop daily observations: `https://api.vedur.is/weather/observations/synop/day`
- AWS daily observations: `https://api.vedur.is/weather/observations/aws/day`
- official station UI: `https://athuganir.vedur.is/?lng=en`

The implementation uses official daily aggregates directly. It does not derive daily values from current-only latest feeds, forecasts, gridded products, or third-party wrappers.

## Station Scope

- active `sj` stations use the official AWS daily endpoint
- active `sk` and `ur` stations use the official synop daily endpoint
- the provider reads the official machine-readable station list and routes each station to the matching daily endpoint conservatively

## Supported Elements

- `tas_mean`: raw `t`
- `tas_max`: raw `txx` on synop stations, raw `tx` on AWS stations
- `tas_min`: raw `tnn` on synop stations, raw `tn` on AWS stations
- `precipitation`: raw `r`
- `wind_speed`: raw `f`
- `wind_speed_max`: raw `fg`
- `relative_humidity`: raw `rh`
- `pressure`: raw `p` as sea-level pressure
- `vapour_pressure`: raw `vp`
- `snow_depth`: raw `snd`, synop stations only
- `sunshine_duration`: raw `sun` on synop stations, raw `rsun` on AWS stations

## Unsupported Or Postponed

- `solar_radiation` is intentionally not exposed from AWS `radgl` because the official daily parameter is documented in `W/m^2`, not the canonical energy unit `MJ m^-2`
- `wind_direction` is not exposed in this first pass
- `cloud_cover` is not exposed in this first pass even though synop daily responses may contain `n`
- `snow_depth` is unavailable on AWS stations because the official AWS daily schema does not expose `snd`
- forecasts, model values, gridded products, and climate indices stay out of scope

## Units And Missing Values

- no provider-side unit conversions are applied for the supported daily elements
- temperatures stay in `degC`
- precipitation stays in `mm`
- wind speeds stay in `m/s`
- humidity stays in `%`
- pressure and vapour pressure stay in `hPa`
- snow depth stays in `cm`
- sunshine duration stays in `hr`
- JSON `null` values are preserved as missing values in the normalized output

## FAO Status

This path is not treated as FAO-ready in the current repository. It does not expose canonical `solar_radiation`, and this note does not claim provider-side derivation beyond the official published daily fields.
