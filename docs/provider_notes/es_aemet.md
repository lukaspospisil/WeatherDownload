# AEMET Spain

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

Current first-pass provider path:

- `country="ES"`
- `provider="aemet"`
- `resolution="daily"`

Source:

- AEMET OpenData daily climatological observations
- station inventory: `inventarioestaciones/todasestaciones`
- daily data: station-scoped climatology endpoint under `valores/climatologicos/diarios`

Credentials:

- live downloads require an AEMET OpenData API key
- accepted environment variables:
  - `WEATHERDOWNLOAD_AEMET_API_KEY`
  - `AEMET_API_KEY`
- WeatherDownload sends the key as the `api_key` HTTP header

Observed elements in the current pass:

- `tas_mean` from raw `tmed`
- `tas_max` from raw `tmax`
- `tas_min` from raw `tmin`
- `precipitation` from raw `prec`
- `wind_speed` from raw `velmedia`
- `relative_humidity` from raw `hrMedia`
- `sunshine_duration` from raw `sol`

Units and normalization:

- `tmed`, `tmax`, `tmin`: degrees Celsius
- `prec`: millimetres
- `sol`: hours
- `hrMedia`: percent
- `velmedia`: treated as km/h and converted to canonical m/s by dividing by `3.6`
- AEMET decimal strings use commas and are normalized to decimal points
- trace precipitation `prec="Ip"` is mapped to `0.0 mm` as a below-measurable-threshold trace

Caveats:

- hourly and 10-minute support are intentionally not implemented in this first pass
- `hrMax`, `hrMin`, `horaHrMax`, and `horaHrMin` are visible in AEMET daily examples but are not mapped in this slice
- observed `vapour_pressure` is not exposed
- the provider is still not provider-level FAO-ready because `vapour_pressure` is not an observed provider field
- the shared FAO workflow example now supports `ES / aemet / daily` only through the existing explicit `--fill-missing allow-derived` workflow-layer fallback for `vapour_pressure` from observed `tas_mean` plus observed `relative_humidity`
- that compatibility is workflow-level only; derived `vapour_pressure` remains `derived_opt_in` provenance rather than observed provider data
- station inventory metadata does not currently provide clean coverage dates, so `all_history=True` is not implemented
