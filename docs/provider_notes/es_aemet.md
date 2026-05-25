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

Observed elements in the first pass:

- `tas_mean` from raw `tmed`
- `tas_max` from raw `tmax`
- `tas_min` from raw `tmin`
- `precipitation` from raw `prec`
- `wind_speed` from raw `velmedia`
- `sunshine_duration` from raw `sol`

Units and normalization:

- `tmed`, `tmax`, `tmin`: degrees Celsius
- `prec`: millimetres
- `sol`: hours
- `velmedia`: treated as km/h and converted to canonical m/s by dividing by `3.6`
- AEMET decimal strings use commas and are normalized to decimal points
- trace precipitation `prec="Ip"` is mapped to `0.0 mm` as a below-measurable-threshold trace

Caveats:

- hourly and 10-minute support are intentionally not implemented in this first pass
- relative humidity and vapour pressure are intentionally not exposed yet
- the provider is observed-only and not FAO-ready in this first pass
- station inventory metadata does not currently provide clean coverage dates, so `all_history=True` is not implemented
