# SI ARSO

Source: Slovenian Environment Agency (ARSO) public WebMet archive endpoints behind the meteo.si time-series archive UI, using station-level daily climate observations.

Implemented path:

- `country=SI`
- `provider=arso`
- `resolution=daily`

Supported observed daily elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`
- `snow_depth`
- `sunshine_duration`

Unsupported in this first pass:

- wind, humidity, pressure, solar radiation, vapour pressure
- any derived variables
- FAO-ready status

Notes:

- This is a separate national provider and does not replace `SI / ghcnd / daily`.
- Values are station-level observed ARSO archive series, not gridded or reanalysis products.
- Daily temperatures and precipitation are used directly from ARSO source units `°C` and `mm`.
- Snow depth is published by ARSO in `cm` and converted to canonical `mm`.
- Sunshine duration is published in hours and used directly.
- Station discovery comes from the public `webmet/archive/locations.xml` endpoint, which exposes station id, name, lon, lat, elevation, and station type.
- Observation downloads use the public `webmet/archive/data.xml` endpoint with server-side station, variable, and date-range filtering.
- The ARSO daily archive metadata exposed through `settings.xml` documents a historical lower bound of `1948-01-01`.
