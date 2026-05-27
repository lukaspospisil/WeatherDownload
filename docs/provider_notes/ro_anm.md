# RO ANM

Source: Romanian National Meteorological Administration (ANM) INSPIRE/WFS station dataset, using the station-level M201 CLIMAT daily series exposed through ANM INSPIRE WaterML endpoints.

Implemented path:

- `country=RO`
- `provider=anm`
- `resolution=daily`

Supported observed daily elements:

- `tas_mean`
- `tas_max`
- `tas_min`
- `precipitation`

Unsupported in this first pass:

- `snow_depth`
- wind, humidity, pressure, solar radiation, sunshine duration, vapour pressure
- any derived variables
- FAO-ready status

Notes:

- This is a separate national provider and does not replace `RO / ghcnd / daily`.
- Values are station-level observed ANM CLIMAT series, not gridded products.
- Temperature is published by ANM in kelvin and converted to canonical degrees Celsius.
- Daily total precipitation is published in `kg/m2`, which is treated numerically as canonical millimeters.
- Station discovery comes from the ANM INSPIRE network/station WFS documents.
- Observation downloads currently fetch one station/element WaterML series at a time and filter the requested date range client-side, because a stable server-side date-sliced WaterML query was not confirmed during audit.
