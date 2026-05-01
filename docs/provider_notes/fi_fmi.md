# FMI Open Data Finland

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Finnish Meteorological Institute (FMI) Open Data WFS provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `FI`
- provider: `fmi`
- resolution(s): `1hour`

## Station identifiers

- `station_id` is the FMI `fmisid` (numeric station id, stored and handled as a string)

## Source

- WFS base: `https://opendata.fmi.fi/wfs`
- stored query: `fmi::observations::weather::timevaluepair`
- parameters (first slice): `t2m`, `ws_10min`

## Supported elements (first slice)

Raw-to-canonical mapping:

| Raw | Canonical |
| --- | --- |
| `t2m` | `tas_mean` |
| `ws_10min` | `wind_speed` |

Notes:

- element requests are converted to raw WFS `parameters=...` values
- units may not be present inline in the XML payload; when present, they are preserved in parser metadata

## Not implemented (yet)

- station metadata discovery via `fmi::ef::stations` (live station listing is implemented; network selection is currently conservative)
- additional elements beyond the conservative first slice
- `daily` resolution support
