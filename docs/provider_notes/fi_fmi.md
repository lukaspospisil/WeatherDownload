# FMI Open Data Finland

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Finnish Meteorological Institute (FMI) Open Data WFS provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `FI`
- provider: `fmi`
- resolution(s): `1hour` (no `daily` support yet)

## Station identifiers

- `station_id` is the FMI `fmisid` (numeric station id, stored and handled as a string)

## Source

- WFS base: `https://opendata.fmi.fi/wfs`
- stored query: `fmi::observations::weather::timevaluepair`
- parameters (first slice): `t2m`, `ws_10min`, `rh`, `p_sea`, `r_1h`

## Supported elements (first slice)

Raw-to-canonical mapping:

| Raw | Canonical |
| --- | --- |
| `t2m` | `tas_mean` |
| `ws_10min` | `wind_speed` |
| `rh` | `relative_humidity` |
| `p_sea` | `pressure` |
| `r_1h` | `precipitation` |

Notes:

- element requests are converted to raw WFS `parameters=...` values
- units may not be present inline in the XML payload; when present, they are preserved in parser metadata
- `p_sea` is mean sea-level pressure (MSL), mapped to canonical `pressure` in this conservative pass
- station metadata discovery currently fetches a conservative subset of stations from the FMI Environmental Monitoring Facility networks `AWS` and `SYNOP`
- `elevation_m` is not exposed by this station listing path and is currently null in WeatherDownload station metadata tables

## Not implemented (yet)

- a full-fidelity station discovery strategy beyond the conservative `AWS` + `SYNOP` subset
- additional elements beyond the conservative first slice
- `daily` resolution support
- `FI / ghcnd / daily` remains available separately as the NOAA GHCN-Daily wrapper path
