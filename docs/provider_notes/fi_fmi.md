# FMI Open Data Finland

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

This note documents the current Finnish Meteorological Institute (FMI) Open Data WFS provider slice implemented in WeatherDownload.

## Provider identifiers

- country: `FI`
- provider: `fmi`
- resolution(s): `1hour`, `daily` (daily is a conservative first slice)

## Station identifiers

- `station_id` is the FMI `fmisid` (numeric station id, stored and handled as a string)

## Source

- WFS base: `https://opendata.fmi.fi/wfs`
- stored query (1hour): `fmi::observations::weather::timevaluepair`
- parameters (1hour first slice): `t2m`, `ws_10min`, `rh`, `p_sea`, `r_1h`
- stored query (daily): `fmi::observations::weather::daily::timevaluepair`
- parameters (daily first slice): `tday`, `tmin`, `tmax`, `rrday`

## Supported elements (1hour first slice)

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

## Supported elements (daily first slice)

Raw-to-canonical mapping:

| Raw | Canonical |
| --- | --- |
| `tday` | `tas_mean` |
| `tmin` | `tas_min` |
| `tmax` | `tas_max` |
| `rrday` | `precipitation` |

Notes:

- daily timestamps are returned as `00:00Z` instants; the semantics are daily aggregates
- `snow` is available in FMI daily outputs (unit `cm`) but is intentionally not mapped in WeatherDownload yet

## Not implemented (yet)

- a full-fidelity station discovery strategy beyond the conservative `AWS` + `SYNOP` subset
- additional elements beyond the conservative first slice
- additional daily elements beyond `tday/tmin/tmax/rrday` (including `snow`)
- `FI / ghcnd / daily` remains available separately as the NOAA GHCN-Daily wrapper path
