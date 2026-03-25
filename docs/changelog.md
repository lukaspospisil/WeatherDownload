# Changelog

## Unreleased

### Added

- Austria provider via the official GeoSphere Austria Dataset API for the narrow `AT / historical / daily` station-observation slice
- Belgium provider via the official RMI/KMI open-data AWS platform for `BE / historical / daily`, `1hour`, and `10min`
- Denmark provider via the official DMI open-data APIs for `DK / historical / daily`, `1hour`, and `10min`
- Netherlands provider via the official KNMI Data Platform for the narrow `NL / historical / daily` validated station-observation slice
- Sweden provider via the official SMHI Meteorological Observations API for `SE / historical / daily` and `1hour`
- canonical daily element support for the current Austria slice:
  - `tas_mean`
  - `tas_max`
  - `tas_min`
  - `precipitation`
  - `sunshine_duration`
  - `wind_speed`
  - `pressure`
  - `relative_humidity`
- GeoSphere Austria station discovery from the official `klima-v2-1d` metadata endpoint
- Austria provider notes and a minimal daily example script
- shared `examples/download_fao.py` workflow for observed daily FAO-oriented input packaging across the implemented supported countries
- optional `.info` sidecar files for shared `download_fao` exports, recording fill policy plus observed/derived/missing field counts
- experimental Slovakia provider via SHMU OpenDATA for the narrow `SK / recent / daily` station-observation slice
- canonical element support for the current SHMU slice:
  - `tas_max`
  - `tas_min`
  - `sunshine_duration`
  - `precipitation`
- minimal probe-derived station discovery for the current SHMU recent daily payload
- experimental SHMU probe example and provider notes for maintainers

### Notes

- Austria support is currently limited to `historical / daily`
- Belgium support currently covers `historical / daily`, `1hour`, and `10min`
- Denmark support currently covers `historical / daily`, `1hour`, and `10min`
- GeoSphere daily quality is normalized from companion `<parameter>_flag` columns into `quality`, while `flag` remains null
- Netherlands support is currently limited to `historical / daily` and requires a KNMI API key
- Sweden support is currently limited to `historical / daily` and `1hour`
- Slovakia support remains experimental and intentionally limited to recent daily data
- SHMU station metadata are currently minimal and probe-derived
- validated historical Slovakia climate downloads are not implemented
