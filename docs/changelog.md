# Changelog

## Unreleased

### Added

- Austria provider via the official GeoSphere Austria Dataset API for the narrow `AT / historical / daily` station-observation slice
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
- GeoSphere daily quality is normalized from companion `<parameter>_flag` columns into `quality`, while `flag` remains null
- Slovakia support remains experimental and intentionally limited to recent daily data
- SHMU station metadata are currently minimal and probe-derived
- validated historical Slovakia climate downloads are not implemented
