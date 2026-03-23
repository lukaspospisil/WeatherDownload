# Changelog

## Unreleased

### Added

- experimental Slovakia provider via SHMU OpenDATA for the narrow `SK / recent / daily` station-observation slice
- canonical element support for the current SHMU slice:
  - `tas_max`
  - `tas_min`
  - `sunshine_duration`
  - `precipitation`
- minimal probe-derived station discovery for the current SHMU recent daily payload
- experimental SHMU probe example and provider notes for maintainers

### Notes

- Slovakia support remains experimental and intentionally limited to recent daily data
- SHMU station metadata are currently minimal and probe-derived
- validated historical Slovakia climate downloads are not implemented
