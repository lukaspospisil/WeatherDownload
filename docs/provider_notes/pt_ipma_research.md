# Portugal IPMA Research Note

Audit date: `2026-06-07`

Target: `PT / ipma / daily`

Intended goal: confirm whether IPMA exposes a stable official public machine-readable national station-level historical daily source suitable for WeatherDownload.

## Outcome

Gate 0 failed. No `PT / ipma / daily` provider was implemented.

## Official sources checked

- `https://api.ipma.pt/open-data/observation/meteorology/stations/stations.json`
- `https://api.ipma.pt/open-data/observation/meteorology/stations/observations.json`
- `https://www.ipma.pt/en/oclima/series.longas/list.jsp`
- `https://www.ipma.pt/en/oclima/series.longas/list-long-series-stations.json`
- `https://api.ipma.pt/open-data/observation/climate/monthly-long-series/tmintmaxdaily_1855-2018_Lisbon-Geofisico.xlsx`
- `https://api.ipma.pt/open-data/observation/climate/monthly-long-series/precdaily_1863-2018_Lisbon-Geofisico_28072020.xls`
- `https://api.ipma.pt/open-data/observation/climate/monthly-long-series/pressdaily_Lisbon-Geofisico_igidl_1864-2006_no-grav-correction_28072020.xlsx`
- `https://www.ipma.pt/en/otempo/obs.superficie/index-map-dia-chart.jsp`

## What worked

- Official IPMA station metadata is publicly reachable.
- The official `observations.json` endpoint is publicly reachable.
- Official long-series daily files exist for Lisboa / Geofisico.
- The long-series materials expose some daily temperature, precipitation, and pressure history.

## Why Gate 0 failed

- The official station observations endpoint is a recent hourly feed only.
- The live observations payload covered only one recent 24-hour interval, not historical daily observations.
- The long-series daily files appear limited to Lisboa / Geofisico rather than a national station network.
- The long-series data do not provide a clean national daily station contract suitable for WeatherDownload discovery and downloading.
- The precipitation long-series file is legacy `.xls`, which is not a good implementation path under the repository's no-new-optional-dependencies constraint.
- Other official pages checked were chart/UI or municipality-style climate products, not a stable station-level historical daily feed.

## Current repository status

- `PT / ipma / daily` is not implemented.
- `PT` should remain with official `PT / ipma / 1hour` if present, plus `PT / ghcnd / daily` fallback.

## Recommended future action

- Revisit only if IPMA publishes a public national station-level historical daily endpoint or archive with clear metadata, date-interval support, and stable machine-readable access.
- Do not build a provider from the single-station Lisboa / Geofisico long-series files.
- Do not add `PT / ipma / daily` on the basis of chart pages or legacy `.xls` files alone.

## Provider decision

No provider was implemented in this audit.
