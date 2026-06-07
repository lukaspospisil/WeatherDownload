# Ukraine Ukrhydromet Daily Research Note

- Audit date: 2026-06-07
- Target: `UA / ukrhydromet / daily`
- Outcome: Gate 0 failed, so no official daily provider was implemented

## Current Repository Status

WeatherDownload currently keeps Ukraine daily coverage through the mapped-prefix fallback `UA / ghcnd / daily`. The repository uses raw GHCND station ids with prefix `UP` for Ukraine, and explicitly does not use direct GHCND prefix `UA`.

No official Ukraine national daily provider is implemented in the repository.

## GHCND Fallback Status

- Current fallback path: `UA / ghcnd / daily`
- Shared-source type: mapped-prefix GHCND wrapper
- NOAA GHCND prefix used for Ukraine in this repository: `UP`
- Direct GHCND prefix `UA` is intentionally not used for Ukraine in this project

## Official Sources Checked

- Ukrainian Hydrometeorological Center sitemap:
  `https://www.meteo.gov.ua/en/sitemap/`
- Ukrainian Hydrometeorological Center operational hydrological portal:
  `https://dnister.meteo.gov.ua/en/hydro_operational_data`
- Borys Sreznevsky Central Geophysical Observatory station-network example page:
  `https://www.cgo-sreznevskyi.kyiv.ua/en/about-cgo/net/m-piskivka`
- Ukrainian Hydrometeorological Institute data repository for ClimUAd:
  `https://old.uhmi.org.ua/eng/data_repo/ClimUAd_Ukrainian_gridded_daily/`

## What Worked

- Official Ukrainian hydrometeorological institutions are clearly identifiable online.
- Official service pages and station-network pages are publicly reachable.
- Official institutional pages clearly confirm that a national observation network exists and that daily station data are used operationally.
- An official Ukrainian Hydrometeorological Institute repository exposes free daily climate data products for Ukraine.

## Why Gate 0 Failed

- The strongest openly downloadable official daily dataset I found is `ClimUAd`, which is a gridded observation-based climate dataset for Ukraine rather than station-level historical daily observations.
- The task requires station-level historical daily observations with stable station identifiers, not gridded or research-processed climate products.
- I did not find a public official station-level historical daily API or machine-readable CSV/JSON/XML/XLS archive with clear date-range semantics and missing-value handling suitable for WeatherDownload tests.
- The official service pages I found are operational/institutional or hydrological, but they do not provide a clear downloader-ready meteorological daily station archive contract.

## Wartime / Public-Data Availability Notes

- Current war and emergency conditions may affect public data availability, site stability, and how much operational station information is exposed openly.
- I did not find a public official statement in this pass that cleanly turns wartime limitations into a stable alternative downloader contract.
- Because public access patterns may be constrained or unstable, it is especially important not to guess variable semantics or scrape brittle pages into a provider contract.

## Recommended Future Action

- Keep `UA / ghcnd / daily` as the current conservative daily path.
- Revisit `UA / ukrhydromet / daily` only if Ukrhydrometcenter, the Central Geophysical Observatory, or another official Ukrainian hydrometeorological body publishes a stable public station-level historical daily archive or API.
- If official station data become openly downloadable with explicit station IDs, date-range access, and daily element semantics, that would be the best future implementation target.

## Implementation Status

No provider code was added for Ukraine in this audit. `UA / ghcnd / daily` remains unchanged as the existing mapped-prefix fallback.
