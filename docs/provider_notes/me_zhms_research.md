# Montenegro ZHMS Daily Research Note

- Audit date: 2026-06-07
- Target: `ME / zhms / daily`
- Outcome: Gate 0 failed, so no official daily provider was implemented

## Current Repository Status

WeatherDownload currently keeps Montenegro daily coverage through the mapped-prefix fallback `ME / ghcnd / daily`. The repository uses raw GHCND station ids with prefix `MJ` for Montenegro, and explicitly does not use direct GHCND prefix `ME`.

No official Montenegro national daily provider is implemented in the repository.

## GHCND Fallback Status

- Current fallback path: `ME / ghcnd / daily`
- Shared-source type: mapped-prefix GHCND wrapper
- NOAA GHCND prefix used for Montenegro in this repository: `MJ`
- Direct GHCND prefix `ME` is intentionally not used for Montenegro

## Official Sources Checked

- ZHMS main site:
  `https://www.meteo.co.me/index.php?lang=EN`
- ZHMS reports page:
  `https://www.meteo.co.me/page.php?keyword=reports`
- Montenegro open-data dataset for the annual meteorological and hydrological yearbook:
  `https://opendata.gov.me/en_GB/dataset/godisnjak-meteoroloskih-i-hidroloskih-podataka-2023`
- Official data request form referenced from the official ZHMS domain:
  `https://www.meteo.co.me/Data%20request%20form.pdf`

## What Worked

- Official ZHMS pages are publicly visible and clearly identify the national hydrometeorological service.
- The official site exposes weather, climate report, agrometeorological, and other operational sections.
- The Montenegro open-data portal does publish official ZHMS material.
- An official annual meteorological and hydrological yearbook is available through the national open-data portal.

## Why Gate 0 Failed

- The official open-data resource exposed here is a yearbook PDF, not a stable machine-readable station-level historical daily archive.
- I did not find a public CSV, JSON, XML, or XLS endpoint with station-level daily observations and historical date-range access suitable for WeatherDownload.
- The official ZHMS ecosystem appears to support manual data-request workflow through a request form, which does not satisfy the no-manual-order/no-interactive-workflow rule.
- I did not find clear public machine-readable station metadata paired with a historical daily observation feed and explicit missing-value semantics.
- Because the official public footprint is publication-oriented and request-form oriented rather than downloader-contract oriented, a conservative scripted provider would be too brittle for this repository.

## Recommended Future Action

- Keep `ME / ghcnd / daily` as the current conservative daily path.
- Revisit `ME / zhms / daily` only if ZHMS or the Montenegro open-data portal publishes a stable machine-readable historical daily station archive or API.
- If future official resources expose station identifiers, daily element semantics, and downloadable date-range daily observations, that would be the best implementation target.

## Implementation Status

No provider code was added for Montenegro in this audit. `ME / ghcnd / daily` remains unchanged as the existing mapped-prefix fallback.
