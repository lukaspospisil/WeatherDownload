# Bosnia and Herzegovina Meteo BiH Daily Research Note

- Audit date: 2026-06-07
- Target: `BA / meteo_bih / daily`
- Outcome: Gate 0 failed, so no official daily provider was implemented

## Current Repository Status

WeatherDownload currently keeps Bosnia and Herzegovina daily coverage through the mapped-prefix fallback `BA / ghcnd / daily`. The repository uses raw GHCND station ids with prefix `BK` for Bosnia and Herzegovina, and explicitly does not use direct GHCND prefix `BA`.

No official Bosnia and Herzegovina national daily provider is implemented in the repository.

## GHCND Fallback Status

- Current fallback path: `BA / ghcnd / daily`
- Shared-source type: mapped-prefix GHCND wrapper
- NOAA GHCND prefix used for Bosnia and Herzegovina in this repository: `BK`
- Direct GHCND prefix `BA` is intentionally not used because in GHCND it means Bahrain

## Institutional Scope Note

Meteorological operations in Bosnia and Herzegovina are institutionally split. The Federal Hydrometeorological Institute of Bosnia and Herzegovina (FHMZ BiH) covers the Federation of BiH side, while the Republic Hydrometeorological Service of Republika Srpska (RHMZRS) covers Republika Srpska. Because of that split, any official provider would need either:

- a coherent documented full-country source, or
- a clearly documented partial-coverage path that the repository can represent without overstating it as national coverage

I did not find a safe full-country public daily downloader contract.

## Official Sources Checked

- FHMZ BiH main site:
  `https://www.fhmzbih.gov.ba/`
- FHMZ BiH climate section:
  `https://www.fhmzbih.gov.ba/latinica/KLIMA/index.php`
- FHMZ BiH meteorological yearbooks:
  `https://www.fhmzbih.gov.ba/latinica/KLIMA/godisnjaci.php`
- FHMZ BiH flood/operations report site:
  `https://www.fop.fhmzbih.gov.ba/`
- RHMZRS main site:
  `https://rhmzrs.com/index.php`

## What Worked

- Official sources are publicly visible on both the Federation and Republika Srpska sides.
- FHMZ BiH clearly publishes climatology pages, meteorological yearbooks, and station-network oriented climate material.
- RHMZRS clearly publishes daily report-style files for individual locations.
- The official public footprint is enough to confirm real meteorological operations and real historical reporting on both entity sides.

## Why Gate 0 Failed

- The official source landscape is split across entity institutions rather than exposing one clearly documented full-country station archive.
- The strongest historical material I found from FHMZ BiH is meteorological yearbooks and climate-monitoring pages, which are publication-oriented rather than a stable machine-readable station-level daily API/archive.
- The visible RHMZRS material is daily report file publication, but I did not verify a stable historical date-range machine-readable station archive suitable for WeatherDownload tests.
- I did not find a coherent public CSV/JSON/XML/XLS daily downloader contract with clear station identifiers, missing-value semantics, and date-range access across the official source scope.
- Because the available public materials are split and publication-oriented, implementing a single `BA / meteo_bih / daily` provider would risk overstating partial/entity coverage as full national coverage.

## Recommended Future Action

- Keep `BA / ghcnd / daily` as the current conservative daily path.
- Revisit Bosnia and Herzegovina only if:
  - one official source exposes a stable full-country machine-readable historical daily archive, or
  - the repository gains a clear way to represent officially partial entity-specific daily coverage without mislabeling it as national
- If either FHMZ BiH or RHMZRS later exposes structured station-level daily downloads with clear scope, that could justify a narrower future implementation.

## Implementation Status

No provider code was added for Bosnia and Herzegovina in this audit. `BA / ghcnd / daily` remains unchanged as the existing mapped-prefix fallback.
