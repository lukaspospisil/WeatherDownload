# Turkey MGM Daily Research Note

- Audit date: 2026-06-07
- Target: `TR / mgm / daily`
- Outcome: Gate 0 failed, so no official daily provider was implemented

## Current Repository Status

WeatherDownload currently keeps Turkey daily coverage through the mapped-prefix fallback `TR / ghcnd / daily`. The repository uses raw GHCND station ids with prefix `TU` for Turkey, and explicitly does not use direct GHCND prefix `TR`.

No official Turkey national daily provider is implemented in the repository.

## GHCND Fallback Status

- Current fallback path: `TR / ghcnd / daily`
- Shared-source type: mapped-prefix GHCND wrapper
- NOAA GHCND prefix used for Turkey in this repository: `TU`
- Direct GHCND prefix `TR` is intentionally not used for Turkey in this project

## Official Sources Checked

- MGM main site:
  `https://mgm.gov.tr/`
- MGM data/info access page:
  `https://www.mgm.gov.tr/site/bilgi-edinme.aspx`
- MGM meteorological data information page:
  `https://www.mgm.gov.tr/site/bilgi-edinme.aspx?r=d`
- MGM archive page:
  `https://www.mgm.gov.tr/kurumsal/arsiv.aspx`
- MGM data page:
  `https://www.mgm.gov.tr/veriler.aspx`
- MGM about page:
  `https://www.mgm.gov.tr/eng/about.aspx`

## What Worked

- Official MGM pages are publicly reachable without login.
- The official site clearly establishes MGM as the national meteorological authority for Türkiye.
- MGM publishes broad archive material, climate analyses, forecasts, and official climate reports.
- The official site confirms that meteorological observations are collected and stored in the internal database.

## Why Gate 0 Failed

- The clearest official statement on the data-access page says that meteorological information and statistical data beyond what is already published on the website are provided through a fee/manual workflow, which fails the no-paid/no-manual-order rule.
- I did not find a public station-level historical daily API or stable CSV/JSON/XML/XLS download contract suitable for WeatherDownload tests.
- The public archive and climate pages are oriented around reports, analyses, maps, forecasts, and climate summaries rather than a downloader-ready historical daily station archive.
- I did not find a clear public official station metadata export paired with stable date-range daily observation downloads and explicit missing-value semantics.
- Because the open public footprint is publication-oriented and the stronger data-access path appears to be paid/manual, a conservative scripted provider would not meet repository requirements.

## Recommended Future Action

- Keep `TR / ghcnd / daily` as the current conservative daily path.
- Revisit `TR / mgm / daily` only if MGM exposes a public machine-readable historical daily station archive or API with stable station identifiers and date-range access.
- If the current public site eventually exposes downloadable daily station datasets directly, that would be the best future implementation target.

## Implementation Status

No provider code was added for Turkey in this audit. `TR / ghcnd / daily` remains unchanged as the existing mapped-prefix fallback.
