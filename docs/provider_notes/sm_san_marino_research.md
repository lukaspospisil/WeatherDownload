# San Marino Daily Coverage Research Note

Audit date: 2026-06-07

Target:
- country: `SM`
- provider: `ghcnd` fallback feasibility and official San Marino daily source audit
- resolution: `daily`

Current repository status:
- `SM` daily coverage remains `not_attempted`
- no `weatherdownload/providers/sm/` package is implemented
- no provider was implemented from this audit

## NOAA GHCND prefix findings

Official NOAA GHCN-Daily country metadata was checked first.

Sources checked:
- `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt`
- `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
- `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`

What worked:
- the official NOAA metadata endpoints were reachable
- the country-code list could be inspected directly

What was found:
- NOAA GHCND does not list an `SM` country prefix for San Marino
- `SM` is therefore not a safe direct-prefix wrapper candidate
- this audit did not find an accepted San Marino-specific mapped GHCND prefix in NOAA metadata

Conclusion:
- `SM / ghcnd / daily` was not implemented
- San Marino must not be mapped to Italy or any neighboring country for convenience
- neighboring-country stations are not acceptable as San Marino coverage

## Official San Marino sources checked

Only official San Marino government or civil-protection sources were considered for Gate 0.

Sources checked:
- `https://www.gov.sm/`
- `https://www.territorio.sm/pub1/TerritorioSM/Protezione-Civile.html`
- `https://gov.sm/pub2/GovSM/La-PA-Risponde/FAQ.html?tema=Servizio-di-Protezione-Civile`
- official San Marino civil-protection ordinances and planning PDFs linked from `gov.sm`

What worked:
- the official government and civil-protection pages were reachable
- the pages make the responsible public bodies identifiable
- the official documentation clearly shows San Marino receives meteorological alerting through the Emilia-Romagna civil-protection warning chain

## Why coverage could not be implemented

Gate 0 failed for an official `SM` daily provider.

Why it failed:
- no public machine-readable station metadata export was found on official San Marino sources
- no public station-level historical daily observations API, CSV, JSON, XLS, or similar archive was confirmed
- the official material found was civil-protection, alerting, FAQ, and ordinance content rather than a stable daily station-data contract
- the official pages reference alerting and warning products, not a reproducible historical daily observations service suitable for WeatherDownload tests
- no clear official variable semantics, unit rules, missing-value rules, or stable historical date-access pattern was established

## Recommended future action

- keep `SM` daily coverage unchanged for now
- if a future official San Marino source exposes public station-level historical daily observations with stable identifiers and documented semantics, re-audit from that source
- if future official NOAA metadata introduces a distinct San Marino GHCND mapping, reassess conservative fallback feasibility from the NOAA metadata directly

No provider was implemented from this audit.
