# Vatican City Daily Coverage Research Note

Audit date: 2026-06-07

Target:
- country: `VA`
- provider: `ghcnd` fallback feasibility and official Vatican daily source audit
- resolution: `daily`

Current repository status:
- `VA` daily coverage remains `not_attempted`
- no `weatherdownload/providers/va/` package is implemented
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
- NOAA GHCND does not list a `VA` country prefix for Vatican City
- `VA` is therefore not a safe direct-prefix wrapper candidate
- this audit did not find an accepted Vatican-specific mapped GHCND prefix in NOAA metadata

Conclusion:
- `VA / ghcnd / daily` was not implemented
- Vatican City must not be mapped to Italy or any neighboring country for convenience
- neighboring-country stations, including Rome stations, are not acceptable as Vatican City coverage

## Official Vatican sources checked

Only official Vatican City State or Holy See sources were considered for Gate 0.

Sources checked:
- `https://www.vaticanstate.va/en/state-and-government/structure-of-the-government/directorates/tag-manager/specola-vaticana.html`
- `https://www.vaticanstate.va/en/news/2012-visits-to-the-vatican-observatory.html`
- `https://www.vaticanstate.va/en/news/2492-interview-with-the-new-director-of-the-vatican-observatory-father-richard-d-souza-s-j.html`
- `https://www.vaticanstate.va/images/trimestrale/2026/1-2026-eng.pdf`

What worked:
- the official Vatican Observatory pages were reachable
- the pages confirm that the Vatican Observatory is an official scientific body under the Governorate of Vatican City State
- the official material confirms active meteorological and climate research at the Vatican Observatory

## Why coverage could not be implemented

Gate 0 failed for an official `VA` daily provider.

Why it failed:
- no public machine-readable station metadata export was found on official Vatican sources
- no public station-level historical daily observations API, CSV, JSON, XLS, or similar archive was confirmed
- the official material found was institutional, historical, and research-oriented rather than a stable daily station-data contract
- the 2026 official Vatican Observatory publication explicitly says the Observatory "does not need its own observation sites" and that the research relies on access to datasets and collaboration, which does not establish a Vatican City station-level daily feed
- no clear official variable semantics, unit rules, missing-value rules, or stable historical date-access pattern was established for Vatican City daily observations

## Recommended future action

- keep `VA` daily coverage unchanged for now
- if a future official Vatican source exposes public station-level historical daily observations with stable identifiers and documented semantics, re-audit from that source
- if future official NOAA metadata introduces a distinct Vatican City GHCND mapping, reassess conservative fallback feasibility from the NOAA metadata directly

No provider was implemented from this audit.
