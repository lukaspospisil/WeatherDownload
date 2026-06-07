# North Macedonia UHMR Daily Research Note

- Audit date: 2026-06-07
- Target: `MK / uhmr / daily`
- Outcome: Gate 0 failed, so no official daily provider was implemented

## Current Repository Status

WeatherDownload currently keeps North Macedonia daily coverage through the thin shared fallback `MK / ghcnd / daily`. No official North Macedonia national daily provider is implemented in the repository.

The existing fallback follows the project's direct-prefix GHCND convention for `MK`, while documentation already notes that NOAA's GHCND country list labels the `MK` prefix as "Macedonia". In repository docs and coverage output, this path is treated as North Macedonia coverage.

## Official Sources Checked

- UHMR main site:
  `https://uhmr.gov.mk/`
- UHMR current meteorological data page:
  `https://uhmr.gov.mk/aktuelni-podatoci/`
- UHMR daily hydrological report page:
  `https://uhmr.gov.mk/dneven-hidroloshki-izveshtaj/`
- UHMR three-day forecast page:
  `https://uhmr.gov.mk/tridnevna-prognoza/`
- UHMR SYNOP/depesha interface:
  `https://synop.meteo.gov.mk/`

## What Worked

- Official UHMR pages clearly exist and are identifiable as the national hydrometeorological service.
- The public site exposes current meteorological monitoring content and forecast products.
- Public snippets from the official current-data page show station names and current observation fields such as pressure, temperature, humidity, wind, precipitation, and snow.
- The UHMR site structure also advertises climatology, agrometeorology, and data-related sections.

## Why Gate 0 Failed

- I did not find a public machine-readable historical daily station archive in CSV, JSON, XML, XLS, or similar form.
- The visible official meteorological data page is current or near-real-time station output, not a documented historical daily archive with date-range access.
- The official SYNOP/depesha interface appears to require login, so it does not satisfy the no-login public-access rule.
- I did not find a stable public daily station endpoint with clear variables, units, missing-value handling, and historical date-interval semantics suitable for WeatherDownload tests.
- During direct live fetch attempts, the main UHMR pages were also not reliably retrievable through the browsing tool, which further weakens confidence in a conservative scripted implementation path.

## Recommended Future Action

- Keep `MK / ghcnd / daily` as the current conservative daily path.
- Revisit `MK / uhmr / daily` only if UHMR exposes a stable public historical daily station archive or API with machine-readable payloads and clear station/date semantics.
- If UHMR publishes a public climatology or database export section with station-level downloadable daily observations, that would be the best future implementation target.

## Implementation Status

No provider code was added for North Macedonia in this audit. `MK / ghcnd / daily` remains unchanged as the existing fallback.
