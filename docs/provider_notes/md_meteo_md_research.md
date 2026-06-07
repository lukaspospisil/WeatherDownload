# Moldova Meteo.md Research Note

Audit date: `2026-06-07`

Target: `MD / meteo_md / daily`

Intended goal: confirm whether the State Hydrometeorological Service of Moldova exposes a stable official public machine-readable historical daily station source suitable for WeatherDownload.

## Outcome

Gate 0 failed. No `MD / meteo_md / daily` provider was implemented.

## Official sources checked

- `https://www.meteo.md/en/`
- `https://www.meteo.md/index.php/ru/weather/synop-archive`
- `https://www.meteo.md/index.php/en/about/meteo_center_en/meteo_network_en/`
- `https://www.meteo.md/index.php/en/about/meteo_center_en/mcc/`
- `https://old.meteo.md/newen/mcmcmon.htm`

## What worked

- The official `meteo.md` and `old.meteo.md` pages were reachable from a clean script using normal HTTPS requests.
- The official site exposes stable station names and station codes directly in page HTML for the national meteorological network.
- The official meteorological network page documents observed variables at stations, including temperature, precipitation, snow cover, humidity, wind, pressure, and solar-radiation-related observations.
- The official `synop-archive` page exists and exposes an official archive form with date and time inputs.
- The Meteorology and Climatology Center page confirms that the service maintains a long meteorological database and offers products such as meteorological data tables from stations and posts.

## Why Gate 0 failed

- The clearest live archive surface found was `synop-archive`, and it is explicitly a date-and-time query, which points to archived SYNOP-style snapshots rather than a station-level daily observations archive.
- The official archive form posts to `https://www.meteo.md/index.php?ACT=29`, but in scripted testing that POST returned a 1-byte opaque HTML response rather than a stable, parseable results page or machine-readable payload.
- No official CSV, JSON, XLS, or other machine-readable daily station download endpoint was discovered on the modern site pages inspected.
- The older official documentation indicates “Data on daily observations (for the current month)” and other tabular products such as TMS and TMP, but this does not establish a public historical daily archive suitable for WeatherDownload.
- The official MCC page suggests that detailed tabular meteorological products exist as service outputs, but it does not expose a clear public self-service machine-readable historical daily contract.
- I did not find a documented public endpoint with clear daily variable names, units, missing-value semantics, and historical date-interval behavior that could be implemented conservatively and tested reliably.

## Current repository status

- `MD / meteo_md / daily` is not implemented.
- `MD` should remain on `MD / ghcnd / daily` fallback.

## Recommended future action

- Revisit only if the official Moldovan SHS site publishes a public historical daily station archive or API with machine-readable responses, stable station metadata, and clear units and missing-value rules.
- Revisit if the official archive form begins returning stable parsable results or downloadable daily files instead of an opaque response.
- Do not build `MD / meteo_md / daily` from the current SYNOP archive form, service/product descriptions, or current-month references alone.

## Provider decision

No provider was implemented in this audit.
