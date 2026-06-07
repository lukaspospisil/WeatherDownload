# Serbia RHMZ Daily Research Note

- Audit date: 2026-06-07
- Target: `RS / rhmz / daily`
- Outcome: Gate 0 failed, so no official daily provider was implemented

## Current Repository Status

WeatherDownload currently keeps Serbia daily coverage through the mapped-prefix fallback `RS / ghcnd / daily`. The repository uses raw GHCND station ids with prefix `RI` for Serbia, and explicitly does not use direct GHCND prefix `RS`.

No official Serbia national daily provider is implemented in the repository.

## GHCND Fallback Status

- Current fallback path: `RS / ghcnd / daily`
- Shared-source type: mapped-prefix GHCND wrapper
- NOAA GHCND prefix used for Serbia in this repository: `RI`
- Direct GHCND prefix `RS` is intentionally not used for Serbia because in GHCND it means Russia

## Official Sources Checked

- RHMZ current observations landing page:
  `https://www.hidmet.gov.rs/latin/osmotreni/index.php`
- RHMZ 24-hour precipitation page:
  `https://www.hidmet.gov.rs/latin/osmotreni/padavine.php`
- RHMZ climatology landing page:
  `https://www.hidmet.gov.rs/latin/meteorologija/klimatologija.php`
- RHMZ climatological yearbooks page:
  `https://hidmet.gov.rs/latin/meteorologija/klimatologija_godisnjaci.php`
- Official climatological annual PDF example:
  `https://www.hidmet.gov.rs/data/meteo_godisnjaci/Republika%20Srbija%20-%20Meteorolo%C5%A1ki%20godisnjak%201%20-%20klimatoloki%20podaci%20-%202023.pdf`

## What Worked

- Official RHMZ pages are publicly reachable without login.
- The official site exposes current and recent station observations, including temperature, pressure, humidity, wind, and weather-state style fields.
- The official site also exposes a 24-hour precipitation page with a clear daily accumulation window.
- RHMZ publishes official climatological yearbooks that include broad historical daily climatological material from the national station network.

## Why Gate 0 Failed

- The current and recent observation pages are operational web pages, not a documented historical daily station archive with stable date-range query semantics.
- The climatological historical material I found is published as PDF yearbooks, not as a stable machine-readable CSV/JSON/XML/XLS archive or API suitable for WeatherDownload parsers and tests.
- I did not find a public station-level historical daily endpoint with clear URL patterns, variables, missing-value semantics, and reliable machine-readable download structure.
- Although the official footprint is substantial, the public downloader contract remains publication-oriented rather than API/archive oriented, which is too brittle for a conservative implementation.

## Recommended Future Action

- Keep `RS / ghcnd / daily` as the current conservative daily path.
- Revisit `RS / rhmz / daily` only if RHMZ exposes a stable machine-readable historical daily station archive or API with station identifiers and date-range access.
- If the existing climatological material becomes available as structured CSV/XLS/JSON downloads, that would be the best future implementation target.

## Implementation Status

No provider code was added for Serbia in this audit. `RS / ghcnd / daily` remains unchanged as the existing mapped-prefix fallback.
