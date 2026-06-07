# Belarus Belhydromet Daily Research Note

- Audit date: 2026-06-07
- Target: `BY / belhydromet / daily`
- Outcome: Gate 0 failed, so no official daily provider was implemented

## Current Repository Status

WeatherDownload currently keeps Belarus daily coverage through the mapped-prefix fallback `BY / ghcnd / daily`. The repository uses raw GHCND station ids with prefix `BO` for Belarus, and explicitly does not use direct GHCND prefix `BY`.

No official Belarus national daily provider is implemented in the repository.

## GHCND Fallback Status

- Current fallback path: `BY / ghcnd / daily`
- Shared-source type: mapped-prefix GHCND wrapper
- NOAA GHCND prefix used for Belarus in this repository: `BO`
- Direct GHCND prefix `BY` is intentionally not used because this project uses `BO` for Belarus

## Official Sources Checked

- Belhydromet main domain:
  `https://www.belgidromet.by/`
- Belhydromet weather subdomain:
  `https://www.meteo.belgidromet.by/`
- Hydromet domain associated with the Belarus hydrometeorological service:
  `https://hydromet.by/`

## What Worked

- The official Belarus hydrometeorological service identity is clear and publicly referenced through Belhydromet domains.
- Public-facing weather and service surfaces appear to exist for Belhydromet.
- The available public footprint is enough to confirm that Belhydromet operates the national observation network and publishes weather information.

## Why Gate 0 Failed

- I did not find a public official station-level historical daily archive in CSV, JSON, XML, XLS, or similar machine-readable form.
- I did not find a stable public API or date-range downloader contract for historical daily meteorological station observations.
- The accessible public footprint appears oriented toward current weather/service presentation rather than a documented historical daily station data export.
- I did not verify clear public station metadata paired with explicit daily variable semantics, missing-value handling, and stable download patterns suitable for WeatherDownload tests.
- Because the public official contract could not be established conservatively from the official sources checked, implementing a scripted provider would be too brittle for this repository.

## Recommended Future Action

- Keep `BY / ghcnd / daily` as the current conservative daily path.
- Revisit `BY / belhydromet / daily` only if Belhydromet exposes a public machine-readable historical daily station archive or API with stable station identifiers and date-range access.
- If an official open-data or data-download section appears on Belhydromet domains with clear daily observation semantics, that would be the best future implementation target.

## Implementation Status

No provider code was added for Belarus in this audit. `BY / ghcnd / daily` remains unchanged as the existing mapped-prefix fallback.
