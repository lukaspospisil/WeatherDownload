# Monaco Research Note

Audit date: `2026-06-07`

Target: `MC / daily`

Intended goal: confirm whether Monaco has a safe WeatherDownload daily path, either through an official national machine-readable source or through a conservative NOAA GHCND fallback.

## Outcome

Gate 0 failed. No `MC` daily provider was implemented.

## Official sources checked

- Monaco government weather notice page referencing operational weather information:
  - `https://en.gouv.mc/News/Meteo-vigilance-orange`
- Monaco government environmental/climate publications:
  - `https://en.gouv.mc/content/download/449108/5095930/file/Recueil%20de%20donn%C3%A9es2018.pdf`
  - `https://en.gouv.mc/content/download/434120/4921345/file/Environment%20booklet%202017.pdf`
- Official NOAA GHCND metadata:
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt`
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt`
  - `https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt`

## What worked

- The current repository audit showed that `MC` is still a true daily coverage gap with status `not_attempted`.
- The official NOAA GHCND country list was reachable and confirmed that `MC` is already assigned to `Macau S.A.R`, not Monaco.
- Public sources indicate that Monaco government weather communication relies on Météo-France forecasts and government climate publications, which is useful context for future research.

## Why Gate 0 failed

- A simple GHCND fallback is not safe here because the official NOAA GHCND prefix `MC` belongs to `Macau S.A.R`, not Monaco.
- I did not find an official Monaco-specific GHCND country prefix or an accepted mapped-prefix convention in the current project patterns for Monaco.
- The official Monaco government sources checked were notices and publications, not a public machine-readable historical daily station-data contract.
- The Monaco government pages I probed were also not reliably script-friendly during this audit, timing out on direct requests from a clean script, which further weakens implementation confidence.
- Météo-France does publish weather information for Monaco, but in this audit I did not establish a Monaco-specific official public machine-readable historical daily station dataset suitable for WeatherDownload under the repository’s conservative policy.

## Current repository status

- `MC` remains without a daily provider.
- No fallback was added.

## Recommended future action

- Revisit only if Monaco government sources or a clearly accepted official Monaco meteorological data service publish a machine-readable historical daily station archive with stable station identifiers, variables, units, missing-value semantics, and date-interval access.
- Do not map Monaco to the NOAA `MC` GHCND prefix, because that prefix is officially used for Macau.
- Do not add Monaco coverage from government PDFs, weather notices, or frontend forecast pages alone.

## Provider decision

No provider was implemented in this audit.
