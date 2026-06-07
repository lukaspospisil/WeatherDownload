# Croatia DHMZ Research Note

Audit date: `2026-06-07`

Target: `HR / dhmz / daily`

Intended goal: confirm whether DHMZ exposes a stable official public machine-readable historical daily station source suitable for WeatherDownload.

## Outcome

Gate 0 failed. No `HR / dhmz / daily` provider was implemented.

## Official sources checked

- `https://meteo.hr/`
- `https://meteo.hr/podaci_e.php`
- `https://meteo.hr/klima_e.php`
- `https://meteo.hr/podaci_e.php?section=podaci_vrijeme&param=hrvatska_e`
- `https://meteo.hr/klima_e.php?section=klima_podaci&param=k2_1`
- `https://meteopodaci.dhz.hr/`
- `https://meteopodaci.dhz.hr/rest/postaja/slojeviPostaje`
- `https://meteopodaci.dhz.hr/rest/postaja/najblizePostajeFc`
- `POST https://meteopodaci.dhz.hr/rest/storeManager/fetch/Postaja`
- `POST https://meteopodaci.dhz.hr/rest/storeManager/fetch/MeteoElement`
- `POST https://meteopodaci.dhz.hr/rest/storeManager/fetch/MeteoMjerniElement`
- `POST https://meteopodaci.dhz.hr/rest/storeManager/fetch/ZahtjevStrojnaIsporuka`
- `https://meteopodaci.dhz.hr/wfs?service=WFS&request=GetCapabilities`
- `https://meteopodaci.dhz.hr/wfs?service=WFS&version=1.0.0&request=DescribeFeatureType&typeName=amp:standardna_mjerenja`
- `https://meteopodaci.dhz.hr/wfs?service=WFS&version=1.0.0&request=DescribeFeatureType&typeName=skmp:standardna_mjerenja`
- public WFS feature fetches for `ostalo:postaje`, `ostalo:postaje_dhmz`, `amp:standardna_mjerenja`, and `skmp:standardna_mjerenja`

## What worked

- DHMZ exposes public machine-readable station metadata.
- DHMZ exposes public machine-readable element metadata.
- The official portal exposes public WFS/WMS capabilities and station layers.
- The WFS schemas for measurement layers include timestamp/value-style fields.

## Why Gate 0 failed

- The `meteo.hr` climate/data pages looked like HTML tables and year selectors, not a stable machine-readable daily export.
- The portal REST surface exposed metadata and request/order workflow endpoints, including machine-delivery request paths, rather than a clear open historical daily feed.
- No confirmed public historical daily station observations endpoint or downloadable daily CSV/JSON/XLS contract was found.
- The sampled public WFS measurement features in `amp:standardna_mjerenja` and `skmp:standardna_mjerenja` were null-ish and did not demonstrate usable historical daily station data.

## Current repository status

- `HR / dhmz / daily` is not implemented.
- `HR` should remain on `HR / ghcnd / daily` fallback.

## Recommended future action

- Revisit only if DHMZ publishes a clearly documented public historical daily station observations endpoint or downloadable archive.
- Treat the existing metadata and WFS station layers as promising discovery evidence, but not enough for implementation by themselves.
- Do not add `HR / dhmz / daily` until a reproducible daily data contract is confirmed.

## Provider decision

No provider was implemented in this audit.
