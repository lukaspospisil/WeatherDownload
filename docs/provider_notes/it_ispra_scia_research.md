# Italy ISPRA SCIA Research Note

Audit date: `2026-06-07`

Target: `IT / ispra_scia / daily`

Intended goal: confirm whether SCIA exposes a stable official public machine-readable daily station-series workflow suitable for WeatherDownload.

## Outcome

Gate 0 failed. No `IT / ispra_scia / daily` provider was implemented.

## Official sources checked

- `https://scia.isprambiente.it/`
- `https://scia.isprambiente.it/dati-e-indicatori/`
- `https://scia.isprambiente.it/dati/`
- `https://scia.isprambiente.it/documentazione/`
- `https://scia.isprambiente.it/pagina-in-manutenzione/`
- `https://scia.isprambiente.it/wp-json/wp/v2/pages/601`
- `https://scia.isprambiente.it/wp-json/wp/v2/pages/220`
- `https://scia.isprambiente.it/wp-json/wp/v2/pages/20`
- `https://scia.isprambiente.it/wp-json/wp/v2/search`
- `http://193.206.192.214/serverstazioni/stazioni400.php`
- `http://193.206.192.214/servertsutm/serietemporali400.php`
- `http://193.206.192.214/serveranalisiutm/analisi400.php`
- `http://193.206.192.214/servermappedaily/mappedaily400.php`
- `https://193.206.192.214/serverstazioni/stazioni400.php`
- `https://193.206.192.214/servertsutm/serietemporali400.php`

## What worked

- SCIA is clearly an official ISPRA system.
- SCIA documentation describes outputs as climate indicators derived from observation networks.
- The older official `dati` page still links to legacy official ISPRA apps.
- The legacy station app is partially reachable over HTTPS and exposes a browser-oriented OpenLayers page with WFS-backed station layers.
- Machine-readable station metadata may exist behind the station app.

## Why Gate 0 failed

- The current `dati-e-indicatori` page routes daily station and daily time-series buttons to a maintenance page.
- The critical legacy time-series app returned HTTP `500` on direct scripted access.
- Legacy HTTP links from the official page returned `404`.
- Only the HTTPS station page showed content during the audit.
- No confirmed public reproducible daily CSV/JSON endpoint was found with clear date-interval parameters, variable semantics, missing-value rules, and stable scripted access.
- A reachable station-map page alone is not enough to implement `daily`.

## Current repository status

- `IT / ispra_scia / daily` is not implemented.
- `IT` should remain on `IT / ghcnd / daily` fallback.

## Recommended future action

- Revisit only if SCIA restores a live public daily station-series workflow with stable machine-readable downloads.
- If SCIA becomes usable later, document it as a climate-indicator source unless official evidence shows direct raw station observations.
- Do not add `IT / ispra_scia / daily` from the station-map page alone.

## Provider decision

No provider was implemented in this audit.
