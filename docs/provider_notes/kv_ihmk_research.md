# Kosovo IHMK Research Note

This is a provider research note. It records an audit pass for possible `KV / ihmk / daily` or `KV / khmi / daily` coverage and explains why no Kosovo national daily provider is implemented yet.

## Outcome

No stable public machine-readable national daily provider was confirmed during this audit.

`KV` remains without implemented national daily coverage.

`KV` also remains rejected as a GHCND fallback target:

- Kosovo has no separate GHCND country prefix
- a previous coordinate-based audit against live NOAA GHCND metadata found `0` live GHCND station coordinates inside the Kosovo polygon
- `KV` must not map to `RI`, because `RI` is Serbia in GHCND, not Kosovo

## Official institutional source

The official institutional source is the Kosovo Hydrometeorological Institute:

- KHMI / IHMK within the Kosovo Environmental Protection Agency (KEPA / AMMK)
- institutional page: `https://www.ammk-rks.net/en/drejtorite/39/detyrat-dhe-prgjegjsit`
- agency overview: `https://www.ammk-rks.net/en/per-ne`
- contact evidence: `https://ammk-rks.net/en/kontakti`

The official pages describe KHMI/IHMK as the institute that performs hydrometeorological measurements in Kosovo, maintains meteorological and hydrological station networks, and studies, stores, exchanges, and publishes hydrometeorological data.

## Public sources inspected

### KHMI / KEPA / AMMK pages

- `https://www.ammk-rks.net/en/drejtorite/39/detyrat-dhe-prgjegjsit`
- `https://www.ammk-rks.net/en/per-ne`
- `https://www.ammk-rks.net/en/publikime/25/p4`
- `https://www.ammk-rks.net/en/shto-publikime-tjera`
- `https://www.ammk-rks.net/en/drejtorite/41/meteorologjia`

### KHMI publications

- `https://www.ammk-rks.net/assets/cms/uploads/files/Kosovo%20Hydrometeorological%20YearBook%202024.pdf`
- `https://www.ammk-rks.net/assets/cms/uploads/files/Publikime-raporte/Vjetari_Hidrometeorologjik_-_2014_-_Eng_-_Web_New.pdf`

The yearbooks show that IHMK/KHMI has a real internal meteorology/climatology/hydrology database and that annual publications are generated from it. They also show that the institute tracks daily and monthly values, station networks, temperature, precipitation, sunshine duration, relative humidity, and other meteorological fields.

That is useful institutional evidence, but the yearbooks are PDF publications rather than a stable public daily data interface.

### Air Quality Portal

- portal overview: `https://airqualitykosova.rks-gov.net/en/about-air-quality-portal/`
- user guide PDF: `https://airqualitykosova.rks-gov.net/wp-content/uploads/2020/11/user-guide.pdf`

Supporting evidence from the official portal and user guide:

- the portal exposes monitoring-station data and says downloads are available as Excel or CSV
- the portal is focused on air-quality monitoring and forecast products
- the user guide describes validated pollutant measurements and forecast/model products, not a documented national meteorological daily archive
- search snippets mention "basic weather conditions" alongside pollutant displays, but no stable public station-level meteorological schema, station metadata contract, or documented daily observation archive was confirmed during this audit

When opened during this audit, the main Air Quality Portal pages redirected to `https://cons.rks-gov.net`, which is another stability concern for use as a WeatherDownload provider base.

## Why no provider was implemented

No `KV / ihmk / daily` provider was added because the audit did not confirm all of the conditions needed for a stable WeatherDownload implementation:

- no public official daily meteorological API was found
- no public official JSON or XML station-observation endpoint was confirmed
- no stable official CSV or XLS/XLSX daily station archive for meteorological observations was confirmed
- no durable public station metadata contract for a meteorological daily feed was confirmed
- the clearest public meteorological sources are PDF yearbooks and similar publication outputs
- the air-quality portal is an institutional source, but it is scoped to air-quality monitoring/forecast products and did not provide a clearly documented, stable national daily meteorology interface during this audit

Because of those gaps, implementing from public PDFs or unstable portal behavior would violate the repository rule against adding scraped PDF providers without explicit approval.

## Provider decision

Not implemented:

- `KV / ihmk / daily`
- `KV / khmi / daily`
- `KV / ghcnd / daily`

Current repository meaning should remain:

- `KV` has no stable public machine-readable national daily provider confirmed yet
- `KV` stays `not_attempted` in the current coverage classification rather than being promoted to `national_daily` or `ghcnd_daily`

## Future work

Possible next steps, if Kosovo coverage is revisited:

- contact KHMI/IHMK directly and ask for an official public station-level CSV/API/XLS archive
- look for an official restored/open-data endpoint behind KEPA/AMMK or the Kosovo government domain
- re-audit the air-quality portal only if a stable downloadable station-data interface becomes publicly available again
- re-check whether an official meteorology portal publishes station metadata and observed subdaily data that can be aggregated to daily values without scraping PDFs
