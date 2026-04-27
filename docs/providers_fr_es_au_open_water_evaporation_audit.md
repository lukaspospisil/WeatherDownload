# France, Spain, And Australia Open-Water Evaporation Audit

Investigation date: 2026-04-28

## Summary

This audit checked whether additional official providers could safely support the canonical daily element `open_water_evaporation`.

Semantic rule used throughout:

- `open_water_evaporation` must mean measured evaporation from an open water surface, evaporation pan, Class A pan, or evaporimeter
- PET, ET0, reference evaporation, modeled evaporation, and forecast evaporation are excluded

Result:

- `FR / Météo-France`: not supported
- `ES / AEMET`: unclear
- `AU / BoM`: semantically supported but provider work needed

No new provider was implemented from this audit pass.

## Audit Table

| Country | Candidate source | Classification | Decision |
| --- | --- | --- | --- |
| `FR` | Météo-France `Données climatologiques de base - quotidiennes` | `not_supported` | Do not implement |
| `ES` | AEMET OpenData daily climatologies | `unclear` | Do not implement |
| `AU` | BoM Daily Weather Observations / Climate Data Online | `semantically_supported_but_provider_work_needed` | Do not implement in this pass |

## France

Official source checked:

- dataset page: `https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-quotidiennes/`
- official field dictionary for the daily `autres-parametres` files:
  - `https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_descriptif_champs_autres-parametres.csv`
- official field dictionary for the daily `RR-T-Vent` files:
  - `https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_descriptif_champs_RR-T-Vent.csv`

Observed evidence:

- the daily source is official, public, machine-readable, and distributed as department/period `csv.gz`
- the official `autres-parametres` dictionary includes:
  - `ETPMON`: daily Penman-Monteith ETP
  - `ETPGRILLE`: daily Penman-Monteith ETP at the nearest grid point
- no clearly documented measured open-water, pan, or evaporimeter daily variable was verified in the official field dictionaries inspected for this source

Classification:

- `not_supported`

Reason:

- the verified evaporation-like variables are ETP products, not measured open-water or pan evaporation

## Spain

Official source checked:

- OpenData overview and access requirements:
  - `https://opendata.aemet.es/centrodedescargas/info`
- OpenData product catalog:
  - `https://opendata.aemet.es/centrodedescargas/productosAEMET`
- OpenData FAQs:
  - `https://opendata.aemet.es/centrodedescargas/docs/FAQs130917.pdf`

Observed evidence:

- AEMET OpenData is an official REST API
- access requires an API key
- the official product catalog exposes both:
  - `Climatologías diarias`
  - `Climatologías mensuales/anuales`
- the FAQs document the official daily climatology endpoint pattern:
  - `/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{fechaFinStr}/estacion/{idema}`
- this audit did not verify an official daily field dictionary proving that the daily climatology response includes a measured open-water, pan, or evaporimeter evaporation variable

Classification:

- `unclear`

Reason:

- the source family and daily endpoint are real, and WeatherDownload already has one environment-variable API-key pattern through KNMI
- however, without verified official daily field semantics for evaporation, implementing `open_water_evaporation` would be speculative
- hints that monthly or annual climatology products may expose evaporation totals were not treated as sufficient evidence for a daily measured variable

## Australia

Official source checked:

- Daily Weather Observations statistics definitions:
  - `https://www.bom.gov.au/climate/cdo/about/definitionsother.shtml`
- Water and the Land evaporation overview:
  - `https://www.bom.gov.au/watl/evaporation/`
- average evaporation methodology:
  - `https://www.bom.gov.au/climate/maps/averages/evaporation/about.shtml`

Observed evidence:

- BoM explicitly defines evaporation as measured by a `Class A evaporation pan`
- the public BoM evaporation pages consistently distinguish this from evapotranspiration
- the public climate pages also describe daily evaporation data existing in BoM's station archive inputs
- however, direct automated fetch attempts against BoM daily observation endpoints were blocked by the site with an anti-scraping message instead of yielding a stable machine-readable response suitable for a new provider implementation

Classification:

- `semantically_supported_but_provider_work_needed`

Reason:

- the meteorological meaning is a strong fit for `open_water_evaporation`
- the missing piece is a clean, automation-friendly official delivery contract that fits the current WeatherDownload provider architecture without fragile scraping

## Recommendation

Keep `open_water_evaporation` support limited to:

- `CZ / CHMI / historical_csv / daily` via raw `VY`
- `SK / SHMU / recent / daily` via raw `voda_vypar`

Possible next steps for a future audit:

- `FR`: no follow-up unless a separate official measured pan/open-water variable is found
- `ES`: inspect authenticated OpenData schema details only after deciding on an AEMET API-key configuration contract
- `AU`: investigate whether BoM provides a stable official download service, bulk file endpoint, or documented API that avoids HTML scraping and anti-bot blocks
