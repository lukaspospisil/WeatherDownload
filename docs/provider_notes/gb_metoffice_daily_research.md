# Great Britain Met Office Daily Research Note

Audit date: 2026-06-07

Target:
- country: `GB`
- provider: `metoffice`
- resolution: `daily`

Current repository status:
- `GB / ghcnd / daily` is implemented as the current daily fallback
- `GB / metoffice_datahub / 1hour` is implemented as the official recent hourly path
- no official Met Office daily provider was implemented from this audit

## Official sources checked

- Met Office Weather DataHub home:
  - `https://datahub.metoffice.gov.uk/`
- Met Office Weather DataHub observations overview:
  - `https://datahub.metoffice.gov.uk/docs/g/category/observations/overview`
- Met Office Weather DataHub support / API key FAQ:
  - `https://datahub.metoffice.gov.uk/support/faqs`
- CEDA MIDAS Open user guide:
  - `https://help.ceda.ac.uk/article/4982-midas-open-user-guide`
- CEDA MIDAS quick start guide:
  - `https://help.ceda.ac.uk/article/94-midas`
- Met Office archive daily registers page:
  - `https://www.metoffice.gov.uk/research/library-and-archive/archive/daily-registers`

## What worked

- The official Met Office Weather DataHub documentation is reachable and clearly documents the current Land Observations API.
- The official DataHub observations overview is explicit that the current Land Observations API is:
  - hourly
  - JSON
  - recent only
  - limited to the past 48 hours
- The official DataHub support documentation is explicit that API access requires an API key obtained after registering and subscribing to a product.
- Official CEDA documentation confirms that MIDAS Open exists as machine-readable historical station data with station metadata CSV files and yearly flat files.
- Official Met Office archive pages confirm that historical daily observation material exists in archive/register form.

## Why Gate 0 failed

Gate 0 failed for `GB / metoffice / daily`.

Reasons:
- The official Met Office Weather DataHub observations API is not public no-login/no-key access. It requires account registration, product subscription, and an API key.
- The official DataHub observations product is not a historical daily station API. The current documented observations scope is hourly land observations for the past 48 hours.
- Official site-specific daily products on DataHub are forecast products, not observed historical daily station observations.
- MIDAS Open is promising historically, but the official CEDA access model is still not a simple public no-login API path suitable for this repository’s conservative Gate 0 rule. Official CEDA guidance also states that users need to register as CEDA users, and the broader MIDAS collection requires additional access application.
- The Met Office archive daily registers page is official, but it is an archive/library access route rather than a stable machine-readable daily station-data contract suitable for WeatherDownload tests.

## Recommended future action

- Keep `GB / ghcnd / daily` as the current daily fallback.
- Keep `GB / metoffice_datahub / 1hour` as the official recent hourly provider.
- Revisit only if the Met Office publishes one of the following under an official public no-login/no-key contract:
  - a true historical daily station observations API
  - a stable open CSV/JSON/XLS daily station archive
  - a public official MIDAS Open endpoint that does not require CEDA account-based access

No `GB / metoffice / daily` provider was implemented from this audit.
