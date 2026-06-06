# Provider Notes

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

These notes hold source-specific details that do not belong in the conceptual provider model or the generated capability matrix. Use them for source URLs, provider identifiers, station ID conventions, raw-to-canonical mappings, units, and caveats.

For the broader documentation structure:

- conceptual model: [Provider Model](../providers.md)
- current capability matrix: [Supported Capabilities](../supported_capabilities.md)
- canonical element semantics: [Canonical Elements](../canonical_elements.md)
- output columns: [Normalized Output Schemas](../output_schema.md)

The capability matrix is the authoritative current overview of `country + provider + resolution + element`. The notes below are intentionally narrower and focus on source behavior.

Canonical country codes follow the registry output. Public alias inputs can still normalize to the canonical code, for example `UK -> GB`.

## National providers

- [GeoSphere Austria](at_geosphere.md)
- [RMI/KMI Belgium](be_rmi.md)
- [CHMI Czech Republic](cz_chmi.md)
- [ECCC GeoMet Canada](ca_eccc.md)
- [MeteoSwiss Switzerland](ch_meteoswiss.md)
- [DMI Denmark](dk_dmi.md)
- [Ilmateenistus Estonia](ee_ilmateenistus.md)
- [AEMET Spain](es_aemet.md)
- [FMI Finland](fi_fmi.md)
- [Meteo-France France](fr_meteo_france.md)
- [Met Office Weather DataHub Great Britain](gb_metoffice_datahub.md)
- [NOAA GHCN-Daily United Kingdom](gb_ghcnd.md)
- [HungaroMet Hungary](hu_hungaromet.md)
- [Met Eireann Ireland](ie_meteireann.md)
- [Meteo.lt Lithuania](lt_meteo_lt.md)
- [LVGMC Latvia](lv_lvgmc.md)
- [MeteoLux Luxembourg](lu_meteolux.md)
- [ASTA Luxembourg](lu_asta.md)
- [Luxembourg Historical Research Note](lu_meteolux_research.md)
- [KNMI Netherlands](nl_knmi.md)
- [MET Norway Frost](no_frost.md)
- [IMGW-PIB Poland](pl_imgw.md)
- [IPMA Portugal](pt_ipma.md)
- [ANM Romania](ro_anm.md)
- [ARSO Slovenia](si_arso.md)
- [SMHI Sweden](se_smhi.md)
- [SHMU Slovakia](sk_shmu.md)

## Shared-source wrappers

- [NOAA GHCN-Daily United States](us_noaa_ghcnd.md)
- [NOAA GHCN-Daily Canada](ca_noaa_ghcnd.md)
- [NOAA GHCN-Daily Mexico](mx_noaa_ghcnd.md)
- [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md)
- [NOAA GHCN-Daily Mapped-Prefix Wrappers](ghcnd_mapped_prefix_wrappers.md)
- [NOAA GHCN-Daily Czech Republic](cz_noaa_ghcnd.md)
- [NOAA GHCN-Daily Portugal](pt_noaa_ghcnd.md)

The shared GHCN implementation lives under `weatherdownload/providers/ghcnd/`, while country wrappers stay thin. The shared wrapper helper supports both direct-prefix and mapped-prefix wrappers. The direct-prefix note covers the current `AL`, `BG`, `CY`, `FI`, `GB`, `GR`, `HR`, `IS`, `IT`, `LV`, `MD`, `MK`, `MT`, `NL`, `NO`, `NZ`, `RO`, and `SI` wrappers. The mapped-prefix note covers the current `AT`, `CH`, `CZ`, `DE`, `DK`, `ME`, `PT`, `RS`, `SE`, and `SK` wrappers. Denmark now also has an official `DK / dmi / daily` note for the stable climateData stationValue daily provider, alongside the existing DMI subdaily paths and the thin shared `DK / ghcnd / daily` fallback. Estonia now also has an official `EE / ilmateenistus / daily` note alongside the shared `EE / ghcnd / daily` fallback. Great Britain now also has a separate official `GB / metoffice_datahub / 1hour` note for the recent local hourly provider, alongside the thin shared `GB / ghcnd / daily` fallback. France now has its own national Meteo-France note plus the shared `FR / ghcnd / daily` path in the capability matrix. Latvia now also has an official `LV / lvgmc / daily` note alongside the thin shared `LV / ghcnd / daily` fallback. Lithuania now also has an official `LT / meteo_lt / daily` note alongside the thin shared `LT / ghcnd / daily` fallback. Albania, Cyprus, Malta, Moldova, and North Macedonia now use the same thin shared `AL / ghcnd / daily`, `CY / ghcnd / daily`, `MT / ghcnd / daily`, `MD / ghcnd / daily`, and `MK / ghcnd / daily` fallback pattern, explicitly as GHCN-only paths rather than national providers; in NOAA's GHCND country list the `MK` prefix is labeled "Macedonia". Montenegro now uses the mapped shared `ME / ghcnd / daily` fallback with GHCN prefix `MJ`; direct GHCND prefix `ME` is not used for Montenegro. Serbia now uses the mapped shared `RS / ghcnd / daily` fallback with GHCN prefix `RI`; NOAA GHCND prefix `RS` means Russia and is not used for Serbia. Netherlands now also has an official `NL / knmi / daily` note alongside the thin shared `NL / ghcnd / daily` fallback. Norway now also has an official `NO / frost / daily` note alongside the shared `NO / ghcnd / daily` fallback. Czech Republic, Montenegro, Portugal, and Serbia keep short country-specific mapped-prefix notes because their GHCN prefix mapping is worth making explicit. The U.S. note stays separate because `US / ghcnd / daily` is the only current GHCN wrapper that supports `open_water_evaporation`.
