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
- [AEMET Spain](es_aemet.md)
- [FMI Finland](fi_fmi.md)
- [Meteo-France France](fr_meteo_france.md)
- [Met Office Weather DataHub Great Britain](gb_metoffice_datahub.md)
- [NOAA GHCN-Daily United Kingdom](gb_ghcnd.md)
- [HungaroMet Hungary](hu_hungaromet.md)
- [Met Eireann Ireland](ie_meteireann.md)
- [MeteoLux Luxembourg](lu_meteolux.md)
- [ASTA Luxembourg](lu_asta.md)
- [Luxembourg Historical Research Note](lu_meteolux_research.md)
- [KNMI Netherlands](nl_knmi.md)
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

The shared GHCN implementation lives under `weatherdownload/providers/ghcnd/`, while country wrappers stay thin. The shared wrapper helper supports both direct-prefix and mapped-prefix wrappers. The direct-prefix note covers the current `BG`, `EE`, `FI`, `GB`, `GR`, `HR`, `IS`, `IT`, `LT`, `LV`, `NO`, `NZ`, `RO`, and `SI` wrappers. The mapped-prefix note covers the current `AT`, `CH`, `CZ`, `DE`, `DK`, `PT`, `SE`, and `SK` wrappers. Great Britain now also has a separate official `GB / metoffice_datahub / 1hour` note for the recent local hourly provider, alongside the thin shared `GB / ghcnd / daily` fallback. France now has its own national Meteo-France note plus the shared `FR / ghcnd / daily` path in the capability matrix. Czech Republic and Portugal also keep short country-specific notes because their GHCN prefix mapping is worth making explicit. The U.S. note stays separate because `US / ghcnd / daily` is the only current GHCN wrapper that supports `open_water_evaporation`.
