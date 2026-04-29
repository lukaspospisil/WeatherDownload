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

## National providers

- [GeoSphere Austria](at_geosphere.md)
- [RMI/KMI Belgium](be_rmi.md)
- [CHMI Czech Republic](cz_chmi.md)
- [MeteoSwiss Switzerland](ch_meteoswiss.md)
- [DMI Denmark](dk_dmi.md)
- [Meteo-France France](fr_meteo_france.md)
- [HungaroMet Hungary](hu_hungaromet.md)
- [KNMI Netherlands](nl_knmi.md)
- [IMGW-PIB Poland](pl_imgw.md)
- [SMHI Sweden](se_smhi.md)
- [SHMU Slovakia](sk_shmu.md)

## Shared-source wrappers

- [NOAA GHCN-Daily United States](us_noaa_ghcnd.md)
- [NOAA GHCN-Daily Canada](ca_noaa_ghcnd.md)
- [NOAA GHCN-Daily Mexico](mx_noaa_ghcnd.md)
- [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md)
- [NOAA GHCN-Daily Mapped-Prefix Wrappers](ghcnd_mapped_prefix_wrappers.md)
- [NOAA GHCN-Daily Czech Republic](cz_noaa_ghcnd.md)

The shared GHCN implementation lives under `weatherdownload/providers/ghcnd/`, while country wrappers stay thin. The shared wrapper helper supports both direct-prefix and mapped-prefix wrappers. The direct-prefix note covers the current `FI`, `IT`, `NO`, and `NZ` wrappers. The mapped-prefix note covers the current `AT`, `CH`, `CZ`, `DE`, `DK`, `SE`, and `SK` wrappers. France now has its own national Meteo-France note plus the shared `FR / ghcnd / daily` path in the capability matrix. Czech Republic also keeps a short country-specific note because it sits next to the separate CHMI provider family. The U.S. note stays separate because `US / ghcnd / daily` is the only current GHCN wrapper that supports `open_water_evaporation`.
