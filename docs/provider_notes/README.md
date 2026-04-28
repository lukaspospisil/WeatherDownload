# Provider Notes

<p align="right">
  <img src="../images/logo.svg" alt="WeatherDownload logo" width="180">
</p>

These notes contain source-specific details that do not belong in the conceptual provider model or the generated capability matrix.

Use these pages for:

- official source URLs
- provider identifiers
- station ID conventions
- raw-to-canonical mappings
- source-specific units and conversions
- inventory-driven availability details
- caveats and limitations

Start here for the broader documentation structure:

- conceptual model: [Provider Model](../providers.md)
- current capability matrix: [Supported Capabilities](../supported_capabilities.md)
- canonical element semantics: [Canonical Elements](../canonical_elements.md)
- output columns: [Normalized Output Schemas](../output_schema.md)

## National Providers

- [GeoSphere Austria](at_geosphere.md)
- [RMI/KMI Belgium](be_rmi.md)
- [MeteoSwiss Switzerland](ch_meteoswiss.md)
- [DMI Denmark](dk_dmi.md)
- [HungaroMet Hungary](hu_hungaromet.md)
- [KNMI Netherlands](nl_knmi.md)
- [IMGW-PIB Poland](pl_imgw.md)
- [SMHI Sweden](se_smhi.md)
- [SHMU Slovakia](sk_shmu.md)

## Shared GHCN-Daily Notes

- [NOAA GHCN-Daily United States](us_noaa_ghcnd.md)
- [NOAA GHCN-Daily Canada](ca_noaa_ghcnd.md)
- [NOAA GHCN-Daily Mexico](mx_noaa_ghcnd.md)
- [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md)

The shared GHCN implementation lives under `weatherdownload/providers/ghcnd/`, while country wrappers stay thin. The direct-prefix note covers the current `FI`, `FR`, `IT`, `NO`, and `NZ` wrappers. The U.S. note stays separate because `US / ghcnd / daily` is the only current GHCN wrapper that supports `open_water_evaporation`.

