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

- [Meteo.ad Andorra](ad_meteo_ad.md)
- [Albania IGJEUM Daily Research Note](al_igjeum_research.md)
- [GeoSphere Austria](at_geosphere.md)
- [Bosnia and Herzegovina Meteo BiH Daily Research Note](ba_meteo_bih_research.md)
- [RMI/KMI Belgium](be_rmi.md)
- [Belarus Belhydromet Daily Research Note](by_belhydromet_research.md)
- [NIMH Bulgaria](bg_nimh.md)
- [CHMI Czech Republic](cz_chmi.md)
- [ECCC GeoMet Canada](ca_eccc.md)
- [MeteoSwiss Switzerland](ch_meteoswiss.md)
- [Cyprus Meteo.cy Daily Research Note](cy_meteo_cy_research.md)
- [DWD Germany](de_dwd.md)
- [DMI Denmark](dk_dmi.md)
- [Ilmateenistus Estonia](ee_ilmateenistus.md)
- [AEMET Spain](es_aemet.md)
- [FMI Finland](fi_fmi.md)
- [Meteo-France France](fr_meteo_france.md)
- [Met Office Weather DataHub Great Britain](gb_metoffice_datahub.md)
- [Great Britain Met Office Daily Research Note](gb_metoffice_daily_research.md)
- [NOAA GHCN-Daily United Kingdom](gb_ghcnd.md)
- [Greece HNMS Research Note](gr_hnms_research.md)
- [HungaroMet Hungary](hu_hungaromet.md)
- [Met Eireann Ireland](ie_meteireann.md)
- [Vedur Iceland](is_vedur.md)
- [MeteoSwiss Liechtenstein](li_meteoswiss.md)
- [Meteo.lt Lithuania](lt_meteo_lt.md)
- [LVGMC Latvia](lv_lvgmc.md)
- [Montenegro ZHMS Daily Research Note](me_zhms_research.md)
- [North Macedonia UHMR Daily Research Note](mk_uhmr_research.md)
- [Moldova Meteo.md Research Note](md_meteo_md_research.md)
- [Malta Met Office Daily Research Note](mt_malta_metoffice_research.md)
- [Monaco Research Note](mc_monaco_research.md)
- [MeteoLux Luxembourg](lu_meteolux.md)
- [ASTA Luxembourg](lu_asta.md)
- [Luxembourg Historical Research Note](lu_meteolux_research.md)
- [Croatia DHMZ Research Note](hr_dhmz_research.md)
- [Italy ISPRA SCIA Research Note](it_ispra_scia_research.md)
- [Kosovo IHMK Research Note](kv_ihmk_research.md)
- [KNMI Netherlands](nl_knmi.md)
- [MET Norway Frost](no_frost.md)
- [IMGW-PIB Poland](pl_imgw.md)
- [IPMA Portugal](pt_ipma.md)
- [Portugal IPMA Research Note](pt_ipma_research.md)
- [ANM Romania](ro_anm.md)
- [Serbia RHMZ Daily Research Note](rs_rhmz_research.md)
- [ARSO Slovenia](si_arso.md)
- [San Marino Research Note](sm_san_marino_research.md)
- [SMHI Sweden](se_smhi.md)
- [SHMU Slovakia](sk_shmu.md)
- [Turkey MGM Daily Research Note](tr_mgm_research.md)
- [Ukraine Ukrhydromet Daily Research Note](ua_ukrhydromet_research.md)
- [Vatican City Research Note](va_vatican_research.md)

## Shared-source wrappers

- [NOAA GHCN-Daily United States](us_noaa_ghcnd.md)
- [NOAA GHCN-Daily Canada](ca_noaa_ghcnd.md)
- [NOAA GHCN-Daily Mexico](mx_noaa_ghcnd.md)
- [NOAA GHCN-Daily Direct-Prefix Wrappers](ghcnd_direct_prefix_wrappers.md)
- [NOAA GHCN-Daily Mapped-Prefix Wrappers](ghcnd_mapped_prefix_wrappers.md)
- [NOAA GHCN-Daily Czech Republic](cz_noaa_ghcnd.md)
- [NOAA GHCN-Daily Portugal](pt_noaa_ghcnd.md)

The shared GHCN implementation lives under `weatherdownload/providers/ghcnd/`, while country wrappers stay thin. The shared wrapper helper supports both direct-prefix and mapped-prefix wrappers. The direct-prefix note covers the current `AL`, `BG`, `CY`, `FI`, `GB`, `GR`, `HR`, `IS`, `IT`, `LV`, `MD`, `MK`, `MT`, `NL`, `NO`, `NZ`, `RO`, and `SI` wrappers. The mapped-prefix note covers the current `AT`, `BA`, `BY`, `CH`, `CZ`, `DE`, `DK`, `ME`, `PT`, `RS`, `SE`, `SK`, `TR`, and `UA` wrappers. Bulgaria now also has an official `BG / nimh / daily` note for the conservative NIMH open-data daily path, alongside the thin shared `BG / ghcnd / daily` fallback. Denmark now also has an official `DK / dmi / daily` note for the stable climateData stationValue daily provider, alongside the existing DMI subdaily paths and the thin shared `DK / ghcnd / daily` fallback. Estonia now also has an official `EE / ilmateenistus / daily` note alongside the shared `EE / ghcnd / daily` fallback. Finland now also has an official `FI / fmi / daily` note alongside the shared `FI / ghcnd / daily` fallback. Great Britain now also has a separate official `GB / metoffice_datahub / 1hour` note for the recent local hourly provider, alongside the thin shared `GB / ghcnd / daily` fallback. France now has its own national Meteo-France note plus the shared `FR / ghcnd / daily` path in the capability matrix. Iceland now also has an official `IS / vedur / daily` note alongside the thin shared `IS / ghcnd / daily` fallback. Latvia now also has an official `LV / lvgmc / 1hour` and `LV / lvgmc / daily` note alongside the thin shared `LV / ghcnd / daily` fallback. Lithuania now also has an official `LT / meteo_lt / daily` note alongside the thin shared `LT / ghcnd / daily` fallback. Albania, Cyprus, Malta, Moldova, and North Macedonia now use the same thin shared `AL / ghcnd / daily`, `CY / ghcnd / daily`, `MT / ghcnd / daily`, `MD / ghcnd / daily`, and `MK / ghcnd / daily` fallback pattern, explicitly as GHCN-only paths rather than national providers; in NOAA's GHCND country list the `MK` prefix is labeled "Macedonia". Bosnia and Herzegovina now uses the mapped shared `BA / ghcnd / daily` fallback with GHCN prefix `BK`; NOAA GHCND prefix `BA` means Bahrain and is not used for Bosnia and Herzegovina. Belarus now uses the mapped shared `BY / ghcnd / daily` fallback with GHCN prefix `BO`; NOAA GHCND prefix `BY` means Burundi and is not used for Belarus. Montenegro now uses the mapped shared `ME / ghcnd / daily` fallback with GHCN prefix `MJ`; direct GHCND prefix `ME` is not used for Montenegro. Serbia now uses the mapped shared `RS / ghcnd / daily` fallback with GHCN prefix `RI`; NOAA GHCND prefix `RS` means Russia and is not used for Serbia. Slovakia now uses the mapped shared `SK / ghcnd / daily` fallback with GHCN prefix `LO`; direct GHCND prefix `SK` is not used for Slovakia, and the separate `SK / recent / daily` SHMU path remains experimental and recent-only. Turkey now uses the mapped shared `TR / ghcnd / daily` fallback with GHCN prefix `TU`; direct GHCND prefix `TR` is not used for Turkey. Ukraine now uses the mapped shared `UA / ghcnd / daily` fallback with GHCN prefix `UP`; direct GHCND prefix `UA` is not used for Ukraine. Netherlands now also has an official `NL / knmi / daily` note alongside the thin shared `NL / ghcnd / daily` fallback. Norway now also has an official `NO / frost / daily` note alongside the shared `NO / ghcnd / daily` fallback. Bosnia and Herzegovina, Belarus, Czech Republic, Montenegro, Portugal, Serbia, Slovakia, Turkey, and Ukraine keep short country-specific mapped-prefix notes because their GHCN prefix mapping is worth making explicit. The U.S. note stays separate because `US / ghcnd / daily` is the only current GHCN wrapper that supports `open_water_evaporation`.
