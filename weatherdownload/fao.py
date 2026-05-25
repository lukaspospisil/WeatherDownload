from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd


FAO56_DERIVED_COLUMNS = [
    "es",
    "vpd",
    "delta",
    "pressure",
    "gamma",
    "Ra",
    "N",
    "Rs",
    "Rso",
    "Rns",
    "Rnl",
    "Rn",
    "G",
    "E_FAO",
]
FAO56_DERIVED_UNITS = {
    "es": "kPa",
    "vpd": "kPa",
    "delta": "kPa degC^-1",
    "pressure": "kPa",
    "gamma": "kPa degC^-1",
    "Ra": "MJ m^-2 day^-1",
    "N": "h day^-1",
    "Rs": "MJ m^-2 day^-1",
    "Rso": "MJ m^-2 day^-1",
    "Rns": "MJ m^-2 day^-1",
    "Rnl": "MJ m^-2 day^-1",
    "Rn": "MJ m^-2 day^-1",
    "G": "MJ m^-2 day^-1",
    "E_FAO": "mm day^-1",
}
RS_RSO_RATIO_CLAMP = (0.0, 1.0)
SOLAR_CONSTANT_MJ_M2_MIN = 0.0820
STEFAN_BOLTZMANN_CONSTANT_MJ_K4_M2_DAY = 4.903e-9


def saturation_vapour_pressure_kpa(temperature_c):
    return 0.6108 * np.exp((17.27 * temperature_c) / (temperature_c + 237.3))


def mean_saturation_vapour_pressure_kpa(tas_min_c, tas_max_c):
    return (saturation_vapour_pressure_kpa(tas_min_c) + saturation_vapour_pressure_kpa(tas_max_c)) / 2.0


def slope_saturation_vapour_pressure_curve_kpa_per_c(tas_mean_c):
    return 4098.0 * saturation_vapour_pressure_kpa(tas_mean_c) / np.power(tas_mean_c + 237.3, 2)


def atmospheric_pressure_from_elevation_kpa(elevation_m):
    return 101.3 * np.power((293.0 - 0.0065 * elevation_m) / 293.0, 5.26)


def psychrometric_constant_kpa_per_c(pressure_kpa):
    return 0.000665 * pressure_kpa


def wind_speed_2m(wind_speed_z_m_s, measurement_height_m: float = 10.0):
    return wind_speed_z_m_s * 4.87 / np.log(67.8 * measurement_height_m - 5.42)


def extraterrestrial_radiation_mj_m2_day(latitude_rad, day_of_year):
    inverse_relative_distance = 1.0 + 0.033 * np.cos((2.0 * np.pi / 365.0) * day_of_year)
    solar_declination = 0.409 * np.sin((2.0 * np.pi / 365.0) * day_of_year - 1.39)
    sunset_hour_angle = _sunset_hour_angle(latitude_rad, day_of_year)
    return (
        (24.0 * 60.0 / np.pi)
        * SOLAR_CONSTANT_MJ_M2_MIN
        * inverse_relative_distance
        * (
            sunset_hour_angle * np.sin(latitude_rad) * np.sin(solar_declination)
            + np.cos(latitude_rad) * np.cos(solar_declination) * np.sin(sunset_hour_angle)
        )
    )


def daylight_hours(latitude_rad, day_of_year):
    sunset_hour_angle = _sunset_hour_angle(latitude_rad, day_of_year)
    return (24.0 / np.pi) * sunset_hour_angle


def solar_radiation_from_sunshine_mj_m2_day(sunshine_hours, daylight_hours, extraterrestrial_radiation):
    n_over_n = sunshine_hours / daylight_hours
    return (0.25 + 0.50 * n_over_n) * extraterrestrial_radiation


def clear_sky_radiation_mj_m2_day(elevation_m, extraterrestrial_radiation):
    return (0.75 + 2e-5 * elevation_m) * extraterrestrial_radiation


def net_shortwave_radiation_mj_m2_day(solar_radiation, albedo: float = 0.23):
    return (1.0 - albedo) * solar_radiation


def net_longwave_radiation_mj_m2_day(
    tas_min_c,
    tas_max_c,
    actual_vapour_pressure_kpa,
    solar_radiation,
    clear_sky_radiation,
):
    tas_max_k = tas_max_c + 273.16
    tas_min_k = tas_min_c + 273.16
    rs_rso_ratio = solar_radiation / clear_sky_radiation
    rs_rso_ratio = np.clip(rs_rso_ratio, RS_RSO_RATIO_CLAMP[0], RS_RSO_RATIO_CLAMP[1])
    return (
        STEFAN_BOLTZMANN_CONSTANT_MJ_K4_M2_DAY
        * ((np.power(tas_max_k, 4) + np.power(tas_min_k, 4)) / 2.0)
        * (0.34 - 0.14 * np.sqrt(np.clip(actual_vapour_pressure_kpa, 0.0, None)))
        * (1.35 * rs_rso_ratio - 0.35)
    )


def fao56_pm_et0_daily(
    *,
    delta_kpa_per_c,
    net_radiation_mj_m2_day,
    soil_heat_flux_mj_m2_day,
    gamma_kpa_per_c,
    tas_mean_c,
    wind_speed_2m_m_s,
    vapour_pressure_deficit_kpa,
):
    numerator = (
        0.408 * delta_kpa_per_c * (net_radiation_mj_m2_day - soil_heat_flux_mj_m2_day)
        + gamma_kpa_per_c * (900.0 / (tas_mean_c + 273.0)) * wind_speed_2m_m_s * vapour_pressure_deficit_kpa
    )
    denominator = delta_kpa_per_c + gamma_kpa_per_c * (1.0 + 0.34 * wind_speed_2m_m_s)
    return numerator / denominator


def compute_fao56_daily_from_wide(
    daily_wide: pd.DataFrame,
    stations: pd.DataFrame,
    *,
    wind_measurement_height_m: float = 10.0,
    use_observed_pressure: bool = False,
) -> pd.DataFrame:
    if daily_wide.empty:
        result = daily_wide.copy()
        for column in _fao_daily_output_columns():
            result[column] = pd.Series(dtype="float64")
        return result

    required_wide_columns = [
        "station_id",
        "date",
        "tas_mean",
        "tas_max",
        "tas_min",
        "wind_speed",
        "vapour_pressure",
        "sunshine_duration",
    ]
    missing_wide_columns = [column for column in required_wide_columns if column not in daily_wide.columns]
    if missing_wide_columns:
        raise KeyError(f"compute_fao56_daily_from_wide is missing required daily columns: {missing_wide_columns}")

    normalized_stations = _normalize_station_columns(stations)
    required_station_columns = ["station_id", "latitude", "elevation_m"]
    missing_station_columns = [column for column in required_station_columns if column not in normalized_stations.columns]
    if missing_station_columns:
        raise KeyError(f"compute_fao56_daily_from_wide is missing required station columns: {missing_station_columns}")

    result = daily_wide.copy()
    station_columns = [column for column in normalized_stations.columns if column not in result.columns or column == "station_id"]
    merged = result.merge(normalized_stations.loc[:, station_columns], on="station_id", how="left", validate="m:1")

    dates = pd.to_datetime(merged["date"], errors="coerce")
    doy = dates.dt.dayofyear.astype("float64")
    latitude_deg = pd.to_numeric(merged["latitude"], errors="coerce")
    latitude_rad = np.deg2rad(latitude_deg)
    elevation_m = pd.to_numeric(merged["elevation_m"], errors="coerce")

    tas_mean_c = pd.to_numeric(merged["tas_mean"], errors="coerce")
    tas_max_c = pd.to_numeric(merged["tas_max"], errors="coerce")
    tas_min_c = pd.to_numeric(merged["tas_min"], errors="coerce")
    wind_speed_z_m_s = pd.to_numeric(merged["wind_speed"], errors="coerce")
    sunshine_hours = pd.to_numeric(merged["sunshine_duration"], errors="coerce")
    vapour_pressure_hpa = pd.to_numeric(merged["vapour_pressure"], errors="coerce")
    ea_kpa = vapour_pressure_hpa / 10.0

    pressure_fao_kpa = atmospheric_pressure_from_elevation_kpa(elevation_m)
    pressure_for_gamma_kpa = pressure_fao_kpa
    if use_observed_pressure and "pressure_observed" in merged.columns:
        pressure_observed_kpa = pd.to_numeric(merged["pressure_observed"], errors="coerce") / 10.0
        pressure_for_gamma_kpa = pressure_observed_kpa.where(pressure_observed_kpa.notna(), pressure_fao_kpa)

    es_kpa = mean_saturation_vapour_pressure_kpa(tas_min_c, tas_max_c)
    vpd_raw_kpa = es_kpa - ea_kpa
    vpd_kpa = pd.Series(vpd_raw_kpa, index=merged.index, dtype="float64").clip(lower=0.0)
    delta_kpa_per_c = slope_saturation_vapour_pressure_curve_kpa_per_c(tas_mean_c)
    gamma_kpa_per_c = psychrometric_constant_kpa_per_c(pressure_for_gamma_kpa)
    ra_mj_m2_day = extraterrestrial_radiation_mj_m2_day(latitude_rad, doy)
    n_h = daylight_hours(latitude_rad, doy)
    rs_mj_m2_day = solar_radiation_from_sunshine_mj_m2_day(sunshine_hours, n_h, ra_mj_m2_day)
    rso_mj_m2_day = clear_sky_radiation_mj_m2_day(elevation_m, ra_mj_m2_day)
    rs_mj_m2_day = pd.Series(rs_mj_m2_day, index=merged.index, dtype="float64").clip(lower=0.0)
    valid_rso = pd.Series(rso_mj_m2_day, index=merged.index, dtype="float64").gt(0.0)
    rs_mj_m2_day.loc[valid_rso] = np.minimum(
        rs_mj_m2_day.loc[valid_rso],
        pd.Series(rso_mj_m2_day, index=merged.index, dtype="float64").loc[valid_rso],
    )
    rso_mj_m2_day = pd.Series(rso_mj_m2_day, index=merged.index, dtype="float64")
    rns_mj_m2_day = net_shortwave_radiation_mj_m2_day(rs_mj_m2_day)

    rnl_mj_m2_day = pd.Series(np.nan, index=merged.index, dtype="float64")
    valid_longwave = rso_mj_m2_day.gt(0.0)
    rnl_mj_m2_day.loc[valid_longwave] = net_longwave_radiation_mj_m2_day(
        tas_min_c.loc[valid_longwave],
        tas_max_c.loc[valid_longwave],
        ea_kpa.loc[valid_longwave],
        rs_mj_m2_day.loc[valid_longwave],
        rso_mj_m2_day.loc[valid_longwave],
    )
    rn_mj_m2_day = rns_mj_m2_day - rnl_mj_m2_day
    u2_m_s = wind_speed_2m(wind_speed_z_m_s, measurement_height_m=wind_measurement_height_m)

    soil_heat_flux_mj_m2_day = pd.Series(0.0, index=merged.index, dtype="float64")
    e_fao = pd.Series(
        fao56_pm_et0_daily(
            delta_kpa_per_c=delta_kpa_per_c,
            net_radiation_mj_m2_day=rn_mj_m2_day,
            soil_heat_flux_mj_m2_day=soil_heat_flux_mj_m2_day,
            gamma_kpa_per_c=gamma_kpa_per_c,
            tas_mean_c=tas_mean_c,
            wind_speed_2m_m_s=u2_m_s,
            vapour_pressure_deficit_kpa=vpd_kpa,
        ),
        index=merged.index,
        dtype="float64",
    )

    computed = result.copy()
    computed["doy"] = doy
    computed["ea_kpa"] = pd.Series(ea_kpa, index=merged.index, dtype="float64")
    computed["es_kpa"] = pd.Series(es_kpa, index=merged.index, dtype="float64")
    computed["vpd_raw_kpa"] = pd.Series(vpd_raw_kpa, index=merged.index, dtype="float64")
    computed["vpd_kpa"] = pd.Series(vpd_kpa, index=merged.index, dtype="float64")
    computed["delta_kpa_per_c"] = pd.Series(delta_kpa_per_c, index=merged.index, dtype="float64")
    computed["pressure_fao_kpa"] = pd.Series(pressure_fao_kpa, index=merged.index, dtype="float64")
    computed["gamma_kpa_per_c"] = pd.Series(gamma_kpa_per_c, index=merged.index, dtype="float64")
    computed["Ra_MJ_m2_day"] = pd.Series(ra_mj_m2_day, index=merged.index, dtype="float64")
    computed["N_h"] = pd.Series(n_h, index=merged.index, dtype="float64")
    computed["Rs_MJ_m2_day"] = rs_mj_m2_day
    computed["Rso_MJ_m2_day"] = rso_mj_m2_day
    computed["Rns_MJ_m2_day"] = pd.Series(rns_mj_m2_day, index=merged.index, dtype="float64")
    computed["Rnl_MJ_m2_day"] = rnl_mj_m2_day
    computed["Rn_MJ_m2_day"] = pd.Series(rn_mj_m2_day, index=merged.index, dtype="float64")
    computed["u2_m_s"] = pd.Series(u2_m_s, index=merged.index, dtype="float64")
    computed["E_FAO"] = e_fao
    return computed


def compute_fao56_daily_intermediates(
    daily_table: pd.DataFrame,
    *,
    station_metadata: Mapping[str, Any] | pd.Series | None = None,
    latitude: float | None = None,
    elevation_m: float | None = None,
    vapour_pressure_unit: str = "hPa",
) -> pd.DataFrame:
    if vapour_pressure_unit.strip().lower() not in {"hpa", "kpa"}:
        raise ValueError(f"Unsupported vapour pressure unit: {vapour_pressure_unit}")

    if daily_table.empty:
        result = daily_table.copy()
        for column in FAO56_DERIVED_COLUMNS:
            result[column] = pd.Series(dtype="float64")
        return result

    latitude_value = _resolve_metadata_value(station_metadata, "latitude", latitude)
    elevation_value = _resolve_metadata_value(station_metadata, "elevation_m", elevation_m)

    prepared = daily_table.copy()
    if vapour_pressure_unit.strip().lower() == "kpa":
        prepared["vapour_pressure"] = pd.to_numeric(prepared["vapour_pressure"], errors="coerce") * 10.0

    station_id_value = str(prepared["station_id"].iloc[0]) if "station_id" in prepared.columns and not prepared.empty else "station"
    station_table = pd.DataFrame(
        [
            {
                "station_id": station_id_value,
                "latitude": latitude_value,
                "elevation_m": elevation_value,
            }
        ]
    )
    if "station_id" not in prepared.columns:
        prepared.insert(0, "station_id", station_id_value)

    computed = compute_fao56_daily_from_wide(prepared, station_table, wind_measurement_height_m=2.0, use_observed_pressure=False)
    result = daily_table.copy()
    result["es"] = computed["es_kpa"]
    result["vpd"] = computed["vpd_kpa"]
    result["delta"] = computed["delta_kpa_per_c"]
    result["pressure"] = computed["pressure_fao_kpa"]
    result["gamma"] = computed["gamma_kpa_per_c"]
    result["Ra"] = computed["Ra_MJ_m2_day"]
    result["N"] = computed["N_h"]
    result["Rs"] = computed["Rs_MJ_m2_day"]
    result["Rso"] = computed["Rso_MJ_m2_day"]
    result["Rns"] = computed["Rns_MJ_m2_day"]
    result["Rnl"] = computed["Rnl_MJ_m2_day"]
    result["Rn"] = computed["Rn_MJ_m2_day"]
    result["G"] = 0.0
    result["E_FAO"] = computed["E_FAO"]
    return result


def _normalize_station_columns(stations: pd.DataFrame) -> pd.DataFrame:
    renamed = stations.copy()
    column_aliases = {
        "latitude_deg": "latitude",
        "lat": "latitude",
        "elevation": "elevation_m",
        "elevation_masl": "elevation_m",
    }
    applicable = {source: target for source, target in column_aliases.items() if source in renamed.columns and target not in renamed.columns}
    if applicable:
        renamed = renamed.rename(columns=applicable)
    return renamed


def _fao_daily_output_columns() -> list[str]:
    return [
        "doy",
        "ea_kpa",
        "es_kpa",
        "vpd_raw_kpa",
        "vpd_kpa",
        "delta_kpa_per_c",
        "pressure_fao_kpa",
        "gamma_kpa_per_c",
        "Ra_MJ_m2_day",
        "N_h",
        "Rs_MJ_m2_day",
        "Rso_MJ_m2_day",
        "Rns_MJ_m2_day",
        "Rnl_MJ_m2_day",
        "Rn_MJ_m2_day",
        "u2_m_s",
        "E_FAO",
    ]


def _resolve_metadata_value(
    station_metadata: Mapping[str, Any] | pd.Series | None,
    field_name: str,
    explicit_value: float | None,
) -> float | None:
    if explicit_value is not None:
        return explicit_value
    if station_metadata is None:
        return None
    raw_value = station_metadata.get(field_name) if hasattr(station_metadata, "get") else None
    parsed = pd.to_numeric(pd.Series([raw_value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _sunset_hour_angle(latitude_rad, day_of_year):
    solar_declination = 0.409 * np.sin((2.0 * np.pi / 365.0) * day_of_year - 1.39)
    sunset_term = -np.tan(latitude_rad) * np.tan(solar_declination)
    return np.arccos(np.clip(sunset_term, -1.0, 1.0))
