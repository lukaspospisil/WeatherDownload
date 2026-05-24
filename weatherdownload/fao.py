from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd


FAO56_DERIVED_COLUMNS = [
    'es',
    'vpd',
    'delta',
    'pressure',
    'gamma',
    'Ra',
    'N',
    'Rs',
    'Rso',
    'Rns',
    'Rnl',
    'Rn',
    'G',
    'E_FAO',
]
FAO56_DERIVED_UNITS = {
    'es': 'kPa',
    'vpd': 'kPa',
    'delta': 'kPa degC^-1',
    'pressure': 'kPa',
    'gamma': 'kPa degC^-1',
    'Ra': 'MJ m^-2 day^-1',
    'N': 'h day^-1',
    'Rs': 'MJ m^-2 day^-1',
    'Rso': 'MJ m^-2 day^-1',
    'Rns': 'MJ m^-2 day^-1',
    'Rnl': 'MJ m^-2 day^-1',
    'Rn': 'MJ m^-2 day^-1',
    'G': 'MJ m^-2 day^-1',
    'E_FAO': 'mm day^-1',
}
RS_RSO_RATIO_CLAMP = (0.0, 1.0)


def compute_fao56_daily_intermediates(
    daily_table: pd.DataFrame,
    *,
    station_metadata: Mapping[str, Any] | pd.Series | None = None,
    latitude: float | None = None,
    elevation_m: float | None = None,
    vapour_pressure_unit: str = 'hPa',
) -> pd.DataFrame:
    if daily_table.empty:
        result = daily_table.copy()
        for column in FAO56_DERIVED_COLUMNS:
            result[column] = pd.Series(dtype='float64')
        return result

    latitude_value = _resolve_metadata_value(station_metadata, 'latitude', latitude)
    elevation_value = _resolve_metadata_value(station_metadata, 'elevation_m', elevation_m)

    result = daily_table.copy()
    tmean = pd.to_numeric(result['tas_mean'], errors='coerce')
    tmax = pd.to_numeric(result['tas_max'], errors='coerce')
    tmin = pd.to_numeric(result['tas_min'], errors='coerce')
    wind_speed = pd.to_numeric(result['wind_speed'], errors='coerce')
    sunshine_duration = pd.to_numeric(result['sunshine_duration'], errors='coerce')
    vapour_pressure = pd.to_numeric(result['vapour_pressure'], errors='coerce')
    ea = _vapour_pressure_to_kpa(vapour_pressure, vapour_pressure_unit=vapour_pressure_unit)

    dates = _extract_dates(result)
    day_of_year = dates.dt.dayofyear.astype('float64')
    latitude_rad = np.deg2rad(latitude_value) if pd.notna(latitude_value) else np.nan

    es_tmax = _saturation_vapour_pressure_kpa(tmax)
    es_tmin = _saturation_vapour_pressure_kpa(tmin)
    es = (es_tmax + es_tmin) / 2.0
    vpd = es - ea
    delta = 4098.0 * _saturation_vapour_pressure_kpa(tmean) / np.power(tmean + 237.3, 2)

    pressure = (
        101.3 * np.power((293.0 - 0.0065 * elevation_value) / 293.0, 5.26)
        if pd.notna(elevation_value)
        else np.nan
    )
    gamma = 0.000665 * pressure if pd.notna(pressure) else np.nan

    dr = 1.0 + 0.033 * np.cos((2.0 * np.pi / 365.0) * day_of_year)
    solar_declination = 0.409 * np.sin((2.0 * np.pi / 365.0) * day_of_year - 1.39)

    omega_s = pd.Series(np.nan, index=result.index, dtype='float64')
    if np.isfinite(latitude_rad):
        sunset_term = -np.tan(latitude_rad) * np.tan(solar_declination.to_numpy(dtype='float64'))
        sunset_term = np.clip(sunset_term, -1.0, 1.0)
        omega_s = pd.Series(np.arccos(sunset_term), index=result.index, dtype='float64')

    Ra = pd.Series(np.nan, index=result.index, dtype='float64')
    N = pd.Series(np.nan, index=result.index, dtype='float64')
    if np.isfinite(latitude_rad):
        sin_latitude = np.sin(latitude_rad)
        cos_latitude = np.cos(latitude_rad)
        Ra = (
            (24.0 * 60.0 / np.pi)
            * 0.0820
            * dr
            * (
                omega_s * sin_latitude * np.sin(solar_declination)
                + cos_latitude * np.cos(solar_declination) * np.sin(omega_s)
            )
        )
        N = (24.0 / np.pi) * omega_s

    n_over_N = pd.Series(np.nan, index=result.index, dtype='float64')
    valid_daylight = N.gt(0.0) & sunshine_duration.notna()
    n_over_N.loc[valid_daylight] = sunshine_duration.loc[valid_daylight] / N.loc[valid_daylight]
    Rs = (0.25 + 0.50 * n_over_N) * Ra

    Rso = pd.Series(np.nan, index=result.index, dtype='float64')
    if pd.notna(elevation_value):
        Rso = (0.75 + 2e-5 * elevation_value) * Ra

    Rs = Rs.clip(lower=0.0)
    rs_rso_ratio = pd.Series(np.nan, index=result.index, dtype='float64')
    valid_rso = Rso.gt(0.0)
    Rs.loc[valid_rso] = np.minimum(Rs.loc[valid_rso], Rso.loc[valid_rso])
    rs_rso_ratio.loc[valid_rso] = (Rs.loc[valid_rso] / Rso.loc[valid_rso]).clip(
        lower=RS_RSO_RATIO_CLAMP[0],
        upper=RS_RSO_RATIO_CLAMP[1],
    )

    Rns = 0.77 * Rs
    tmax_k = tmax + 273.16
    tmin_k = tmin + 273.16
    ea_nonnegative = ea.clip(lower=0.0)
    Rnl = (
        4.903e-9
        * ((np.power(tmax_k, 4) + np.power(tmin_k, 4)) / 2.0)
        * (0.34 - 0.14 * np.sqrt(ea_nonnegative))
        * (1.35 * rs_rso_ratio - 0.35)
    )
    Rn = Rns - Rnl
    G = pd.Series(0.0, index=result.index, dtype='float64')

    eto_denominator = delta + gamma * (1.0 + 0.34 * wind_speed)
    eto_numerator = 0.408 * delta * (Rn - G) + gamma * (900.0 / (tmean + 273.0)) * wind_speed * vpd
    E_FAO = pd.Series(np.nan, index=result.index, dtype='float64')
    valid_eto = eto_denominator.notna() & eto_numerator.notna() & (eto_denominator != 0.0)
    E_FAO.loc[valid_eto] = eto_numerator.loc[valid_eto] / eto_denominator.loc[valid_eto]

    derived = {
        'es': es,
        'vpd': vpd,
        'delta': delta,
        'pressure': pd.Series(pressure, index=result.index, dtype='float64'),
        'gamma': pd.Series(gamma, index=result.index, dtype='float64'),
        'Ra': pd.Series(Ra, index=result.index, dtype='float64'),
        'N': pd.Series(N, index=result.index, dtype='float64'),
        'Rs': Rs,
        'Rso': Rso,
        'Rns': Rns,
        'Rnl': Rnl,
        'Rn': Rn,
        'G': G,
        'E_FAO': E_FAO,
    }
    for name in FAO56_DERIVED_COLUMNS:
        result[name] = pd.to_numeric(derived[name], errors='coerce')
    return result


def _resolve_metadata_value(
    station_metadata: Mapping[str, Any] | pd.Series | None,
    field_name: str,
    explicit_value: float | None,
) -> float | None:
    if explicit_value is not None:
        return explicit_value
    if station_metadata is None:
        return None
    raw_value = station_metadata.get(field_name) if hasattr(station_metadata, 'get') else None
    parsed = pd.to_numeric(pd.Series([raw_value]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)


def _extract_dates(daily_table: pd.DataFrame) -> pd.Series:
    if 'date' in daily_table.columns:
        return pd.to_datetime(daily_table['date'], errors='coerce')
    if isinstance(daily_table.index, pd.DatetimeIndex):
        return pd.Series(daily_table.index, index=daily_table.index)
    raise KeyError("compute_fao56_daily_intermediates requires a 'date' column or DatetimeIndex.")


def _vapour_pressure_to_kpa(vapour_pressure: pd.Series, *, vapour_pressure_unit: str) -> pd.Series:
    normalized_unit = vapour_pressure_unit.strip().lower()
    if normalized_unit == 'kpa':
        return vapour_pressure
    if normalized_unit == 'hpa':
        return vapour_pressure / 10.0
    raise ValueError(f'Unsupported vapour pressure unit: {vapour_pressure_unit}')


def _saturation_vapour_pressure_kpa(temperature_c: pd.Series) -> pd.Series:
    return 0.6108 * np.exp((17.27 * temperature_c) / (temperature_c + 237.3))
