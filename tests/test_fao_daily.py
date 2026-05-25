from __future__ import annotations

import pandas as pd
import pytest

from weatherdownload.fao import (
    atmospheric_pressure_from_elevation_kpa,
    compute_fao56_daily_from_wide,
    saturation_vapour_pressure_kpa,
    wind_speed_2m,
)


def test_saturation_vapour_pressure_kpa_at_20c_is_reasonable() -> None:
    result = float(saturation_vapour_pressure_kpa(20.0))
    assert result == pytest.approx(2.338, rel=0.01)


def test_atmospheric_pressure_from_elevation_kpa_at_sea_level_is_reasonable() -> None:
    result = float(atmospheric_pressure_from_elevation_kpa(0.0))
    assert result == pytest.approx(101.3, rel=0.01)


def test_wind_speed_2m_reduces_10m_wind_speed() -> None:
    result = float(wind_speed_2m(3.0, measurement_height_m=10.0))
    assert result < 3.0


def test_compute_fao56_daily_from_wide_computes_e_fao_for_complete_row() -> None:
    daily_wide = pd.DataFrame(
        [
            {
                "station_id": "A",
                "date": "2024-06-15",
                "tas_mean": 20.0,
                "tas_max": 28.0,
                "tas_min": 12.0,
                "wind_speed": 2.5,
                "vapour_pressure": 15.0,
                "sunshine_duration": 10.0,
                "pressure_observed": 1005.0,
            }
        ]
    )
    stations = pd.DataFrame(
        [
            {
                "station_id": "A",
                "latitude": 50.0,
                "elevation_m": 250.0,
            }
        ]
    )

    result = compute_fao56_daily_from_wide(daily_wide, stations)

    assert "E_FAO" in result.columns
    assert pd.notna(result.loc[0, "E_FAO"])
    assert float(result.loc[0, "E_FAO"]) > 0.0
    assert result.loc[0, "vpd_raw_kpa"] == pytest.approx(result.loc[0, "vpd_kpa"])
    assert pd.notna(result.loc[0, "u2_m_s"])
    assert float(result.loc[0, "u2_m_s"]) < 2.5
    assert result.loc[0, "pressure_observed"] == 1005.0


def test_compute_fao56_daily_from_wide_clips_negative_vpd_for_fao() -> None:
    daily_wide = pd.DataFrame(
        [
            {
                "station_id": "A",
                "date": "2024-06-15",
                "tas_mean": 20.0,
                "tas_max": 20.0,
                "tas_min": 20.0,
                "wind_speed": 2.5,
                "vapour_pressure": 30.0,
                "sunshine_duration": 10.0,
            }
        ]
    )
    stations = pd.DataFrame(
        [
            {
                "station_id": "A",
                "latitude": 50.0,
                "elevation_m": 250.0,
            }
        ]
    )

    result = compute_fao56_daily_from_wide(daily_wide, stations)

    assert float(result.loc[0, "vpd_raw_kpa"]) < 0.0
    assert result.loc[0, "vpd_kpa"] == 0.0


def test_compute_fao56_daily_from_wide_keeps_missing_inputs_as_nan_outputs() -> None:
    daily_wide = pd.DataFrame(
        [
            {
                "station_id": "A",
                "date": "2024-06-15",
                "tas_mean": 20.0,
                "tas_max": 28.0,
                "tas_min": 12.0,
                "wind_speed": 2.5,
                "vapour_pressure": pd.NA,
                "sunshine_duration": 10.0,
            }
        ]
    )
    stations = pd.DataFrame(
        [
            {
                "station_id": "A",
                "latitude": 50.0,
                "elevation_m": 250.0,
            }
        ]
    )

    result = compute_fao56_daily_from_wide(daily_wide, stations)

    assert pd.isna(result.loc[0, "E_FAO"])
