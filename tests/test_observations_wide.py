from __future__ import annotations

import pandas as pd
import pytest

from weatherdownload.utils.observations import observations_to_wide


def test_observations_to_wide_basic_long_to_wide_conversion() -> None:
    observations = pd.DataFrame(
        [
            {"station_id": "A", "date": "2024-01-02", "element": "tas_max", "value": 6.0},
            {"station_id": "A", "date": "2024-01-01", "element": "tas_mean", "value": 1.5},
            {"station_id": "A", "date": "2024-01-01", "element": "tas_max", "value": 5.0},
            {"station_id": "B", "date": "2024-01-01", "element": "tas_mean", "value": 2.5},
        ]
    )

    result = observations_to_wide(observations)

    assert list(result.columns) == ["station_id", "date", "tas_max", "tas_mean"]
    assert result.loc[0, "station_id"] == "A"
    assert result.loc[0, "date"] == "2024-01-01"
    assert result.loc[0, "tas_max"] == 5.0
    assert result.loc[0, "tas_mean"] == 1.5
    assert result.loc[1, "station_id"] == "A"
    assert result.loc[1, "date"] == "2024-01-02"
    assert result.loc[1, "tas_max"] == 6.0
    assert pd.isna(result.loc[1, "tas_mean"])
    assert result.loc[2, "station_id"] == "B"
    assert result.loc[2, "date"] == "2024-01-01"
    assert pd.isna(result.loc[2, "tas_max"])
    assert result.loc[2, "tas_mean"] == 2.5


def test_observations_to_wide_supports_observation_date_input_column() -> None:
    observations = pd.DataFrame(
        [
            {"station_id": "A", "observation_date": pd.Timestamp("2024-01-01").date(), "element": "pressure", "value": 1001.2},
        ]
    )

    result = observations_to_wide(observations)

    assert list(result.columns) == ["station_id", "date", "pressure"]
    assert result.loc[0, "date"] == pd.Timestamp("2024-01-01").date()
    assert result.loc[0, "pressure"] == 1001.2


def test_observations_to_wide_can_rename_elements_after_pivot() -> None:
    observations = pd.DataFrame(
        [
            {"station_id": "A", "observation_date": "2024-01-01", "element": "pressure", "value": 999.5},
        ]
    )

    result = observations_to_wide(
        observations,
        rename_elements={"pressure": "pressure_observed"},
    )

    assert list(result.columns) == ["station_id", "date", "pressure_observed"]
    assert result.loc[0, "pressure_observed"] == 999.5


def test_observations_to_wide_raises_for_duplicate_station_date_element() -> None:
    observations = pd.DataFrame(
        [
            {"station_id": "A", "date": "2024-01-01", "element": "tas_mean", "value": 1.5},
            {"station_id": "A", "date": "2024-01-01", "element": "tas_mean", "value": 1.7},
        ]
    )

    with pytest.raises(ValueError, match="duplicated station/date/element rows"):
        observations_to_wide(observations)
