from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .registry import get_dataset_spec
from ...metadata import STATION_OBSERVATION_METADATA_COLUMNS
from .hourly_fmi import build_minimal_station_metadata_from_timevaluepair_fixture

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='FI', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='FI',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_fmi(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    # Station discovery is intentionally not implemented yet. For tests and offline use,
    # allow passing a local timevaluepair fixture as the metadata source.
    if source_url is not None and Path(source_url).exists():
        return build_minimal_station_metadata_from_timevaluepair_fixture(source_url)
    return pd.DataFrame(
        columns=[
            'station_id',
            'gh_id',
            'begin_date',
            'end_date',
            'full_name',
            'longitude',
            'latitude',
            'elevation_m',
        ]
    )


def read_station_observation_metadata_fmi(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    # Not implemented yet for FMI.
    return pd.DataFrame(columns=STATION_OBSERVATION_METADATA_COLUMNS)
