from __future__ import annotations

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .registry import get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='FR', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='FR',
    get_dataset_spec=get_dataset_spec,
)
