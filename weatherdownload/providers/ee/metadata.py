from __future__ import annotations

import pandas as pd

from ..ghcnd.wrappers import (
    build_station_metadata_reader,
    build_station_observation_metadata_reader,
)
from .parser import (
    normalize_ee_observation_metadata,
    normalize_ee_station_metadata,
    parse_ee_payload_json,
    read_text_from_source,
)
from .registry import EE_ILMATEENISTUS_PARAMETER_METADATA, get_dataset_spec

read_station_metadata_ghcnd = build_station_metadata_reader(country_prefix='EE', get_dataset_spec=get_dataset_spec)
read_station_observation_metadata_ghcnd = build_station_observation_metadata_reader(
    country_prefix='EE',
    get_dataset_spec=get_dataset_spec,
)


def read_station_metadata_ilmateenistus(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('ilmateenistus', 'daily')
    source = source_url or spec.station_metadata_url
    records = parse_ee_payload_json(read_text_from_source(source, timeout))
    return normalize_ee_station_metadata(records, spec)


def read_station_observation_metadata_ilmateenistus(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    spec = get_dataset_spec('ilmateenistus', 'daily')
    source = source_url or spec.station_metadata_url
    records = parse_ee_payload_json(read_text_from_source(source, timeout))
    return normalize_ee_observation_metadata(records, spec, EE_ILMATEENISTUS_PARAMETER_METADATA)
