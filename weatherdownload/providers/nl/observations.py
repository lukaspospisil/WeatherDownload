from __future__ import annotations

from ..ghcnd.wrappers import build_daily_observation_downloader
from .daily import download_daily_observations_knmi
from .registry import get_dataset_spec

download_daily_observations_ghcnd = build_daily_observation_downloader(get_dataset_spec=get_dataset_spec)

