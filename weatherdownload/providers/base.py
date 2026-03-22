from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

import pandas as pd

if TYPE_CHECKING:
    from weatherdownload.queries import ObservationQuery


@dataclass(frozen=True)
class WeatherProvider:
    country_code: str
    name: str
    read_station_metadata: Callable[[str | None, int], pd.DataFrame]
    read_station_observation_metadata: Callable[[str | None, int], pd.DataFrame]
    list_dataset_specs: Callable[[], list[Any]]
    list_implemented_dataset_specs: Callable[[], list[Any]]
    get_dataset_spec: Callable[[str, str], Any]
    download_observations: Callable[[ObservationQuery, int, pd.DataFrame | None], pd.DataFrame]
