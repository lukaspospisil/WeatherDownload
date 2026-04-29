from __future__ import annotations

from ..ghcnd.registry import GHCND_STANDARD_CANONICAL_ELEMENTS
from ..ghcnd.wrappers import build_country_wrapper_bundle


_GHCND_BUNDLE = build_country_wrapper_bundle(
    country_code='CH',
    ghcn_prefix='SZ',
    canonical_elements=GHCND_STANDARD_CANONICAL_ELEMENTS,
)

SUPPORTED_CANONICAL_ELEMENTS = _GHCND_BUNDLE.supported_canonical_elements
list_dataset_specs = _GHCND_BUNDLE.list_dataset_specs
list_implemented_dataset_specs = _GHCND_BUNDLE.list_implemented_dataset_specs
get_dataset_spec = _GHCND_BUNDLE.get_dataset_spec
read_station_metadata_ghcnd = _GHCND_BUNDLE.read_station_metadata
read_station_observation_metadata_ghcnd = _GHCND_BUNDLE.read_station_observation_metadata
download_daily_observations_ghcnd = _GHCND_BUNDLE.download_daily_observations
build_station_dly_url = _GHCND_BUNDLE.build_station_dly_url
