from __future__ import annotations

from ..ghcnd.wrappers import build_country_wrapper_bundle


_GHCND_BUNDLE = build_country_wrapper_bundle(
    country_code='AT',
    ghcn_prefix='AU',
    canonical_elements={
        'tas_max': ('TMAX',),
        'tas_min': ('TMIN',),
        'precipitation': ('PRCP',),
    },
)

SUPPORTED_CANONICAL_ELEMENTS = _GHCND_BUNDLE.supported_canonical_elements
list_dataset_specs = _GHCND_BUNDLE.list_dataset_specs
list_implemented_dataset_specs = _GHCND_BUNDLE.list_implemented_dataset_specs
get_dataset_spec = _GHCND_BUNDLE.get_dataset_spec
read_station_metadata_ghcnd = _GHCND_BUNDLE.read_station_metadata
read_station_observation_metadata_ghcnd = _GHCND_BUNDLE.read_station_observation_metadata
download_daily_observations_ghcnd = _GHCND_BUNDLE.download_daily_observations
build_station_dly_url = _GHCND_BUNDLE.build_station_dly_url
