from __future__ import annotations

import pandas as pd

from .ghcnd import read_station_metadata_ghcnd, read_station_observation_metadata_ghcnd
from .ghcnd import download_daily_observations_ghcnd
from .registry import get_dataset_spec, list_dataset_specs, list_implemented_dataset_specs
from ...metadata import _read_station_metadata_chmi, _read_station_observation_metadata_chmi
from ..base import WeatherProvider


def _download_observations(*args, **kwargs):
    from ...observations import _download_observations_chmi

    query = args[0] if args else kwargs.get('query')
    if getattr(query, 'provider', None) == 'ghcnd':
        return download_daily_observations_ghcnd(*args, **kwargs)
    return _download_observations_chmi(*args, **kwargs)


def _is_ghcnd_metadata_source(source_url: str | None) -> bool:
    if source_url is None:
        return False
    normalized = source_url.replace('\\', '/').lower()
    filename = normalized.rsplit('/', 1)[-1]
    return 'ghcnd' in normalized or filename.endswith('stations.txt') or filename.endswith('inventory.txt')


def _build_station_elements_attrs(chmi_table: pd.DataFrame, ghcnd_table: pd.DataFrame) -> dict[tuple[str, str], dict[str, list[str]]]:
    attrs: dict[tuple[str, str], dict[str, list[str]]] = {
        (spec.provider, spec.resolution): {}
        for spec in list_implemented_dataset_specs()
    }
    implemented_chmi_specs = [spec for spec in list_implemented_dataset_specs() if spec.provider != 'ghcnd']
    chmi_station_ids = [str(value) for value in chmi_table.get('station_id', pd.Series(dtype='object')).tolist()]
    for spec in implemented_chmi_specs:
        attrs[(spec.provider, spec.resolution)] = {
            station_id: list(spec.supported_elements)
            for station_id in chmi_station_ids
        }
    ghcnd_attrs = ghcnd_table.attrs.get('station_provider_raw_elements_by_path', {})
    for path_key, station_map in ghcnd_attrs.items():
        attrs.setdefault(path_key, {})
        attrs[path_key].update(station_map)
    return attrs


def _attach_station_elements_attrs(chmi_table: pd.DataFrame, ghcnd_table: pd.DataFrame) -> pd.DataFrame:
    if chmi_table.empty and ghcnd_table.empty:
        return chmi_table.copy()
    table = chmi_table.copy() if not chmi_table.empty else ghcnd_table.copy()
    table.attrs['station_provider_raw_elements_by_path'] = _build_station_elements_attrs(chmi_table, ghcnd_table)
    return table


def _combine_station_metadata(chmi_table: pd.DataFrame, ghcnd_table: pd.DataFrame) -> pd.DataFrame:
    if chmi_table.empty:
        combined = ghcnd_table.copy()
    elif ghcnd_table.empty:
        combined = chmi_table.copy()
    else:
        combined = pd.concat([chmi_table, ghcnd_table], ignore_index=True)
        combined = combined.sort_values('station_id', kind='stable').reset_index(drop=True)
    combined.attrs['station_provider_raw_elements_by_path'] = _build_station_elements_attrs(chmi_table, ghcnd_table)
    return combined


def _read_station_metadata(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    if _is_ghcnd_metadata_source(source_url):
        ghcnd_table = read_station_metadata_ghcnd(source_url, timeout)
        return _attach_station_elements_attrs(pd.DataFrame(columns=ghcnd_table.columns), ghcnd_table)
    if source_url is not None:
        chmi_table = _read_station_metadata_chmi(source_url, timeout)
        return _attach_station_elements_attrs(chmi_table, pd.DataFrame(columns=chmi_table.columns))
    chmi_table = _read_station_metadata_chmi(source_url, timeout)
    ghcnd_table = read_station_metadata_ghcnd(None, timeout)
    return _combine_station_metadata(chmi_table, ghcnd_table)


def _read_station_observation_metadata(source_url: str | None = None, timeout: int = 60) -> pd.DataFrame:
    if _is_ghcnd_metadata_source(source_url):
        return read_station_observation_metadata_ghcnd(source_url, timeout)
    if source_url is not None:
        return _read_station_observation_metadata_chmi(source_url, timeout)
    chmi_table = _read_station_observation_metadata_chmi(source_url, timeout)
    ghcnd_table = read_station_observation_metadata_ghcnd(None, timeout)
    combined = pd.concat([chmi_table, ghcnd_table], ignore_index=True)
    return combined.sort_values(['station_id', 'element', 'obs_type'], kind='stable').reset_index(drop=True)


SUPPORTED_CANONICAL_ELEMENTS = (
    'tas_mean',
    'tas_max',
    'tas_min',
    'tas_period_max',
    'soil_temperature_10cm',
    'soil_temperature_100cm',
    'sunshine_duration',
    'open_water_evaporation',
    'vapour_pressure',
    'wind_speed',
    'snow_depth',
    'pressure',
    'relative_humidity',
    'precipitation',
    'wind_from_direction',
    'cloud_cover',
    'past_weather_1',
    'past_weather_2',
)


PROVIDER = WeatherProvider(
    country_code='CZ',
    name='CHMI + NOAA GHCN-Daily',
    read_station_metadata=_read_station_metadata,
    read_station_observation_metadata=_read_station_observation_metadata,
    list_dataset_specs=list_dataset_specs,
    list_implemented_dataset_specs=list_implemented_dataset_specs,
    get_dataset_spec=get_dataset_spec,
    download_observations=_download_observations,
    supported_country_codes=('CZ',),
    supported_providers=('ghcnd', 'historical', 'historical_csv', 'now', 'recent'),
    supported_resolutions=('10min', '1hour', 'daily', 'monthly', 'pentadic', 'phenomena', 'yearly'),
    supported_canonical_elements=SUPPORTED_CANONICAL_ELEMENTS,
)

