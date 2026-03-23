from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests

from .shmu_parser import (
    parse_now_data_files,
    parse_now_payload_json,
    parse_recent_daily_month_directories,
    parse_recent_daily_month_files,
    parse_recent_daily_payload_json,
    parse_shmu_metadata_json,
)
from .shmu_registry import get_dataset_spec

SHMU_NOW_METADATA_URL = 'https://opendata.shmu.sk/meteorology/climate/now/metadata/aws1min_metadata.json'
SHMU_NOW_DATA_INDEX_URL = 'https://opendata.shmu.sk/meteorology/climate/now/data/'

PROBE_COLUMNS = [
    'dataset_scope',
    'resolution',
    'source_id',
    'implemented',
    'experimental',
    'metadata_url',
    'data_index_url',
    'sample_url',
    'sample_dataset',
    'sample_interval',
    'sample_frequency',
    'sample_station_count',
    'sample_record_count',
    'metadata_field_count',
    'notes',
]


def probe_shmu_observation_feeds(timeout: int = 60) -> pd.DataFrame:
    rows = [
        _probe_recent_daily(timeout=timeout),
        _probe_now_aws1min(timeout=timeout),
    ]
    return pd.DataFrame.from_records(rows, columns=PROBE_COLUMNS)


def _probe_recent_daily(timeout: int) -> dict[str, object]:
    spec = get_dataset_spec('recent', 'daily')
    metadata_table = parse_shmu_metadata_json(_read_text(spec.metadata_url, timeout=timeout))
    latest_url = resolve_latest_recent_daily_probe_url(timeout=timeout)
    payload_metadata, payload_table = parse_recent_daily_payload_json(_read_text(latest_url, timeout=timeout))
    statistics = payload_metadata.get('statistics') if isinstance(payload_metadata.get('statistics'), dict) else {}
    return {
        'dataset_scope': spec.dataset_scope,
        'resolution': spec.resolution,
        'source_id': spec.source_id,
        'implemented': spec.implemented,
        'experimental': spec.experimental,
        'metadata_url': spec.metadata_url,
        'data_index_url': spec.data_index_url,
        'sample_url': latest_url,
        'sample_dataset': payload_metadata.get('dataset'),
        'sample_interval': payload_metadata.get('interval'),
        'sample_frequency': payload_metadata.get('frequency'),
        'sample_station_count': statistics.get('stations_count', payload_table['ind_kli'].nunique()),
        'sample_record_count': statistics.get('records_count', len(payload_table.index)),
        'metadata_field_count': len(metadata_table.index),
        'notes': 'Implemented experimental path. Observation metadata and station coverage are derived from sampled monthly JSON, not an authoritative station registry.',
    }


def _probe_now_aws1min(timeout: int) -> dict[str, object]:
    metadata_table = parse_shmu_metadata_json(_read_text(SHMU_NOW_METADATA_URL, timeout=timeout))
    today_directory = f"{SHMU_NOW_DATA_INDEX_URL}{pd.Timestamp.now('UTC').strftime('%Y%m%d')}/"
    sample_url = pd.NA
    payload_dataset = pd.NA
    payload_interval = pd.NA
    payload_frequency = pd.NA
    station_count = pd.NA
    record_count = pd.NA
    notes = 'Observed but not implemented yet. File-name timestamps appear to describe snapshot/publication time; use row field `minuta` as the observation timestamp when this feed is implemented.'
    try:
        data_files = parse_now_data_files(_read_text(today_directory, timeout=timeout))
        if data_files:
            sample_url = f'{today_directory}{data_files[-1]}'
            payload_metadata, payload_table = parse_now_payload_json(_read_text(str(sample_url), timeout=timeout))
            statistics = payload_metadata.get('statistics') if isinstance(payload_metadata.get('statistics'), dict) else {}
            payload_dataset = payload_metadata.get('dataset')
            payload_interval = payload_metadata.get('interval')
            payload_frequency = payload_metadata.get('frequency')
            station_count = statistics.get('stations_count', payload_table['ind_kli'].nunique())
            record_count = statistics.get('records_count', len(payload_table.index))
    except Exception as exc:  # pragma: no cover - network availability varies
        notes = f'{notes} Probe warning: {exc}'
    return {
        'dataset_scope': 'now',
        'resolution': '1min',
        'source_id': 'aws1min_now',
        'implemented': False,
        'experimental': True,
        'metadata_url': SHMU_NOW_METADATA_URL,
        'data_index_url': today_directory,
        'sample_url': sample_url,
        'sample_dataset': payload_dataset,
        'sample_interval': payload_interval,
        'sample_frequency': payload_frequency,
        'sample_station_count': station_count,
        'sample_record_count': record_count,
        'metadata_field_count': len(metadata_table.index),
        'notes': notes,
    }


def resolve_latest_recent_daily_probe_url(timeout: int = 60) -> str:
    spec = get_dataset_spec('recent', 'daily')
    month_directories = parse_recent_daily_month_directories(_read_text(spec.data_index_url, timeout=timeout))
    if not month_directories:
        raise ValueError('No SHMU recent daily month directories were found during probing.')
    latest_month = month_directories[-1]
    month_dir_url = f'{spec.data_index_url}{latest_month}/'
    month_files = parse_recent_daily_month_files(_read_text(month_dir_url, timeout=timeout))
    if not month_files:
        raise ValueError(f'No SHMU recent daily files were found in {month_dir_url}')
    return f'{month_dir_url}{month_files[-1]}'


def _read_text(source: str, timeout: int) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text
