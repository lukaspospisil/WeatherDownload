from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import requests

from weatherdownload import ObservationQuery, export_table, read_station_metadata
from weatherdownload.providers import get_provider
from weatherdownload.providers.sk.observations import normalize_daily_observations_shmu
from weatherdownload.providers.sk.parser import parse_recent_daily_payload_json
from weatherdownload.providers.sk.probe import probe_shmu_observation_feeds, resolve_latest_recent_daily_probe_url
from weatherdownload.providers.sk.registry import get_dataset_spec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Probe the experimental SHMU recent/daily feed, cache a sample, and normalize one station-day slice.')
    parser.add_argument('--country', default='SK', help='Country code. Currently only SK is supported by this example.')
    parser.add_argument('--month', default=None, help='Optional month in YYYY-MM format. Defaults to the latest available SHMU recent/daily month.')
    parser.add_argument('--date', default=None, help='Optional date in YYYY-MM-DD format. Defaults to the first available date for the selected station.')
    parser.add_argument('--station-id', default=None, dest='station_id', help='Optional SHMU station_id (ind_kli). Defaults to the first station in the sampled month.')
    parser.add_argument('--element', action='append', dest='elements', default=None, help='Canonical or raw element code. Can be provided multiple times.')
    parser.add_argument('--cache-dir', type=Path, default=Path('outputs/sk_probe_cache'), help='Base cache directory for the experimental SHMU sample.')
    parser.add_argument('--output', type=Path, default=Path('outputs/sk_recent_daily_sample.csv'), help='Normalized sample CSV output path.')
    parser.add_argument('--timeout', type=int, default=60, help='HTTP timeout in seconds.')
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    country = args.country.strip().upper()
    requested_elements = args.elements or ['tas_max', 'precipitation']
    if country != 'SK':
        raise SystemExit('This experimental example currently supports only --country SK.')

    spec = get_dataset_spec('recent', 'daily')
    cache_root = args.cache_dir / country / 'recent' / 'daily'
    metadata_cache_path = cache_root / 'metadata' / 'kli_inter_metadata.json'
    month_cache_path = _resolve_month_cache_path(cache_root, args.month, timeout=args.timeout)

    # Cache the SHMU feed summary so the sampled assumptions stay inspectable offline.
    probe_table = probe_shmu_observation_feeds(timeout=args.timeout)
    cache_probe_summary(probe_table, cache_root)

    # Cache the raw metadata description JSON and the selected monthly observations JSON.
    cache_text_file(spec.metadata_url, metadata_cache_path, timeout=args.timeout)
    cache_text_file(_resolve_month_url(args.month, timeout=args.timeout), month_cache_path, timeout=args.timeout)

    payload_text = month_cache_path.read_text(encoding='utf-8')
    _, raw_table = parse_recent_daily_payload_json(payload_text)
    if raw_table.empty:
        raise SystemExit(f'No records were found in cached SHMU sample {month_cache_path}.')

    station_id = args.station_id or str(raw_table['ind_kli'].iloc[0]).strip()
    date_value = args.date or str(pd.to_datetime(raw_table.loc[raw_table['ind_kli'].astype(str) == station_id, 'datum']).dt.date.iloc[0])

    # Reuse the public query model so canonical element handling matches the rest of the library.
    query = ObservationQuery(
        country='SK',
        dataset_scope='recent',
        resolution='daily',
        station_ids=[station_id],
        start_date=date_value,
        end_date=date_value,
        elements=requested_elements,
    )

    # Derive minimal station metadata from the same cached SHMU month sample.
    station_metadata = read_station_metadata(country='SK', source_url=str(month_cache_path), timeout=args.timeout)
    normalized = normalize_daily_observations_shmu(raw_table, query=query, station_metadata=station_metadata)
    if normalized.empty:
        raise SystemExit(
            f'No normalized SHMU records were found for station_id={station_id}, date={date_value}, elements={query.elements}.'
        )

    provenance_path = write_shmu_cache_provenance(
        cache_root,
        requested_date=date_value,
        requested_station_id=station_id,
        requested_elements=requested_elements,
        resolved_elements=query.elements or [],
    )

    destination = export_table(normalized, output_path=args.output, format='csv')
    print(f'Cached SHMU metadata to {metadata_cache_path}')
    print(f'Cached SHMU monthly observations to {month_cache_path}')
    print(f'Wrote SHMU probe summary to {cache_root / "probe_summary.csv"}')
    print(f'Wrote SHMU provenance to {provenance_path}')
    print(f'Exported normalized SHMU sample to {destination}')
    print(_format_provenance_summary(provenance_path))
    print(normalized.to_string(index=False))
    return 0


def _resolve_month_url(month: str | None, *, timeout: int) -> str:
    if month is None:
        return resolve_latest_recent_daily_probe_url(timeout=timeout)
    month = month.strip()
    if not month:
        return resolve_latest_recent_daily_probe_url(timeout=timeout)
    return f'https://opendata.shmu.sk/meteorology/climate/recent/data/daily/{month}/kli-inter - {month}.json'


def _resolve_month_cache_path(cache_root: Path, month: str | None, *, timeout: int) -> Path:
    month_url = _resolve_month_url(month, timeout=timeout)
    file_name = month_url.rsplit('/', 1)[-1]
    return cache_root / 'months' / file_name


def cache_probe_summary(probe_table: pd.DataFrame, cache_root: Path) -> None:
    cache_root.mkdir(parents=True, exist_ok=True)
    probe_table.to_csv(cache_root / 'probe_summary.csv', index=False, encoding='utf-8')


def cache_text_file(source_url: str, destination: Path, *, timeout: int) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return destination
    response = requests.get(source_url, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    destination.write_text(response.text, encoding='utf-8')
    return destination


def write_shmu_cache_provenance(
    cache_root: Path,
    *,
    requested_date: str,
    requested_station_id: str,
    requested_elements: list[str],
    resolved_elements: list[str],
) -> Path:
    cache_root.mkdir(parents=True, exist_ok=True)
    provider = get_provider('SK')
    provenance = {
        'provider_name': provider.name,
        'experimental': provider.experimental,
        'requested_date': requested_date,
        'requested_station_id': requested_station_id,
        'requested_elements': list(requested_elements),
        'resolved_elements': list(resolved_elements),
        'source_mode': 'recent daily',
    }
    destination = cache_root / 'provenance.json'
    destination.write_text(json.dumps(provenance, indent=2, ensure_ascii=False), encoding='utf-8')
    return destination


def _format_provenance_summary(provenance_path: Path) -> str:
    provenance = json.loads(provenance_path.read_text(encoding='utf-8'))
    return (
        'Provenance summary: '
        f"provider={provenance['provider_name']}; experimental={provenance['experimental']}; "
        f"source_mode={provenance['source_mode']}; station_id={provenance['requested_station_id']}; "
        f"date={provenance['requested_date']}; elements={', '.join(provenance['requested_elements'])}"
    )


if __name__ == '__main__':
    raise SystemExit(main())

