from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from io import StringIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from weatherdownload.providers.ie.registry import (
    IE_AUDIT_REQUIRED_RAW_COLUMNS,
    IE_AUDITED_DAILY_STATIONS_PATH,
    IE_METEIREANN_DAILY_CSV_URL_TEMPLATE,
    IE_METEIREANN_STATION_DETAILS_URL,
)


CANDIDATE_STATIONS = [
    ('175', 'Phoenix Park'),
    ('275', 'Mace Head'),
    ('375', 'Oak Park'),
    ('475', 'Johnstown Castle'),
    ('518', 'Shannon Airport'),
    ('532', 'Dublin Airport'),
    ('575', 'Moore Park'),
    ('675', 'Ballyhaise'),
    ('875', 'Mullingar'),
    ('1075', 'Roches Point'),
    ('1175', 'Newport'),
    ('1275', 'Markree'),
    ('1375', 'Dunsany'),
    ('1475', 'Gurteen'),
    ('1575', 'Malin Head'),
    ('1775', 'Johnstown Castle 2'),
    ('1875', 'Athenry'),
    ('2175', 'Claremorris'),
    ('2275', 'Valentia Observatory'),
    ('2375', 'Belmullet'),
    ('3402', 'Sherkin Island'),
    ('3723', 'Casement'),
    ('3904', 'Cork Airport'),
    ('4935', 'Knock Airport'),
    ('833', 'Newport Furnace'),
]


@dataclass(frozen=True)
class AuditStation:
    station_id: str
    gh_id: None
    begin_date: str
    end_date: str
    full_name: str
    longitude: float | None
    latitude: float | None
    elevation_m: float | None


def main() -> None:
    station_rows = _load_station_details()
    valid: list[AuditStation] = []
    rejected: list[dict[str, str]] = []

    for station_id, station_name in CANDIDATE_STATIONS:
        row = station_rows.get(station_id)
        if row is None:
            rejected.append({'station_id': station_id, 'reason': 'not_in_station_details'})
            continue
        status, missing = _probe_daily_csv(station_id)
        if status != 'ok':
            rejected.append({'station_id': station_id, 'reason': status})
            continue
        if missing:
            rejected.append({'station_id': station_id, 'reason': f'missing_columns:{",".join(missing)}'})
            continue
        valid.append(
            AuditStation(
                station_id=station_id,
                gh_id=None,
                begin_date=_year_to_begin_date(row.get('open year', '')),
                end_date=_year_to_end_date(row.get('close year', '')),
                full_name=_title_name(row.get('name', '')),
                longitude=_parse_float(row.get('longitude', '')),
                latitude=_parse_float(row.get('latitude', '')),
                elevation_m=_parse_float(row.get('height(m)', '')),
            )
        )

    payload = {
        'provider': 'meteireann',
        'resolution': 'daily',
        'station_details_url': IE_METEIREANN_STATION_DETAILS_URL,
        'daily_csv_url_template': IE_METEIREANN_DAILY_CSV_URL_TEMPLATE,
        'required_raw_columns': list(IE_AUDIT_REQUIRED_RAW_COLUMNS),
        'audited_station_count': len(valid),
        'stations': [asdict(station) for station in sorted(valid, key=lambda item: item.station_id)],
        'rejected': rejected,
    }
    IE_AUDITED_DAILY_STATIONS_PATH.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
    print(f'Wrote {IE_AUDITED_DAILY_STATIONS_PATH}')


def _load_station_details() -> dict[str, dict[str, str]]:
    with urlopen(IE_METEIREANN_STATION_DETAILS_URL, timeout=20) as response:
        text = response.read().decode('utf-8-sig')
    reader = csv.DictReader(StringIO(text))
    rows: dict[str, dict[str, str]] = {}
    for row in reader:
        station_id = str(row.get('station name', '')).strip()
        if station_id and station_id not in rows:
            rows[station_id] = {str(key): str(value) for key, value in row.items()}
    return rows


def _probe_daily_csv(station_id: str) -> tuple[str, list[str]]:
    url = IE_METEIREANN_DAILY_CSV_URL_TEMPLATE.format(station_id=station_id)
    try:
        with urlopen(url, timeout=20) as response:
            text = response.read().decode('utf-8-sig', errors='replace')
    except HTTPError as exc:
        return f'http_{exc.code}', []
    except URLError:
        return 'network_error', []
    lines = [line for line in text.replace('\r\n', '\n').split('\n') if line.strip()]
    header = next((line for line in lines if line.lower().startswith('date,')), '')
    if not header:
        return 'no_header', []
    columns = [column.strip().casefold() for column in header.split(',')]
    missing = [column for column in IE_AUDIT_REQUIRED_RAW_COLUMNS if column not in columns]
    return 'ok', missing


def _parse_float(value: str) -> float | None:
    cleaned = value.strip()
    if not cleaned or cleaned == '(null)':
        return None
    return float(cleaned.replace(',', '.'))


def _year_to_begin_date(value: str) -> str:
    cleaned = value.strip()
    if cleaned.isdigit():
        return f'{cleaned}-01-01T00:00Z'
    return ''


def _year_to_end_date(value: str) -> str:
    cleaned = value.strip()
    if cleaned.isdigit():
        return f'{cleaned}-12-31T23:59Z'
    return ''


def _title_name(value: str) -> str:
    lowered = value.strip().lower()
    return ' '.join(part.capitalize() for part in lowered.split())


if __name__ == '__main__':
    main()
