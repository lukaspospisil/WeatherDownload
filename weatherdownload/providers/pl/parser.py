from __future__ import annotations

import csv
import io
from pathlib import Path

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS

PL_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]
PL_NORMALIZED_SUBDAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'timestamp',
    'value', 'flag', 'quality', 'dataset_scope', 'resolution',
]

PL_DAILY_SYNOP_COLUMNS = [
    'NSP', 'POST', 'ROK', 'MC', 'DZ', 'TMAX', 'WTMAX', 'TMIN', 'WTMIN', 'STD', 'WSTD',
    'TMNG', 'WTMNG', 'SMDB', 'WSMDB', 'ROOP', 'PKSN', 'WPKSN', 'RWSN', 'WRWSN', 'USL',
    'WUSL', 'DESZ', 'WDESZ', 'SNEG', 'WSNEG', 'DISN', 'WDISN', 'GRAD', 'WGRAD', 'MGLA',
    'WMGLA', 'ZMGL', 'WZMGL', 'SADZ', 'WSADZ', 'GOLO', 'WGOLO', 'ZMNI', 'WZMNI', 'ZMWS',
    'WZMWS', 'ZMET', 'WZMET', 'FF10', 'WFF10', 'FF15', 'WFF15', 'BRZA', 'WBRZA', 'ROSA',
    'WROSA', 'SZRO', 'WSZRO', 'DZPS', 'WDZPS', 'DZBL', 'WDZBL', 'SGR', 'IZD', 'WIZD',
    'IZG', 'WIZG', 'AKTN', 'WAKTN',
]

PL_DAILY_KLIMAT_COLUMNS = [
    'NSP', 'POST', 'ROK', 'MC', 'DZ', 'TMAX', 'WTMAX', 'TMIN', 'WTMIN', 'STD', 'WSTD',
    'TMNG', 'WTMNG', 'SMDB', 'WSMDB', 'ROOP', 'PKSN', 'WPKSN',
]

PL_HOURLY_SYNOP_COLUMNS = [
    'NSP', 'POST', 'ROK', 'MC', 'DZ', 'GG', 'HPOD', 'WHPOD', 'HPON', 'WHPON', 'HPOW', 'WHPOW',
    'HTXT', 'POM1', 'POM2', 'WID', 'WWID', 'WIDO', 'WWIDO', 'WIDA', 'WWIDA', 'NOG', 'WNOG',
    'KRWR', 'WKRWR', 'FWR', 'WFWR', 'PORW', 'WPORW', 'TEMP', 'WTEMP', 'TTZW', 'WTTZW', 'WENT',
    'TWLW', 'CPW', 'WCPW', 'WLGW', 'WWLGW', 'TPTR', 'WTPTR', 'PPPS', 'WPPPS', 'PPPM', 'WPPPM',
    'TECH', 'APP', 'WAPP', 'WO6G', 'WWO6G', 'ROPT', 'WROPT', 'POGB', 'POGU', 'CLCM', 'WCLCM',
    'CHCL', 'WCHCL', 'CHLT', 'CHCM', 'WCHCM', 'CHMT', 'CHCH', 'WCHCH', 'CHHT', 'SGRN', 'WSGRN',
    'DEFI', 'WDEFI', 'USLN', 'WUSLN', 'ROSW', 'WROSW', 'PORK', 'WPORK', 'GODP', 'MINP', 'TG05',
    'WTG05', 'TG10', 'WTG10', 'TG20', 'WTG20', 'TG50', 'WTG50', 'TG100', 'WTG100', 'TMIN',
    'WTMIN', 'TMAX', 'WTMAX', 'TGMI', 'WTGMI', 'RWSN', 'WRWSN', 'PKSN', 'WPKSN', 'HSS', 'WHSS',
    'GRSN', 'WGRSN', 'GATS', 'UKPO', 'HPRO', 'WHPRO', 'CIPR', 'WCIPR',
]

_PL_MISSING_SENTINELS = {''}


def parse_pl_station_metadata_csv(csv_text: str) -> pd.DataFrame:
    reader = csv.reader(io.StringIO(csv_text.lstrip('\ufeff')))
    rows: list[dict[str, object]] = []
    for row in reader:
        if len(row) < 3:
            continue
        gh_id = _clean_string(row[0])
        full_name = _clean_string(row[1])
        station_id = normalize_pl_station_id(row[2])
        if not station_id:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': gh_id or pd.NA,
                'begin_date': '',
                'end_date': '',
                'full_name': full_name or pd.NA,
                'longitude': None,
                'latitude': None,
                'elevation_m': None,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values(['station_id', 'gh_id'], kind='stable').reset_index(drop=True)
    return frame



def normalize_pl_observation_metadata(
    stations: pd.DataFrame,
    specs_and_metadata: list[tuple[object, dict[str, dict[str, str]]]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for station in stations.itertuples(index=False):
        for spec, parameter_metadata in specs_and_metadata:
            default_obs_type = _obs_type_for_resolution(getattr(spec, 'resolution', 'daily'))
            default_schedule = _schedule_for_resolution(getattr(spec, 'resolution', 'daily'))
            for raw_code in getattr(spec, 'supported_elements', ()):
                metadata = parameter_metadata.get(raw_code, {})
                rows.append(
                    {
                        'obs_type': metadata.get('obs_type', default_obs_type),
                        'station_id': station.station_id,
                        'begin_date': station.begin_date,
                        'end_date': station.end_date,
                        'element': raw_code,
                        'schedule': metadata.get('schedule', default_schedule),
                        'name': metadata.get('name', raw_code),
                        'description': metadata.get('description', pd.NA),
                        'height': pd.NA,
                    }
                )
    frame = pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.drop_duplicates().reset_index(drop=True)



def parse_pl_daily_synop_csv(csv_text: str) -> pd.DataFrame:
    return _parse_pl_delimited_csv(csv_text, PL_DAILY_SYNOP_COLUMNS)



def parse_pl_daily_klimat_csv(csv_text: str) -> pd.DataFrame:
    return _parse_pl_delimited_csv(csv_text, PL_DAILY_KLIMAT_COLUMNS)



def parse_pl_hourly_synop_csv(csv_text: str) -> pd.DataFrame:
    return _parse_pl_delimited_csv(csv_text, PL_HOURLY_SYNOP_COLUMNS)



def _parse_pl_delimited_csv(csv_text: str, columns: list[str]) -> pd.DataFrame:
    table = pd.read_csv(
        io.StringIO(csv_text.lstrip('\ufeff')),
        header=None,
        names=columns,
        dtype=str,
        keep_default_na=False,
    )
    for column in table.columns:
        table[column] = table[column].map(_clean_string)
    return table



def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    return decode_pl_bytes(response.content)



def decode_pl_bytes(payload: bytes) -> str:
    for encoding in ('utf-8-sig', 'cp1250', 'latin-1'):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode('utf-8', errors='replace')



def normalize_pl_station_id(value: object) -> str:
    cleaned = _clean_string(value).replace(' ', '')
    if not cleaned:
        return ''
    digits = ''.join(character for character in cleaned if character.isdigit())
    if not digits:
        return cleaned.upper()
    return digits.zfill(5)



def normalize_pl_gh_id(value: object) -> str:
    return _clean_string(value)



def normalize_pl_observation_date(row: pd.Series) -> object:
    year = _clean_string(row.get('ROK'))
    month = _clean_string(row.get('MC')).zfill(2)
    day = _clean_string(row.get('DZ')).zfill(2)
    if not year or not month or not day:
        return None
    parsed = pd.to_datetime(f'{year}-{month}-{day}', errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()



def normalize_pl_observation_timestamp(row: pd.Series) -> pd.Timestamp | None:
    year = _clean_string(row.get('ROK'))
    month = _clean_string(row.get('MC')).zfill(2)
    day = _clean_string(row.get('DZ')).zfill(2)
    hour = _clean_string(row.get('GG')).zfill(2)
    if not year or not month or not day or not hour:
        return None
    parsed = pd.to_datetime(f'{year}-{month}-{day}T{hour}:00:00Z', errors='coerce', utc=True)
    if pd.isna(parsed):
        return None
    return parsed



def normalize_pl_query_timestamp(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize('UTC')
    return timestamp.tz_convert('UTC')



def to_numeric_with_missing(series: pd.Series, flag_series: pd.Series | None = None, zero_when_flag_nine: bool = False) -> pd.Series:
    cleaned = series.astype('string').str.strip()
    if zero_when_flag_nine and flag_series is not None:
        flags = flag_series.astype('string').str.strip()
        cleaned = cleaned.mask(cleaned.eq('') & flags.eq('9'), '0')
    cleaned = cleaned.where(~cleaned.isin(_PL_MISSING_SENTINELS), pd.NA)
    return pd.to_numeric(cleaned.str.replace(',', '.', regex=False), errors='coerce')



def flag_with_missing(series: pd.Series) -> pd.Series:
    cleaned = series.astype('string').str.strip()
    return cleaned.where(cleaned.ne(''), pd.NA)



def station_lookup_by_gh_id(stations: pd.DataFrame) -> dict[str, str]:
    if stations.empty:
        return {}
    gh_ids = stations['gh_id'].astype('string').fillna('').str.strip()
    station_ids = stations['station_id'].astype('string').fillna('').str.strip()
    return {
        gh_id: station_id
        for gh_id, station_id in zip(gh_ids, station_ids)
        if gh_id and station_id
    }



def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip().strip('"')



def _obs_type_for_resolution(resolution: str) -> str:
    if resolution == '1hour':
        return 'HISTORICAL_HOURLY'
    return 'HISTORICAL_DAILY'



def _schedule_for_resolution(resolution: str) -> str:
    if resolution == '1hour':
        return 'PT1H IMGW terminowe synop'
    return 'P1D IMGW daily'
