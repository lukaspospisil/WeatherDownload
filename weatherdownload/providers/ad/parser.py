from __future__ import annotations

import html
import io
import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from posixpath import dirname, normpath
from urllib.parse import urljoin

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import AD_DAILY_PARAMETER_METADATA

AD_NORMALIZED_DAILY_COLUMNS = [
    'station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function',
    'value', 'flag', 'quality', 'provider', 'resolution',
]

_SELECT_STATIONS_RE = re.compile(
    r"<select[^>]+name=['\"]estacio['\"][^>]*id=['\"]estacio_diaris['\"][^>]*>(?P<body>.*?)</select>",
    re.IGNORECASE | re.DOTALL,
)
_OPTION_RE = re.compile(r"<option[^>]+value=['\"](?P<value>[^'\"]+)['\"][^>]*>(?P<label>.*?)</option>", re.IGNORECASE | re.DOTALL)
_DETAIL_ROW_RE = re.compile(r'<tr>\s*<td><strong>(?P<key>.*?)</strong></td>\s*<td[^>]*>(?P<value>.*?)</td>\s*</tr>', re.IGNORECASE | re.DOTALL)
_CHECKBOX_RE = re.compile(r"<input[^>]+name=['\"]dades['\"][^>]+value=['\"](?P<value>[^'\"]+)['\"][^>]*>\s*&nbsp;\s*(?P<label>[^<]*)", re.IGNORECASE)
_CELL_REF_RE = re.compile(r'([A-Z]+)')
_COORDINATE_RE = re.compile(r'(-?\d+,\d+)')

_WORKBOOK_HEADER_TO_FIELD = {
    'data dd mm aaaa': 'observation_date_raw',
    'hora hh mm': 'observation_time_raw',
    'temperatura minima c': 'temp_min',
    'temperatura minima oc': 'temp_min',
    'temperatura maxima c': 'temp_max',
    'temperatura maxima oc': 'temp_max',
    'temperatura mitjana c': 'temp_mitjana',
    'temperatura mitjana oc': 'temp_mitjana',
    'humitat relativa maxima': 'hum_max',
    'humitat relativa minima': 'hum_min',
    'humitat relativa mitjana': 'hum_mitjana',
    'gruix de neu fresca cm': 'neu',
    'insolacio min': 'insolacio_total',
    'radiacio j m 2': 'irradiacio_total',
    'velocitat mitjana del vent m s': 'vel_vent_mitjana',
    'direccio del vent mitjana': 'dir_vent_mitjana',
    'direccio del vent mitjana o': 'dir_vent_mitjana',
    'velocitat maxima del vent m s': 'vel_vent_max',
    'direccio de la velocitat maxima del vent': 'dir_vent_max',
    'direccio de la velocitat maxima del vent o': 'dir_vent_max',
    'precipitacio total mm': 'prec_total',
}


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return _decode_meteo_ad_text(local_path.read_bytes())
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    return _decode_meteo_ad_text(response.content)


def read_bytes_from_source(source: str, timeout: int, requests_module) -> bytes:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_bytes()
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    return response.content


def parse_meteo_ad_station_inventory_html(html_text: str) -> list[tuple[str, str]]:
    match = _SELECT_STATIONS_RE.search(html_text)
    if match is None:
        return []

    stations: list[tuple[str, str]] = []
    seen: set[str] = set()
    for option_match in _OPTION_RE.finditer(match.group('body')):
        station_id = _clean_string(option_match.group('value'))
        station_name = _clean_html_text(option_match.group('label'))
        if not station_id or station_id in seen:
            continue
        seen.add(station_id)
        stations.append((station_id, station_name))
    return stations


def parse_meteo_ad_station_detail_html(html_text: str) -> dict[str, object]:
    fields: dict[str, str] = {}
    for match in _DETAIL_ROW_RE.finditer(html_text):
        key = _normalize_key(match.group('key'))
        value = _clean_html_text(match.group('value'))
        if key:
            fields[key] = value

    station_id = _clean_string(fields.get('codi'))
    full_name = _clean_string(fields.get('nom'))
    longitude, latitude = _parse_coordinates(fields.get('coordenades', ''))
    begin_date, end_date = _parse_measurement_period(fields.get('periode de mesura', ''))

    return {
        'station_id': station_id,
        'gh_id': pd.NA,
        'begin_date': begin_date,
        'end_date': end_date,
        'full_name': full_name or pd.NA,
        'longitude': longitude,
        'latitude': latitude,
        'elevation_m': _parse_float(fields.get('altitud', '')),
    }


def normalize_meteo_ad_observation_metadata(
    stations: pd.DataFrame,
    spec: object,
    parameter_metadata: dict[str, dict[str, str]],
    station_supported_elements: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    station_supported_elements = station_supported_elements or {}

    for station in stations.itertuples(index=False):
        supported_elements = station_supported_elements.get(station.station_id, list(getattr(spec, 'supported_elements', ())))
        for raw_code in supported_elements:
            metadata = parameter_metadata.get(raw_code)
            if metadata is None:
                continue
            rows.append(
                {
                    'obs_type': 'HISTORICAL_DAILY',
                    'station_id': station.station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': 'P1D Meteo.ad climatology export',
                    'name': metadata['name'],
                    'description': metadata['description'],
                    'height': pd.NA,
                }
            )

    return pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)


def parse_meteo_ad_daily_variable_html(html_text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for match in _CHECKBOX_RE.finditer(html_text):
        raw_code = _clean_string(match.group('value'))
        label = _clean_html_text(match.group('label'))
        if raw_code:
            fields[raw_code] = label
    return fields


def parse_meteo_ad_daily_workbook(workbook_bytes: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(workbook_bytes)) as archive:
        shared_strings = _parse_shared_strings(archive)
        worksheet_path = _resolve_first_worksheet_path(archive)
        rows = _parse_worksheet_rows(archive.read(worksheet_path), shared_strings)

    if not rows:
        return pd.DataFrame()

    headers = rows[0]
    indexed_columns: list[tuple[int, str]] = []
    for index, header in enumerate(headers):
        normalized = _WORKBOOK_HEADER_TO_FIELD.get(_normalize_header(header))
        if normalized:
            indexed_columns.append((index, normalized))

    records: list[dict[str, object]] = []
    for row in rows[1:]:
        record = {
            field_name: row[index] if index < len(row) else ''
            for index, field_name in indexed_columns
        }
        if any(_clean_string(value) for value in record.values()):
            records.append(record)

    return pd.DataFrame.from_records(records)


def parse_meteo_ad_observation_date(value: object):
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    parsed = pd.to_datetime(cleaned, format='%d/%m/%Y', errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def parse_meteo_ad_numeric(raw_code: str, value: object) -> object:
    cleaned = _clean_string(value)
    if not cleaned:
        return pd.NA
    normalized = cleaned.replace(',', '.')
    parsed = pd.to_numeric(pd.Series([normalized]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return pd.NA
    numeric_value = float(parsed)
    if raw_code == 'insolacio_total':
        return numeric_value / 60.0
    if raw_code == 'irradiacio_total':
        return numeric_value / 1_000_000.0
    return numeric_value


def supported_daily_elements_from_fields(fields: dict[str, str]) -> list[str]:
    return [raw_code for raw_code in AD_DAILY_PARAMETER_METADATA if raw_code in fields]


def related_station_detail_source(source: str, station_id: str) -> str | None:
    source_path = Path(source)
    if not source_path.exists():
        return None
    candidate = source_path.with_name(f'sample_ad_meteo_ad_station_{station_id}.html')
    if candidate.exists():
        return str(candidate)
    return None


def related_daily_variables_source(source: str, station_id: str) -> str | None:
    source_path = Path(source)
    if not source_path.exists():
        return None
    candidate = source_path.with_name(f'sample_ad_meteo_ad_dades_0_{station_id}.html')
    if candidate.exists():
        return str(candidate)
    return None


def _decode_meteo_ad_text(payload: bytes) -> str:
    for encoding in ('utf-8-sig', 'cp1252', 'latin-1'):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode('utf-8', errors='replace')


def _parse_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        payload = archive.read('xl/sharedStrings.xml')
    except KeyError:
        return []

    root = ET.fromstring(payload)
    namespace = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    strings: list[str] = []
    for item in root.findall('main:si', namespace):
        text_parts = [node.text or '' for node in item.findall('.//main:t', namespace)]
        strings.append(''.join(text_parts))
    return strings


def _resolve_first_worksheet_path(archive: zipfile.ZipFile) -> str:
    workbook_root = ET.fromstring(archive.read('xl/workbook.xml'))
    rels_root = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
    main_ns = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    rel_ns = {'rel': 'http://schemas.openxmlformats.org/package/2006/relationships'}

    first_sheet = workbook_root.find('main:sheets/main:sheet', main_ns)
    if first_sheet is None:
        raise ValueError('Meteo.ad workbook does not contain a worksheet.')

    relationship_id = first_sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')
    if not relationship_id:
        raise ValueError('Meteo.ad workbook sheet is missing a relationship id.')

    for relationship in rels_root.findall('rel:Relationship', rel_ns):
        if relationship.attrib.get('Id') != relationship_id:
            continue
        target = relationship.attrib.get('Target', '')
        return normpath(urljoin('xl/', target))

    raise ValueError('Meteo.ad workbook worksheet relationship could not be resolved.')


def _parse_worksheet_rows(sheet_bytes: bytes, shared_strings: list[str]) -> list[list[str]]:
    root = ET.fromstring(sheet_bytes)
    namespace = {'main': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    rows: list[list[str]] = []

    for row in root.findall('.//main:sheetData/main:row', namespace):
        values: dict[int, str] = {}
        max_index = -1
        for cell in row.findall('main:c', namespace):
            reference = cell.attrib.get('r', '')
            column_index = _column_reference_to_index(reference)
            cell_type = cell.attrib.get('t', '')
            cell_value = _parse_cell_value(cell, namespace, shared_strings, cell_type)
            values[column_index] = cell_value
            max_index = max(max_index, column_index)
        if max_index < 0:
            continue
        rows.append([values.get(index, '') for index in range(max_index + 1)])

    return rows


def _parse_cell_value(
    cell: ET.Element,
    namespace: dict[str, str],
    shared_strings: list[str],
    cell_type: str,
) -> str:
    if cell_type == 'inlineStr':
        return ''.join(node.text or '' for node in cell.findall('.//main:t', namespace))

    value_node = cell.find('main:v', namespace)
    if value_node is None or value_node.text is None:
        return ''

    value_text = value_node.text
    if cell_type == 's':
        try:
            return shared_strings[int(value_text)]
        except (IndexError, ValueError):
            return ''
    return value_text


def _column_reference_to_index(reference: str) -> int:
    match = _CELL_REF_RE.match(reference)
    if match is None:
        return 0
    index = 0
    for letter in match.group(1):
        index = (index * 26) + (ord(letter) - ord('A') + 1)
    return max(index - 1, 0)


def _normalize_header(value: str) -> str:
    return _normalize_key(value).replace('  ', ' ').strip()


def _normalize_key(value: object) -> str:
    cleaned = _clean_html_text(value)
    normalized = unicodedata.normalize('NFKD', cleaned)
    ascii_text = normalized.encode('ascii', errors='ignore').decode('ascii')
    ascii_text = ascii_text.replace('/', ' ').replace('_', ' ')
    ascii_text = re.sub(r'[^a-zA-Z0-9]+', ' ', ascii_text)
    return ascii_text.strip().lower()


def _clean_html_text(value: object) -> str:
    if value is None:
        return ''
    text = html.unescape(str(value))
    text = re.sub(r'<[^>]+>', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


def _parse_coordinates(value: str) -> tuple[float | None, float | None]:
    matches = _COORDINATE_RE.findall(_clean_string(value))
    if len(matches) < 2:
        return None, None
    latitude = _parse_float(matches[-2])
    longitude = _parse_float(matches[-1])
    return longitude, latitude


def _parse_measurement_period(value: str) -> tuple[str, str]:
    cleaned = _clean_string(value)
    years = re.findall(r'(?:19|20)\d{2}', cleaned)
    if not years:
        return '', ''
    begin_year = years[0]
    if 'actual' in _normalize_key(cleaned):
        return f'{begin_year}-01-01T00:00Z', ''
    if len(years) >= 2:
        return f'{begin_year}-01-01T00:00Z', f'{years[1]}-12-31T23:59Z'
    return f'{begin_year}-01-01T00:00Z', ''


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    parsed = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
    if pd.isna(parsed):
        return None
    return float(parsed)
