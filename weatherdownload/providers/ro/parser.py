from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests

from ...metadata import STATION_METADATA_COLUMNS, STATION_OBSERVATION_METADATA_COLUMNS
from .registry import RO_ANM_WFS_BASE_URL, RO_ANM_DAILY_CANONICAL_ELEMENTS

RO_ANM_NORMALIZED_DAILY_COLUMNS = [
    'station_id',
    'gh_id',
    'element',
    'element_raw',
    'observation_date',
    'time_function',
    'value',
    'flag',
    'quality',
    'provider',
    'resolution',
]

RO_ANM_NETWORK_FILENAME = 'network_n100.xml'
RO_ANM_STATION_FILENAME_TEMPLATE = 'station_{station_id}.xml'

NS = {
    'base': 'http://inspire.ec.europa.eu/schemas/base/3.3',
    'ef': 'http://inspire.ec.europa.eu/schemas/ef/4.0',
    'gml': 'http://www.opengis.net/gml/3.2',
    'om': 'http://www.opengis.net/om/2.0',
    'wml2': 'http://www.opengis.net/waterml/2.0',
}

RO_ANM_CANONICAL_BY_RAW = {
    raw_codes[0]: canonical
    for canonical, raw_codes in RO_ANM_DAILY_CANONICAL_ELEMENTS.items()
}

RO_ANM_VALUE_CONVERTERS = {
    'TemperatureAverageDailyCLIMAT': lambda value: value - 273.15,
    'TemperatureMaximumDailyCLIMAT': lambda value: value - 273.15,
    'TemperatureMinimumDailyCLIMAT': lambda value: value - 273.15,
    'TotalPrecipitationCLIMAT': lambda value: value,
}

RO_ANM_MISSING_SENTINELS = {'', 'nan', 'NaN', '-999', '-9999'}


def read_text_from_source(source: str, timeout: int, requests_module) -> str:
    local_path = Path(source)
    if local_path.exists():
        return local_path.read_text(encoding='utf-8')
    response = requests_module.get(source, timeout=timeout)
    response.raise_for_status()
    response.encoding = 'utf-8'
    return response.text


def read_ro_anm_station_metadata(source: str, timeout: int = 60) -> pd.DataFrame:
    network_xml = read_text_from_source(_network_source(source), timeout, requests)
    station_refs = parse_ro_anm_network_station_refs(network_xml)
    rows: list[dict[str, object]] = []
    station_elements: dict[str, list[str]] = {}
    for station_ref in station_refs:
        station_xml = read_text_from_source(_station_source(source, station_ref['station_id']), timeout, requests)
        row, raw_elements = parse_ro_anm_station_feature(station_xml)
        if row is None:
            continue
        rows.append(row)
        station_elements[row['station_id']] = raw_elements

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    frame.attrs['station_provider_raw_elements_by_path'] = {
        ('anm', 'daily'): station_elements,
    }
    return frame


def normalize_ro_anm_observation_metadata(
    stations: pd.DataFrame,
    spec,
    parameter_metadata: dict[str, dict[str, str]],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    station_raw_elements = stations.attrs.get('station_provider_raw_elements_by_path', {}).get((spec.provider, spec.resolution), {})
    for station in stations.itertuples(index=False):
        station_id = str(station.station_id)
        for raw_code in station_raw_elements.get(station_id, []):
            metadata = parameter_metadata[raw_code]
            rows.append(
                {
                    'obs_type': metadata['obs_type'],
                    'station_id': station_id,
                    'begin_date': station.begin_date,
                    'end_date': station.end_date,
                    'element': raw_code,
                    'schedule': metadata['schedule'],
                    'name': metadata['name'],
                    'description': f"{metadata['description']} Source unit: {metadata['unit']}.",
                    'height': pd.NA,
                }
            )
    frame = pd.DataFrame.from_records(rows, columns=STATION_OBSERVATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(['station_id', 'element'], kind='stable').reset_index(drop=True)


def normalize_ro_anm_daily_observation(
    xml_text: str,
    *,
    station_id: str,
    element_raw: str,
    provider: str,
    resolution: str,
    start_date,
    end_date,
) -> pd.DataFrame:
    root = ET.fromstring(xml_text)
    uom_node = root.find('.//wml2:defaultPointMetadata//wml2:uom', NS)
    unit = '' if uom_node is None else str(uom_node.attrib.get('code', '')).strip()

    rows: list[dict[str, object]] = []
    for point in root.findall('.//wml2:point/wml2:MeasurementTVP', NS):
        timestamp_text = _clean_string(point.findtext('wml2:time', default='', namespaces=NS))
        observation_timestamp = pd.to_datetime(timestamp_text, utc=True, errors='coerce')
        if pd.isna(observation_timestamp):
            continue
        observation_date = observation_timestamp.date()
        if start_date is not None and observation_date < start_date:
            continue
        if end_date is not None and observation_date > end_date:
            continue
        value = _parse_measurement_value(point.find('wml2:value', NS), element_raw)
        if value is None:
            continue
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'element': RO_ANM_CANONICAL_BY_RAW[element_raw],
                'element_raw': element_raw,
                'observation_date': observation_date,
                'time_function': pd.NA,
                'value': value,
                'flag': pd.NA,
                'quality': pd.Series([pd.NA], dtype='Int64').iloc[0],
                'provider': provider,
                'resolution': resolution,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=RO_ANM_NORMALIZED_DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame['quality'] = pd.Series([pd.NA] * len(frame), dtype='Int64')
    frame.attrs['source_unit'] = unit
    return frame.sort_values(['station_id', 'observation_date', 'element'], kind='stable').reset_index(drop=True)


def parse_ro_anm_network_station_refs(xml_text: str) -> list[dict[str, str]]:
    root = ET.fromstring(xml_text)
    refs: list[dict[str, str]] = []
    for node in root.findall('.//ef:contains/ef:NetworkFacility/ef:contains', NS):
        href = _clean_string(node.attrib.get('{http://www.w3.org/1999/xlink}href'))
        title = _clean_string(node.attrib.get('{http://www.w3.org/1999/xlink}title'))
        station_id = _station_id_from_href(href)
        if not station_id:
            continue
        refs.append({'station_id': station_id, 'href': href, 'title': title})
    return refs


def parse_ro_anm_station_feature(xml_text: str) -> tuple[dict[str, object] | None, list[str]]:
    root = ET.fromstring(xml_text)
    station = root.find('.//ef:EnvironmentalMonitoringFacility', NS)
    if station is None:
        return None, []
    station_id = _clean_string(station.findtext('gml:identifier', default='', namespaces=NS)).split('.')[-1]
    if not station_id:
        station_id = _clean_string(station.attrib.get('{http://www.opengis.net/gml/3.2}id')).split('.')[-1]
    full_name = _clean_station_name(
        station.findtext('gml:description', default='', namespaces=NS)
    ) or pd.NA

    longitude, latitude = _parse_station_coordinates(station)
    begin_date, end_date = _parse_station_activity_range(station)
    raw_elements: list[str] = []
    for narrower in station.findall('ef:narrower', NS):
        href = _clean_string(narrower.attrib.get('{http://www.w3.org/1999/xlink}href'))
        raw_code = href.rsplit('/', 1)[-1]
        if raw_code in RO_ANM_CANONICAL_BY_RAW:
            raw_elements.append(raw_code)
    raw_elements = sorted(set(raw_elements))

    row = {
        'station_id': station_id,
        'gh_id': pd.NA,
        'begin_date': begin_date,
        'end_date': end_date,
        'full_name': full_name,
        'longitude': longitude,
        'latitude': latitude,
        'elevation_m': pd.NA,
    }
    return row, raw_elements


def _network_source(source: str) -> str:
    local_path = Path(source)
    if local_path.is_dir():
        return str(local_path / RO_ANM_NETWORK_FILENAME)
    return source


def _station_source(source: str, station_id: str) -> str:
    local_path = Path(source)
    if local_path.is_dir():
        return str(local_path / RO_ANM_STATION_FILENAME_TEMPLATE.format(station_id=station_id))
    return (
        f'{RO_ANM_WFS_BASE_URL}?service=WFS&version=2.0.0&request=GetFeature'
        f'&typenames=ef:EnvironmentalMonitoringFacility&resourceID=EnvironmentalMonitoringFacility.{station_id}&srsName=EPSG:4326'
    )


def _station_id_from_href(href: str) -> str:
    if not href:
        return ''
    match = re.search(r'EnvironmentalMonitoringFacility\.(\d+)$', href)
    return '' if match is None else match.group(1)


def _clean_station_name(value: object) -> str:
    cleaned = _clean_string(value)
    match = re.search(r'(?:\((?:Meterological|Meteorological) Station (.+)\)|(?:Meterological|Meteorological) Station (.+))$', cleaned)
    if match is None:
        return cleaned
    station_name = match.group(1) or match.group(2) or cleaned
    return station_name.strip()


def _parse_station_coordinates(station: ET.Element) -> tuple[float | None, float | None]:
    pos_text = _clean_string(station.findtext('.//ef:geometry//gml:pos', default='', namespaces=NS))
    if not pos_text:
        return None, None
    parts = pos_text.split()
    if len(parts) != 2:
        return None, None
    longitude = _parse_float(parts[0])
    latitude = _parse_float(parts[1])
    return longitude, latitude


def _parse_station_activity_range(station: ET.Element) -> tuple[str, str]:
    begin_values: list[str] = []
    end_values: list[str] = []
    for period in station.findall('.//ef:operationalActivityPeriod//gml:TimePeriod', NS):
        begin = _normalize_metadata_datetime(period.findtext('gml:beginPosition', default='', namespaces=NS), default='')
        end_node = period.find('gml:endPosition', NS)
        end = _normalize_metadata_datetime(
            '' if end_node is None else end_node.text,
            default='3999-12-31T23:59Z' if end_node is None or end_node.attrib.get('indeterminatePosition') == 'now' else '',
        )
        if begin:
            begin_values.append(begin)
        if end:
            end_values.append(end)
    return (
        min(begin_values) if begin_values else '',
        max(end_values) if end_values else '3999-12-31T23:59Z',
    )


def _normalize_metadata_datetime(value: object, *, default: str) -> str:
    cleaned = _clean_string(value)
    if not cleaned:
        return default
    timestamp = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(timestamp):
        return default
    return timestamp.strftime('%Y-%m-%dT%H:%MZ')


def _parse_measurement_value(node: ET.Element | None, element_raw: str) -> float | None:
    if node is None:
        return None
    cleaned = _clean_string(node.text)
    if cleaned in RO_ANM_MISSING_SENTINELS:
        return None
    numeric = pd.to_numeric(pd.Series([cleaned]), errors='coerce').iloc[0]
    if pd.isna(numeric):
        return None
    return float(RO_ANM_VALUE_CONVERTERS[element_raw](float(numeric)))


def _parse_float(value: object) -> float | None:
    cleaned = _clean_string(value).replace(',', '.')
    if not cleaned:
        return None
    return float(cleaned)


def _clean_string(value: object) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()
