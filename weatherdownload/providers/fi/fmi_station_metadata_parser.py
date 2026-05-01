from __future__ import annotations

from xml.etree import ElementTree as ET

import pandas as pd

from ...metadata import STATION_METADATA_COLUMNS


def parse_fmi_station_feature_collection(xml_text: str) -> pd.DataFrame:
    """
    Parse FMI Open Data WFS `fmi::ef::stations` response (WFS FeatureCollection)
    and normalize to WeatherDownload station metadata schema.
    """
    if not xml_text.strip():
        return pd.DataFrame(columns=STATION_METADATA_COLUMNS)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError('FMI station metadata response is not valid XML.') from exc

    rows: list[dict[str, object]] = []
    for facility in _iter_facilities(root):
        station_id = _extract_fmisid(facility)
        if not station_id:
            continue
        name = _extract_station_name(facility)
        latitude, longitude = _extract_lat_lon(facility)
        begin_date, end_date = _extract_activity_period(facility)
        rows.append(
            {
                'station_id': station_id,
                'gh_id': pd.NA,
                'begin_date': begin_date,
                'end_date': end_date,
                'full_name': name or pd.NA,
                'longitude': longitude,
                'latitude': latitude,
                'elevation_m': pd.NA,
            }
        )

    frame = pd.DataFrame.from_records(rows, columns=STATION_METADATA_COLUMNS)
    if frame.empty:
        return frame
    frame['station_id'] = frame['station_id'].astype('string').str.strip()
    frame = frame[frame['station_id'].ne('')].drop_duplicates(subset=['station_id']).reset_index(drop=True)
    frame = frame.sort_values('station_id', kind='stable').reset_index(drop=True)
    return frame


def _iter_facilities(root: ET.Element):
    for elem in root.iter():
        if _localname(elem.tag) == 'EnvironmentalMonitoringFacility':
            yield elem


def _extract_fmisid(facility: ET.Element) -> str:
    for elem in facility.iter():
        if _localname(elem.tag) != 'identifier':
            continue
        codespace = _attr_by_localname(elem, 'codeSpace').lower()
        if 'fmisid' not in codespace:
            continue
        value = (elem.text or '').strip()
        if value:
            return value
    return ''


def _extract_station_name(facility: ET.Element) -> str:
    # Prefer ef:name, fall back to gml:name with locationcode/name.
    for elem in facility.iter():
        if _localname(elem.tag) == 'name' and _namespace_hint(elem.tag) == 'ef':
            value = (elem.text or '').strip()
            if value:
                return value
    for elem in facility.iter():
        if _localname(elem.tag) != 'name':
            continue
        codespace = _attr_by_localname(elem, 'codeSpace').lower()
        if 'locationcode/name' in codespace:
            value = (elem.text or '').strip()
            if value:
                return value
    return ''


def _extract_lat_lon(facility: ET.Element) -> tuple[float | None, float | None]:
    # ef:representativePoint/gml:Point/gml:pos -> "lat lon"
    for elem in facility.iter():
        if _localname(elem.tag) != 'representativePoint':
            continue
        for pos in elem.iter():
            if _localname(pos.tag) != 'pos':
                continue
            parts = (pos.text or '').strip().split()
            if len(parts) != 2:
                continue
            lat = _parse_float(parts[0])
            lon = _parse_float(parts[1])
            return lat, lon
    return None, None


def _extract_activity_period(facility: ET.Element) -> tuple[str, str]:
    begin = ''
    end = ''
    for elem in facility.iter():
        if _localname(elem.tag) == 'beginPosition' and not begin:
            begin = _format_datetime((elem.text or '').strip())
        if _localname(elem.tag) == 'endPosition' and not end:
            indeterminate = _attr_by_localname(elem, 'indeterminatePosition').lower()
            if indeterminate:
                # Keep open-ended periods as empty end_date.
                end = ''
            else:
                end = _format_datetime((elem.text or '').strip())
    return begin, end


def _format_datetime(value: str) -> str:
    if not value:
        return ''
    ts = pd.to_datetime(value, utc=True, errors='coerce')
    if pd.isna(ts):
        return ''
    return ts.strftime('%Y-%m-%dT%H:%MZ')


def _parse_float(value: str) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _localname(tag: str) -> str:
    if not isinstance(tag, str):
        return ''
    if tag.startswith('{'):
        return tag.split('}', 1)[1]
    return tag


def _namespace_hint(tag: str) -> str:
    # Best-effort hint based on URI fragments.
    if not isinstance(tag, str) or not tag.startswith('{'):
        return ''
    uri = tag[1:].split('}', 1)[0]
    if uri.endswith('/ef/4.0'):
        return 'ef'
    return ''


def _attr_by_localname(elem: ET.Element, local: str) -> str:
    for key, value in elem.attrib.items():
        if _localname(key) == local:
            return str(value).strip()
    return ''

