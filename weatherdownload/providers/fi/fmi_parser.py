from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from xml.etree import ElementTree as ET

import pandas as pd


FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS = [
    'station_id',
    'gh_id',
    'element',
    'element_raw',
    'timestamp',
    'value',
    'flag',
    'quality',
    'provider',
    'resolution',
]

FMI_TIMEVALUEPAIR_RAW_TO_CANONICAL = {
    't2m': 'tas_mean',
    'ws_10min': 'wind_speed',
}


def normalize_fmi_timevaluepair_hourly_observations(
    xml_text: str,
    *,
    provider: str = 'fmi',
    resolution: str = '1hour',
) -> pd.DataFrame:
    """
    Parse FMI Open Data WFS timevaluepair XML and normalize to WeatherDownload's
    subdaily observation schema.

    Notes:
    - Units are preserved in `frame.attrs["units_by_element_raw"]` when present
      in the payload (`uom` attributes on values).
    - This is parser-only POC code; it does not perform unit conversions.
    """
    if not xml_text.strip():
        return pd.DataFrame(columns=FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise ValueError('FMI timevaluepair XML is not valid XML.') from exc

    rows: list[dict[str, object]] = []
    units_by_raw: dict[str, str] = {}
    station_id_candidates: set[str] = set()

    for observation in _iter_observations(root):
        raw_element = _extract_observed_property_code(observation)
        if not raw_element:
            continue
        station_id = _extract_fmisid(observation)
        if station_id:
            station_id_candidates.add(station_id)

        canonical = FMI_TIMEVALUEPAIR_RAW_TO_CANONICAL.get(raw_element)
        if canonical is None:
            # POC is intentionally conservative.
            continue

        for timestamp, value, unit in _iter_time_value_pairs(observation):
            if timestamp is None:
                continue
            if value is None:
                # Missing values are dropped (consistent with other normalizers).
                continue
            if unit and raw_element not in units_by_raw:
                units_by_raw[raw_element] = unit
            rows.append(
                {
                    'station_id': station_id or '',
                    'gh_id': pd.NA,
                    'element': canonical,
                    'element_raw': raw_element,
                    'timestamp': timestamp,
                    'value': value,
                    'flag': pd.NA,
                    'quality': pd.NA,
                    'provider': provider,
                    'resolution': resolution,
                }
            )

    if not rows:
        frame = pd.DataFrame(columns=FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)
        frame.attrs['units_by_element_raw'] = units_by_raw
        frame.attrs['station_id_candidates'] = sorted(station_id_candidates)
        return frame

    frame = pd.DataFrame.from_records(rows)
    frame['station_id'] = frame['station_id'].astype('string').str.strip()
    frame = frame[frame['station_id'].ne('')]
    frame['timestamp'] = pd.to_datetime(frame['timestamp'], utc=True, errors='coerce')
    frame = frame[frame['timestamp'].notna()]
    frame['value'] = pd.to_numeric(frame['value'], errors='coerce')
    frame = frame[frame['value'].notna()]
    if frame.empty:
        empty = pd.DataFrame(columns=FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS)
        empty.attrs['units_by_element_raw'] = units_by_raw
        empty.attrs['station_id_candidates'] = sorted(station_id_candidates)
        return empty

    frame = frame.sort_values(['station_id', 'timestamp', 'element'], kind='stable').reset_index(drop=True)
    frame = frame.loc[:, FMI_TIMEVALUEPAIR_NORMALIZED_COLUMNS]
    frame.attrs['units_by_element_raw'] = units_by_raw
    frame.attrs['station_id_candidates'] = sorted(station_id_candidates)
    return frame


def _iter_observations(root: ET.Element) -> Iterable[ET.Element]:
    # FMI timevaluepair responses are WFS FeatureCollections containing
    # om:OM_Observation (or similar) members.
    for elem in root.iter():
        if _localname(elem.tag) in {'OM_Observation', 'PointTimeSeriesObservation'}:
            yield elem


def _extract_observed_property_code(observation: ET.Element) -> str:
    # Expected pattern: <om:observedProperty xlink:href=".../t2m"/>
    for elem in observation.iter():
        if _localname(elem.tag) != 'observedProperty':
            continue
        href = _attr_by_localname(elem, 'href')
        if not href:
            continue
        last = href.rsplit('/', 1)[-1].strip()
        return last
    return ''


def _extract_fmisid(observation: ET.Element) -> str:
    # Prefer <gml:identifier codeSpace="...fmisid">100971</gml:identifier>
    for elem in observation.iter():
        if _localname(elem.tag) != 'identifier':
            continue
        codespace = _attr_by_localname(elem, 'codeSpace')
        if codespace and 'fmisid' in codespace.lower():
            value = (elem.text or '').strip()
            return value

    # Fallback: any element or attribute containing "fmisid".
    for elem in observation.iter():
        if 'fmisid' in _localname(elem.tag).lower():
            value = (elem.text or '').strip()
            if value:
                return value
        for key, val in elem.attrib.items():
            if 'fmisid' in _localname(key).lower():
                cleaned = str(val).strip()
                if cleaned:
                    return cleaned
    return ''


def _iter_time_value_pairs(observation: ET.Element) -> Iterable[tuple[datetime | None, float | None, str]]:
    # Expected pattern (WaterML2):
    # wml2:MeasurementTimeseries / wml2:point / wml2:MeasurementTVP / wml2:time, wml2:value
    for tvp in observation.iter():
        if _localname(tvp.tag) != 'MeasurementTVP':
            continue
        time_text = ''
        value_text = None
        unit = ''
        nil = False
        for child in list(tvp):
            name = _localname(child.tag)
            if name == 'time':
                time_text = (child.text or '').strip()
            elif name == 'value':
                value_text = (child.text or '').strip()
                unit = _attr_by_localname(child, 'uom')
                nil = _attr_by_localname(child, 'nil') in {'true', '1'}

        timestamp = _parse_timestamp(time_text)
        if value_text is None or value_text == '' or nil:
            yield (timestamp, None, unit or '')
            continue
        value = _parse_float(value_text)
        yield (timestamp, value, unit or '')


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    parsed = pd.to_datetime(value, utc=True, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


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


def _attr_by_localname(elem: ET.Element, local: str) -> str:
    for key, value in elem.attrib.items():
        if _localname(key) == local:
            return str(value).strip()
    return ''

