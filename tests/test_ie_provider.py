import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.ie.daily import IE_METEIREANN_DAILY_CSV_URL_TEMPLATE
from weatherdownload.providers.ie.parser import (
    IE_NORMALIZED_DAILY_COLUMNS,
    KNOT_TO_M_S,
    load_ie_audited_stations,
    normalize_ie_daily_rows,
    normalize_ie_station_metadata,
    parse_ie_station_details_csv,
    parse_ie_daily_csv_text,
)


SAMPLE_DAILY_TEXT = Path('tests/data/sample_ie_meteireann_dly532.csv').read_text(encoding='utf-8')
SAMPLE_CORK_TEXT = SAMPLE_DAILY_TEXT.replace('Dublin Airport', 'Cork Airport', 1).replace('532', '3904')
SAMPLE_STATION_DETAILS_TEXT = Path('tests/data/sample_ie_station_details.csv').read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class IrelandProviderTests(unittest.TestCase):
    def test_registry_discovery_includes_ie_daily_elements(self) -> None:
        self.assertIn('IE', list_supported_countries())
        self.assertEqual(list_providers(country='IE'), ['meteireann'])
        self.assertEqual(list_resolutions(country='IE', provider='meteireann'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='IE', provider='meteireann', resolution='daily'),
            ['tas_max', 'tas_min', 'precipitation', 'wind_speed', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='IE', provider='meteireann', resolution='daily', provider_raw=True),
            ['maxtp', 'mintp', 'rain', 'wdsp', 'sun'],
        )
        self.assertNotIn('tas_mean', list_supported_elements(country='IE', provider='meteireann', resolution='daily'))
        self.assertNotIn('relative_humidity', list_supported_elements(country='IE', provider='meteireann', resolution='daily'))
        self.assertNotIn('vapour_pressure', list_supported_elements(country='IE', provider='meteireann', resolution='daily'))
        self.assertNotIn('pressure', list_supported_elements(country='IE', provider='meteireann', resolution='daily'))

    def test_ie_station_metadata_contains_dublin_airport(self) -> None:
        stations = read_station_metadata(country='IE')
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertGreater(len(stations), 6)
        dublin = stations[stations['station_id'] == '532'].iloc[0]
        self.assertEqual(dublin['full_name'], 'Dublin Airport')
        self.assertAlmostEqual(float(dublin['latitude']), 53.42778)
        self.assertAlmostEqual(float(dublin['longitude']), -6.24083)
        self.assertAlmostEqual(float(dublin['elevation_m']), 71.0)
        self.assertIn('3904', stations['station_id'].tolist())

    def test_ie_station_details_parser_keeps_multiple_verified_stations(self) -> None:
        parsed = parse_ie_station_details_csv(SAMPLE_STATION_DETAILS_TEXT)
        stations = normalize_ie_station_metadata(parsed)
        self.assertEqual(stations['station_id'].tolist(), ['1575', '2275', '2375', '3723', '3904', '4935', '518', '532'])
        self.assertEqual(stations[stations['station_id'] == '3904'].iloc[0]['full_name'], 'Cork Airport')
        self.assertEqual(stations[stations['station_id'] == '518'].iloc[0]['full_name'], 'Shannon Airport')
        self.assertEqual(stations[stations['station_id'] == '532'].iloc[0]['begin_date'], '1939-01-01T00:00Z')

    def test_ie_audited_station_list_is_used_and_has_more_than_six_stations(self) -> None:
        audited = load_ie_audited_stations()
        self.assertGreater(len(audited), 6)
        self.assertIn('532', audited['station_id'].tolist())
        self.assertTrue(audited['full_name'].notna().all())
        self.assertTrue(audited['latitude'].notna().all())
        self.assertTrue(audited['longitude'].notna().all())

    def test_ie_station_observation_metadata_lists_daily_raw_elements(self) -> None:
        metadata = read_station_observation_metadata(country='IE')
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata[metadata['station_id'] == '532']['element'].tolist()), ['maxtp', 'mintp', 'rain', 'sun', 'wdsp'])
        self.assertTrue(metadata['description'].str.contains('Met Eireann', case=False).any())
        self.assertTrue(metadata['schedule'].str.contains('00 UTC', case=False).any())

    def test_ie_daily_csv_parser_maps_values_and_converts_wind_speed(self) -> None:
        parsed = parse_ie_daily_csv_text(SAMPLE_DAILY_TEXT)
        self.assertEqual(parsed.metadata['station_name'], 'Dublin Airport')

        tas_max = normalize_ie_daily_rows(
            parsed,
            raw_code='maxtp',
            provider='meteireann',
            resolution='daily',
            station_id='532',
            expected_station_name='Dublin Airport',
        )
        wind = normalize_ie_daily_rows(
            parsed,
            raw_code='wdsp',
            provider='meteireann',
            resolution='daily',
            station_id='532',
            expected_station_name='Dublin Airport',
        )
        self.assertEqual(list(tas_max.columns), IE_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(str(tas_max.iloc[0]['observation_date']), '2024-01-01')
        self.assertAlmostEqual(float(tas_max.iloc[0]['value']), 10.2)
        self.assertAlmostEqual(float(wind.iloc[0]['value']), 12.0 * KNOT_TO_M_S, places=6)

    def test_ie_daily_csv_parser_handles_missing_value_and_empty_rows(self) -> None:
        parsed = parse_ie_daily_csv_text(SAMPLE_DAILY_TEXT)
        rain = normalize_ie_daily_rows(
            parsed,
            raw_code='rain',
            provider='meteireann',
            resolution='daily',
            station_id='532',
        )
        self.assertAlmostEqual(float(rain.iloc[0]['value']), 5.6)
        self.assertTrue(pd.isna(rain.iloc[1]['value']))

        empty = normalize_ie_daily_rows(
            parse_ie_daily_csv_text("Station Name,Dublin Airport\ndate,maxtp\n"),
            raw_code='maxtp',
            provider='meteireann',
            resolution='daily',
            station_id='532',
        )
        self.assertTrue(empty.empty)

    def test_ie_daily_csv_parser_rejects_unexpected_station_name(self) -> None:
        parsed = parse_ie_daily_csv_text(SAMPLE_DAILY_TEXT.replace('Dublin Airport', 'Cork Airport', 1))
        with self.assertRaises(ValueError):
            normalize_ie_daily_rows(
                parsed,
                raw_code='maxtp',
                provider='meteireann',
                resolution='daily',
                station_id='532',
                expected_station_name='Dublin Airport',
            )

    def test_ie_daily_csv_parser_rejects_missing_required_column(self) -> None:
        parsed = parse_ie_daily_csv_text("Station Name,Dublin Airport\ndate,rain\n2024/01/01,1.2\n")
        with self.assertRaises(ValueError):
            normalize_ie_daily_rows(
                parsed,
                raw_code='maxtp',
                provider='meteireann',
                resolution='daily',
                station_id='532',
            )

    def test_ie_daily_downloader_with_mocked_http_returns_public_schema(self) -> None:
        def fake_get(url, timeout=60):
            if url == IE_METEIREANN_DAILY_CSV_URL_TEMPLATE.format(station_id='532'):
                return _MockResponse(SAMPLE_DAILY_TEXT)
            if url == IE_METEIREANN_DAILY_CSV_URL_TEMPLATE.format(station_id='3904'):
                return _MockResponse(SAMPLE_CORK_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(
            country='IE',
            provider='meteireann',
            resolution='daily',
            station_ids=['532', '3904'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_max'],
        )
        with patch('weatherdownload.providers.ie.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='IE', station_metadata=read_station_metadata(country='IE', source_url='tests/data/sample_ie_station_details.csv'))

        self.assertEqual(list(observations.columns), IE_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['tas_max'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['maxtp'])
        self.assertEqual(sorted(observations['station_id'].unique().tolist()), ['3904', '532'])
        self.assertEqual(observations['observation_date'].astype(str).tolist().count('2024-01-01'), 2)
        self.assertEqual(observations['observation_date'].astype(str).tolist().count('2024-01-02'), 2)

    def test_ie_daily_csv_url_construction_uses_requested_station_id(self) -> None:
        requested_urls: list[str] = []

        def fake_get(url, timeout=60):
            requested_urls.append(url)
            if url.endswith('dly532.csv'):
                return _MockResponse(SAMPLE_DAILY_TEXT)
            if url.endswith('dly3904.csv'):
                return _MockResponse(SAMPLE_CORK_TEXT)
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(
            country='IE',
            provider='meteireann',
            resolution='daily',
            station_ids=['532', '3904'],
            start_date='2024-01-01',
            end_date='2024-01-01',
            elements=['precipitation'],
        )
        with patch('weatherdownload.providers.ie.daily.requests.get', side_effect=fake_get):
            download_observations(query, country='IE', station_metadata=read_station_metadata(country='IE', source_url='tests/data/sample_ie_station_details.csv'))

        self.assertIn(IE_METEIREANN_DAILY_CSV_URL_TEMPLATE.format(station_id='532'), requested_urls)
        self.assertIn(IE_METEIREANN_DAILY_CSV_URL_TEMPLATE.format(station_id='3904'), requested_urls)

    def test_ie_provider_documentation_and_capabilities_are_updated(self) -> None:
        provider_note = Path('docs/provider_notes/ie_meteireann.md').read_text(encoding='utf-8')
        capabilities = Path('docs/supported_capabilities.md').read_text(encoding='utf-8')

        self.assertIn('Met Eireann Ireland', provider_note)
        self.assertIn('Dublin Airport', provider_note)
        self.assertIn('dly532.csv', provider_note)
        self.assertIn('multi-station', provider_note)
        self.assertIn('station ids are Met Eireann daily station numbers', provider_note)
        self.assertIn('audited validated daily station set', provider_note)
        self.assertIn('wdsp', provider_note)
        self.assertIn('knots', provider_note)
        self.assertIn('not FAO-ready', provider_note)
        self.assertIn('no derived values', provider_note)
        self.assertIn(
            '| `IE` | `meteireann` | `daily` | `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `sunshine_duration` |'.replace('`', ''),
            capabilities.replace('`', ''),
        )


if __name__ == '__main__':
    unittest.main()
