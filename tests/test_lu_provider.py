import unittest
from pathlib import Path
from unittest.mock import patch

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
from weatherdownload.providers.lu.daily import LU_METEOLUX_DAILY_CSV_URL, LU_METEOLUX_WFS_URL
from weatherdownload.providers.lu.parser import (
    LU_NORMALIZED_DAILY_COLUMNS,
    normalize_lu_daily_csv_rows,
    normalize_lu_daily_feature_rows,
    parse_lu_daily_csv_text,
    parse_lu_feature_collection_json,
)


SAMPLE_MAX_TEXT = Path('tests/data/sample_lu_meteolux_maxtemperature.json').read_text(encoding='utf-8')
SAMPLE_MIN_TEXT = Path('tests/data/sample_lu_meteolux_mintemperature.json').read_text(encoding='utf-8')
SAMPLE_PRECIP_TEXT = Path('tests/data/sample_lu_meteolux_totalprecipitation.json').read_text(encoding='utf-8')
SAMPLE_DAILY_CSV_TEXT = Path('tests/data/sample_lu_meteolux_daily_csv.csv').read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class LuxembourgProviderTests(unittest.TestCase):
    def test_registry_discovery_includes_lu_daily_elements(self) -> None:
        self.assertIn('LU', list_supported_countries())
        self.assertEqual(list_providers(country='LU'), ['meteolux'])
        self.assertEqual(list_resolutions(country='LU', provider='meteolux'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='LU', provider='meteolux', resolution='daily'),
            ['tas_max', 'tas_min', 'precipitation', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='LU', provider='meteolux', resolution='daily', provider_raw=True),
            ['maxtemperature', 'mintemperature', 'totalprecipitation', 'DINS'],
        )
        self.assertNotIn('tas_mean', list_supported_elements(country='LU', provider='meteolux', resolution='daily'))
        self.assertNotIn('wind_speed', list_supported_elements(country='LU', provider='meteolux', resolution='daily'))
        self.assertNotIn('vapour_pressure', list_supported_elements(country='LU', provider='meteolux', resolution='daily'))
        self.assertNotIn('open_water_evaporation', list_supported_elements(country='LU', provider='meteolux', resolution='daily'))

    def test_lu_station_metadata_contains_findel_airport(self) -> None:
        stations = read_station_metadata(country='LU')
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['0-20000-0-06590'])
        first = stations.iloc[0]
        self.assertEqual(first['full_name'], 'Luxembourg/Findel Airport')
        self.assertAlmostEqual(float(first['latitude']), 49.63265182)
        self.assertAlmostEqual(float(first['longitude']), 6.232928668)
        self.assertAlmostEqual(float(first['elevation_m']), 376.1)

    def test_lu_station_observation_metadata_lists_daily_raw_elements(self) -> None:
        metadata = read_station_observation_metadata(country='LU')
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['element'].tolist()), ['DINS', 'maxtemperature', 'mintemperature', 'totalprecipitation'])
        self.assertTrue(metadata['description'].str.contains('Findel', case=False).any())
        self.assertTrue(metadata['description'].str.contains('06:00 UTC', case=False).any())

    def test_lu_parser_fixtures_parse_dates_values_station_and_units(self) -> None:
        cases = [
            ('maxtemperature', SAMPLE_MAX_TEXT, 24.8),
            ('mintemperature', SAMPLE_MIN_TEXT, 12.4),
            ('totalprecipitation', SAMPLE_PRECIP_TEXT, 0.0),
        ]
        for raw_code, text, expected_value in cases:
            with self.subTest(raw_code=raw_code):
                payload = parse_lu_feature_collection_json(text)
                normalized = normalize_lu_daily_feature_rows(
                    payload,
                    raw_code=raw_code,
                    provider='meteolux',
                    resolution='daily',
                )
                self.assertEqual(list(normalized.columns), LU_NORMALIZED_DAILY_COLUMNS)
                self.assertEqual(normalized.iloc[0]['station_id'], '0-20000-0-06590')
                self.assertEqual(str(normalized.iloc[0]['observation_date']), '2024-06-01')
                self.assertAlmostEqual(float(normalized.iloc[0]['value']), expected_value)
                self.assertEqual(normalized.iloc[0]['element_raw'], raw_code)

    def test_lu_parser_handles_missing_value_as_nan(self) -> None:
        payload = parse_lu_feature_collection_json(
            """
            {
              "type": "FeatureCollection",
              "features": [
                {
                  "type": "Feature",
                  "properties": {
                    "name_descr": "Findel Airport",
                    "wigos_id": "0-20000-0-06590",
                    "day": "2024-06-01",
                    "maxtemperature": null
                  }
                }
              ]
            }
            """
        )
        normalized = normalize_lu_daily_feature_rows(
            payload,
            raw_code='maxtemperature',
            provider='meteolux',
            resolution='daily',
        )
        self.assertEqual(len(normalized), 1)
        self.assertTrue(normalized['value'].isna().iloc[0])

    def test_lu_parser_handles_empty_feature_collection(self) -> None:
        payload = parse_lu_feature_collection_json('{"type":"FeatureCollection","features":[]}')
        normalized = normalize_lu_daily_feature_rows(
            payload,
            raw_code='maxtemperature',
            provider='meteolux',
            resolution='daily',
        )
        self.assertTrue(normalized.empty)
        self.assertEqual(list(normalized.columns), LU_NORMALIZED_DAILY_COLUMNS)

    def test_lu_parser_skips_unexpected_station_name(self) -> None:
        payload = parse_lu_feature_collection_json(
            """
            {
              "type": "FeatureCollection",
              "features": [
                {
                  "type": "Feature",
                  "properties": {
                    "name_descr": "Somewhere Else",
                    "day": "2024-06-01",
                    "maxtemperature": 20.0
                  }
                }
              ]
            }
            """
        )
        normalized = normalize_lu_daily_feature_rows(
            payload,
            raw_code='maxtemperature',
            provider='meteolux',
            resolution='daily',
        )
        self.assertTrue(normalized.empty)

    def test_lu_parser_accepts_missing_unit_field(self) -> None:
        payload = parse_lu_feature_collection_json(
            """
            {
              "type": "FeatureCollection",
              "features": [
                {
                  "type": "Feature",
                  "properties": {
                    "name_descr": "Findel Airport",
                    "wigos_id": "0-20000-0-06590",
                    "datetime": "2024-06-01T00:00:00Z",
                    "mintemperature": 11.0
                  }
                }
              ]
            }
            """
        )
        normalized = normalize_lu_daily_feature_rows(
            payload,
            raw_code='mintemperature',
            provider='meteolux',
            resolution='daily',
        )
        self.assertEqual(len(normalized), 1)
        self.assertEqual(str(normalized.iloc[0]['observation_date']), '2024-06-01')
        self.assertAlmostEqual(float(normalized.iloc[0]['value']), 11.0)

    def test_lu_parser_uses_datetime_when_day_is_missing(self) -> None:
        payload = parse_lu_feature_collection_json(
            """
            {
              "type": "FeatureCollection",
              "features": [
                {
                  "type": "Feature",
                  "properties": {
                    "name_descr": "Findel Airport",
                    "wigos_id": "0-20000-0-06590",
                    "datetime": "2024-06-01T00:00:00Z",
                    "totalprecipitation": 1.2
                  }
                }
              ]
            }
            """
        )
        normalized = normalize_lu_daily_feature_rows(
            payload,
            raw_code='totalprecipitation',
            provider='meteolux',
            resolution='daily',
        )
        self.assertEqual(str(normalized.iloc[0]['observation_date']), '2024-06-01')
        self.assertAlmostEqual(float(normalized.iloc[0]['value']), 1.2)

    def test_lu_daily_csv_parser_maps_dins_to_sunshine_duration_rows(self) -> None:
        parsed = parse_lu_daily_csv_text(SAMPLE_DAILY_CSV_TEXT)
        self.assertIn('DINS', parsed.columns)
        normalized = normalize_lu_daily_csv_rows(
            parsed,
            raw_code='DINS',
            provider='meteolux',
            resolution='daily',
        )
        self.assertEqual(list(normalized.columns), LU_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(str(normalized.iloc[0]['observation_date']), '2024-06-01')
        self.assertEqual(normalized.iloc[0]['element_raw'], 'DINS')
        self.assertAlmostEqual(float(normalized.iloc[0]['value']), 10.2)
        self.assertTrue(normalized['value'].isna().iloc[1])

    def test_lu_daily_csv_parser_accepts_sep_header_and_comma_decimals(self) -> None:
        csv_text = "sep=;\nDATE;DINS (Hours)\n01.05.2025;10,2\n02.05.2025;\n"
        parsed = parse_lu_daily_csv_text(csv_text)
        normalized = normalize_lu_daily_csv_rows(
            parsed,
            raw_code='DINS',
            provider='meteolux',
            resolution='daily',
        )
        self.assertAlmostEqual(float(normalized.iloc[0]['value']), 10.2)
        self.assertTrue(normalized['value'].isna().iloc[1])

    def test_lu_daily_csv_parser_fails_when_dins_column_is_missing(self) -> None:
        parsed = parse_lu_daily_csv_text("DATE,DXT (degC)\n01.05.2025,25.5\n")
        with self.assertRaises(ValueError):
            normalize_lu_daily_csv_rows(
                parsed,
                raw_code='DINS',
                provider='meteolux',
                resolution='daily',
            )

    def test_lu_daily_downloader_with_mocked_http_returns_public_schema(self) -> None:
        station_metadata = read_station_metadata(country='LU')

        def fake_get(url, params=None, timeout=60):
            if url == LU_METEOLUX_DAILY_CSV_URL:
                self.assertIsNone(params)
                return _MockResponse(SAMPLE_DAILY_CSV_TEXT)
            self.assertEqual(url, LU_METEOLUX_WFS_URL)
            self.assertEqual(params['SERVICE'], 'WFS')
            self.assertEqual(params['VERSION'], '2.0.0')
            self.assertEqual(params['REQUEST'], 'GetFeature')
            self.assertEqual(params['OUTPUTFORMAT'], 'application/json')
            self.assertIn("name_descr = 'Findel Airport'", params['CQL_FILTER'])
            self.assertIn("datetime >= '2024-06-01T00:00:00Z'", params['CQL_FILTER'])
            self.assertIn("datetime < '2024-06-03T00:00:00Z'", params['CQL_FILTER'])
            layer = params['TYPENAMES']
            if layer.endswith('_maxtemperature'):
                return _MockResponse(SAMPLE_MAX_TEXT)
            if layer.endswith('_mintemperature'):
                return _MockResponse(SAMPLE_MIN_TEXT)
            if layer.endswith('_totalprecipitation'):
                return _MockResponse(SAMPLE_PRECIP_TEXT)
            raise AssertionError(f'unexpected layer: {layer}')

        query = ObservationQuery(
            country='LU',
            provider='meteolux',
            resolution='daily',
            station_ids=['0-20000-0-06590'],
            start_date='2024-06-01',
            end_date='2024-06-02',
            elements=['tas_max', 'tas_min', 'precipitation', 'sunshine_duration'],
        )
        with patch('weatherdownload.providers.lu.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='LU', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), LU_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'sunshine_duration', 'tas_max', 'tas_min'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['DINS', 'maxtemperature', 'mintemperature', 'totalprecipitation'])
        self.assertEqual(observations['station_id'].unique().tolist(), ['0-20000-0-06590'])
        self.assertEqual(observations[observations['element'] == 'sunshine_duration']['observation_date'].astype(str).tolist(), ['2024-06-01', '2024-06-02'])

    def test_lu_provider_documentation_and_capabilities_are_updated(self) -> None:
        provider_note = Path('docs/provider_notes/lu_meteolux.md').read_text(encoding='utf-8')
        capabilities = Path('docs/supported_capabilities.md').read_text(encoding='utf-8')

        self.assertIn('MeteoLux Luxembourg', provider_note)
        self.assertIn('Findel Airport', provider_note)
        self.assertIn('not FAO-ready', provider_note)
        self.assertIn('Rn/net radiation is not downloaded', provider_note)
        self.assertIn('sunshine_duration', provider_note)
        self.assertIn('daily CSV', provider_note)
        self.assertIn('| `LU` | `meteolux` | `daily` | `tas_max`, `tas_min`, `precipitation`, `sunshine_duration` |'.replace('`', ''), capabilities.replace('`', ''))


if __name__ == '__main__':
    unittest.main()
