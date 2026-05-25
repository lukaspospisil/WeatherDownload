import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.lu.daily import LU_METEOLUX_WFS_URL
from weatherdownload.providers.lu.parser import (
    LU_NORMALIZED_DAILY_COLUMNS,
    normalize_asta_daily_feature_rows,
    normalize_asta_station_metadata,
    parse_lu_feature_collection_json,
)


SAMPLE_STATION_TEXT = Path('tests/data/sample_lu_asta_station_metadata.json').read_text(encoding='utf-8')
SAMPLE_AVG_TEXT = Path('tests/data/sample_lu_asta_avg_ta200.json').read_text(encoding='utf-8')
SAMPLE_MAX_TEXT = Path('tests/data/sample_lu_asta_max_ta200max.json').read_text(encoding='utf-8')
SAMPLE_MIN_TEXT = Path('tests/data/sample_lu_asta_min_ta200min.json').read_text(encoding='utf-8')
SAMPLE_PRECIP_TEXT = Path('tests/data/sample_lu_asta_sum_nn050.json').read_text(encoding='utf-8')
SAMPLE_WIND_TEXT = Path('tests/data/sample_lu_asta_avg_wv200.json').read_text(encoding='utf-8')
SAMPLE_RH_TEXT = Path('tests/data/sample_lu_asta_avg_rh200.json').read_text(encoding='utf-8')
SAMPLE_SSD_TEXT = Path('tests/data/sample_lu_asta_sum_ssd.json').read_text(encoding='utf-8')


class _MockResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class LuxembourgAstaProviderTests(unittest.TestCase):
    def test_lu_asta_discovery_is_daily_only_and_temperature_only(self) -> None:
        self.assertEqual(list_providers(country='LU'), ['asta', 'meteolux'])
        self.assertEqual(list_resolutions(country='LU', provider='asta'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='LU', provider='asta', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'sunshine_duration'],
        )
        self.assertEqual(
            list_supported_elements(country='LU', provider='asta', resolution='daily', provider_raw=True),
            ['avg_ta200', 'max_ta200max', 'min_ta200min', 'sum_nn050', 'avg_wv200', 'avg_rh200', 'sum_ssd'],
        )
        self.assertNotIn('pressure', list_supported_elements(country='LU', provider='asta', resolution='daily'))
        self.assertNotIn('vapour_pressure', list_supported_elements(country='LU', provider='asta', resolution='daily'))

    def test_asta_station_metadata_parser_keeps_stable_station_ids(self) -> None:
        stations = normalize_asta_station_metadata(parse_lu_feature_collection_json(SAMPLE_STATION_TEXT))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['AGM_012', 'AGM_022'])
        self.assertEqual(stations.iloc[1]['full_name'], 'Arsdorf')
        self.assertAlmostEqual(float(stations.iloc[1]['elevation_m']), 416.0)

    def test_read_station_metadata_country_lu_includes_asta_stations(self) -> None:
        def fake_get(url, params=None, timeout=60):
            self.assertEqual(url, LU_METEOLUX_WFS_URL)
            if params['TYPENAMES'] == 'MF.SpatialSamplingFeature_ASTA':
                return _MockResponse(SAMPLE_STATION_TEXT)
            raise AssertionError(f'unexpected params: {params}')

        with patch('weatherdownload.providers.lu.metadata.requests.get', side_effect=fake_get):
            stations = read_station_metadata(country='LU')

        self.assertIn('AGM_022', stations['station_id'].tolist())
        self.assertIn('Arsdorf', stations['full_name'].tolist())

    def test_lu_asta_observation_metadata_lists_temperature_raw_elements(self) -> None:
        def fake_get(url, params=None, timeout=60):
            self.assertEqual(url, LU_METEOLUX_WFS_URL)
            if params['TYPENAMES'] == 'MF.SpatialSamplingFeature_ASTA':
                return _MockResponse(SAMPLE_STATION_TEXT)
            raise AssertionError(f'unexpected params: {params}')

        with patch('weatherdownload.providers.lu.metadata.requests.get', side_effect=fake_get):
            metadata = read_station_observation_metadata(country='LU')

        asta_metadata = metadata[metadata['station_id'].astype(str).str.startswith('AGM_')].reset_index(drop=True)
        self.assertEqual(
            sorted(asta_metadata['element'].unique().tolist()),
            ['avg_rh200', 'avg_ta200', 'avg_wv200', 'max_ta200max', 'min_ta200min', 'sum_nn050', 'sum_ssd'],
        )

    def test_asta_daily_parser_maps_station_id_dates_and_values(self) -> None:
        payload = parse_lu_feature_collection_json(SAMPLE_AVG_TEXT)
        normalized = normalize_asta_daily_feature_rows(
            payload,
            raw_code='avg_ta200',
            provider='asta',
            resolution='daily',
        )
        self.assertEqual(list(normalized.columns), LU_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(normalized.iloc[0]['station_id'], 'AGM_022')
        self.assertEqual(str(normalized.iloc[0]['observation_date']), '2025-01-02')
        self.assertAlmostEqual(float(normalized.iloc[0]['value']), 1.1)

    def test_asta_daily_parser_maps_precipitation_and_missing_values(self) -> None:
        normalized = normalize_asta_daily_feature_rows(
            parse_lu_feature_collection_json(SAMPLE_PRECIP_TEXT),
            raw_code='sum_nn050',
            provider='asta',
            resolution='daily',
        )
        self.assertEqual(list(normalized.columns), LU_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(normalized['station_id'].tolist(), ['AGM_022', 'AGM_022'])
        self.assertAlmostEqual(float(normalized.iloc[0]['value']), 0.54)
        self.assertTrue(pd.isna(normalized.iloc[1]['value']))

    def test_asta_daily_parser_maps_wind_relative_humidity_and_sunshine(self) -> None:
        wind = normalize_asta_daily_feature_rows(
            parse_lu_feature_collection_json(SAMPLE_WIND_TEXT),
            raw_code='avg_wv200',
            provider='asta',
            resolution='daily',
        )
        rh = normalize_asta_daily_feature_rows(
            parse_lu_feature_collection_json(SAMPLE_RH_TEXT),
            raw_code='avg_rh200',
            provider='asta',
            resolution='daily',
        )
        sunshine = normalize_asta_daily_feature_rows(
            parse_lu_feature_collection_json(SAMPLE_SSD_TEXT),
            raw_code='sum_ssd',
            provider='asta',
            resolution='daily',
        )

        self.assertAlmostEqual(float(wind.iloc[0]['value']), 1.5)
        self.assertAlmostEqual(float(rh.iloc[0]['value']), 77.7)
        self.assertAlmostEqual(float(sunshine.iloc[0]['value']), 3.64)

    def test_asta_daily_parser_handles_empty_or_missing_station_fields(self) -> None:
        empty = normalize_asta_daily_feature_rows(
            {'type': 'FeatureCollection', 'features': []},
            raw_code='avg_rh200',
            provider='asta',
            resolution='daily',
        )
        self.assertTrue(empty.empty)

        malformed_payload = {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'properties': {
                        'day': '2025-01-02',
                        'avg_rh200': 77.7,
                    },
                }
            ],
        }
        malformed = normalize_asta_daily_feature_rows(
            malformed_payload,
            raw_code='avg_rh200',
            provider='asta',
            resolution='daily',
        )
        self.assertTrue(malformed.empty)

    def test_asta_daily_downloader_with_mocked_http_returns_public_schema(self) -> None:
        station_metadata = normalize_asta_station_metadata(parse_lu_feature_collection_json(SAMPLE_STATION_TEXT))

        def fake_get(url, params=None, timeout=60):
            self.assertEqual(url, LU_METEOLUX_WFS_URL)
            layer = params['TYPENAMES']
            self.assertIn("name_descr = 'Arsdorf'", params['CQL_FILTER'])
            self.assertIn("datetime >= '2025-01-02T00:00:00Z'", params['CQL_FILTER'])
            self.assertIn("datetime < '2025-01-04T00:00:00Z'", params['CQL_FILTER'])
            if layer.endswith('_avg_ta200'):
                return _MockResponse(SAMPLE_AVG_TEXT)
            if layer.endswith('_max_ta200max'):
                return _MockResponse(SAMPLE_MAX_TEXT)
            if layer.endswith('_min_ta200min'):
                return _MockResponse(SAMPLE_MIN_TEXT)
            if layer.endswith('_sum_nn050'):
                return _MockResponse(SAMPLE_PRECIP_TEXT)
            if layer.endswith('_avg_wv200'):
                return _MockResponse(SAMPLE_WIND_TEXT)
            if layer.endswith('_avg_rh200'):
                return _MockResponse(SAMPLE_RH_TEXT)
            if layer.endswith('_sum_ssd'):
                return _MockResponse(SAMPLE_SSD_TEXT)
            raise AssertionError(f'unexpected layer: {layer}')

        query = ObservationQuery(
            country='LU',
            provider='asta',
            resolution='daily',
            station_ids=['AGM_022'],
            start_date='2025-01-02',
            end_date='2025-01-03',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'sunshine_duration'],
        )
        with patch('weatherdownload.providers.lu.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='LU', station_metadata=station_metadata)

        self.assertEqual(list(observations.columns), LU_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(
            sorted(observations['element'].unique().tolist()),
            ['precipitation', 'relative_humidity', 'sunshine_duration', 'tas_max', 'tas_mean', 'tas_min', 'wind_speed'],
        )
        self.assertEqual(
            sorted(observations['element_raw'].unique().tolist()),
            ['avg_rh200', 'avg_ta200', 'avg_wv200', 'max_ta200max', 'min_ta200min', 'sum_nn050', 'sum_ssd'],
        )
        self.assertEqual(observations['station_id'].unique().tolist(), ['AGM_022'])

    def test_lu_asta_provider_documentation_and_capabilities_are_updated(self) -> None:
        provider_note = Path('docs/provider_notes/lu_asta.md').read_text(encoding='utf-8')
        capabilities = Path('docs/supported_capabilities.md').read_text(encoding='utf-8')

        self.assertIn('ASTA Luxembourg', provider_note)
        self.assertIn('multi-station Luxembourg agrometeorological network', provider_note)
        self.assertIn('sum_nn050', provider_note)
        self.assertIn('avg_wv200', provider_note)
        self.assertIn('avg_rh200', provider_note)
        self.assertIn('sum_ssd', provider_note)
        self.assertIn('sum_soh', provider_note)
        self.assertIn('Relative Air Pressure', provider_note)
        self.assertIn('not FAO-ready', provider_note)
        self.assertIn('does not directly expose `vapour_pressure`', provider_note)
        self.assertIn(
            '| `LU` | `asta` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `sunshine_duration` |'.replace('`', ''),
            capabilities.replace('`', ''),
        )


if __name__ == '__main__':
    unittest.main()
