import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import get_dataset_spec
from weatherdownload.providers.cz.hourly import NORMALIZED_HOURLY_COLUMNS, build_hourly_download_targets, normalize_hourly_observations, parse_hourly_csv
from weatherdownload.metadata import _parse_station_metadata_csv
from weatherdownload.queries import ObservationQuery


SAMPLE_HOURLY_CSV = Path('tests/data/sample_hourly_e_202401.csv').read_text(encoding='utf-8')
SAMPLE_HOURLY_P_CSV = Path('tests/data/sample_hourly_p_202401.csv').read_text(encoding='utf-8')
SAMPLE_HOURLY_SSV1H_CSV = Path('tests/data/sample_hourly_ssv1h_202401.csv').read_text(encoding='utf-8')
SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')


class HourlyObservationTests(unittest.TestCase):
    def test_query_to_download_mapping(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-31T23:00:00Z', elements=['vapour_pressure'])
        targets = build_hourly_download_targets(query)
        self.assertEqual(len(targets), 1)
        self.assertTrue(targets[0].url.endswith('/humidity/2024/1h-0-20000-0-11406-E-202401.csv'))

    def test_hourly_mapping_supports_multiple_groups(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-31T23:00:00Z', elements=['pressure', 'sunshine_duration'])
        targets = build_hourly_download_targets(query)
        self.assertEqual(len(targets), 2)
        self.assertTrue(targets[0].url.endswith('/synop/2024/1h-0-20000-0-11406-P-202401.csv'))
        self.assertTrue(targets[1].url.endswith('/sunshine/2024/1h-0-20000-0-11406-SSV1H-202401.csv'))

    def test_hourly_mapping_uses_registry_endpoint_pattern(self) -> None:
        spec = get_dataset_spec('historical_csv', '1hour')
        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-31T23:00:00Z', elements=['vapour_pressure'])
        target = build_hourly_download_targets(query)[0]
        expected = spec.endpoint_pattern.format(group=spec.element_groups['E'], year='2024', station_id='0-20000-0-11406', element='E', year_month='202401')
        self.assertEqual(target.url, expected)

    def test_hourly_all_history_builds_targets_from_directory_listing(self) -> None:
        def fake_get(url: str, timeout: int = 60):
            class _Response:
                def __init__(self, text: str) -> None:
                    self.text = text

                def raise_for_status(self) -> None:
                    return None

            if url.endswith('/humidity/'):
                return _Response('<a href="2023/">2023/</a><a href="2024/">2024/</a>')
            if url.endswith('/humidity/2023/'):
                return _Response('<a href="1h-0-20000-0-11406-E-202312.csv">202312</a>')
            if url.endswith('/humidity/2024/'):
                return _Response('<a href="1h-0-20000-0-11406-E-202401.csv">202401</a>')
            raise AssertionError(f'unexpected URL: {url}')

        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], all_history=True, elements=['vapour_pressure'])
        with patch('weatherdownload.providers.cz.hourly.requests.get', side_effect=fake_get):
            targets = build_hourly_download_targets(query)
        self.assertEqual(len(targets), 2)
        self.assertEqual([target.year_month for target in targets], ['202312', '202401'])

    def test_parse_representative_hourly_sample(self) -> None:
        parsed = parse_hourly_csv(SAMPLE_HOURLY_CSV)
        self.assertEqual(list(parsed.columns), ['STATION', 'ELEMENT', 'DT', 'VALUE', 'FLAG', 'QUALITY'])
        self.assertEqual(len(parsed), 3)

    def test_normalized_hourly_output_columns(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['vapour_pressure'])
        parsed = parse_hourly_csv(SAMPLE_HOURLY_CSV)
        metadata = _parse_station_metadata_csv(SAMPLE_META1)
        normalized = normalize_hourly_observations(parsed, query, station_metadata=metadata)
        self.assertEqual(list(normalized.columns), NORMALIZED_HOURLY_COLUMNS)
        self.assertEqual(normalized.iloc[0]['station_id'], '0-20000-0-11406')
        self.assertEqual(normalized.iloc[0]['gh_id'], 'L3CHEB01')
        self.assertEqual(normalized.iloc[0]['element'], 'vapour_pressure')
        self.assertEqual(normalized.iloc[0]['element_raw'], 'E')
        self.assertEqual(str(normalized.iloc[0]['timestamp']), '2024-01-01 00:00:00+00:00')

    def test_normalized_hourly_supports_synop_element(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['pressure'])
        parsed = parse_hourly_csv(SAMPLE_HOURLY_P_CSV)
        normalized = normalize_hourly_observations(parsed, query)
        self.assertEqual(normalized['element'].tolist(), ['pressure', 'pressure', 'pressure'])
        self.assertEqual(normalized['element_raw'].tolist(), ['P', 'P', 'P'])

    def test_normalized_hourly_supports_sunshine_element(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['sunshine_duration'])
        parsed = parse_hourly_csv(SAMPLE_HOURLY_SSV1H_CSV)
        normalized = normalize_hourly_observations(parsed, query)
        self.assertEqual(normalized['element'].tolist(), ['sunshine_duration', 'sunshine_duration', 'sunshine_duration'])
        self.assertEqual(normalized['element_raw'].tolist(), ['SSV1H', 'SSV1H', 'SSV1H'])

    def test_hourly_gh_id_is_nullable_without_metadata(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T02:00:00Z', elements=['vapour_pressure'])
        parsed = parse_hourly_csv(SAMPLE_HOURLY_CSV)
        normalized = normalize_hourly_observations(parsed, query)
        self.assertTrue(normalized['gh_id'].isna().all())


if __name__ == '__main__':
    unittest.main()

