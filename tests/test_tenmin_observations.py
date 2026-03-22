import unittest
from pathlib import Path

from weatherdownload import get_dataset_spec
from weatherdownload.chmi_tenmin import NORMALIZED_TENMIN_COLUMNS, build_tenmin_download_targets, normalize_tenmin_observations, parse_tenmin_csv
from weatherdownload.metadata import _parse_station_metadata_csv
from weatherdownload.queries import ObservationQuery


SAMPLE_TENMIN_CSV = Path('tests/data/sample_10min_t_202401.csv').read_text(encoding='utf-8')
SAMPLE_TENMIN_T10_CSV = Path('tests/data/sample_10min_t10_202401.csv').read_text(encoding='utf-8')
SAMPLE_TENMIN_SSV10M_CSV = Path('tests/data/sample_10min_ssv10m_202401.csv').read_text(encoding='utf-8')
SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')


class TenMinObservationTests(unittest.TestCase):
    def test_query_to_download_mapping(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['T'])
        targets = build_tenmin_download_targets(query)
        self.assertEqual(len(targets), 1)
        self.assertTrue(targets[0].url.endswith('/temperature/2024/10m-0-20000-0-11406-T-202401.csv'))

    def test_tenmin_mapping_supports_multiple_groups(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['T10', 'SSV10M'])
        targets = build_tenmin_download_targets(query)
        self.assertEqual(len(targets), 2)
        self.assertTrue(targets[0].url.endswith('/soil_temperature/2024/10m-0-20000-0-11406-T10-202401.csv'))
        self.assertTrue(targets[1].url.endswith('/sunshine/2024/10m-0-20000-0-11406-SSV10M-202401.csv'))

    def test_tenmin_mapping_uses_registry_endpoint_pattern(self) -> None:
        spec = get_dataset_spec('historical_csv', '10min')
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['T'])
        target = build_tenmin_download_targets(query)[0]
        expected = spec.endpoint_pattern.format(group=spec.element_groups['T'], year='2024', station_id='0-20000-0-11406', element='T', year_month='202401')
        self.assertEqual(target.url, expected)

    def test_parse_representative_tenmin_sample(self) -> None:
        parsed = parse_tenmin_csv(SAMPLE_TENMIN_CSV)
        self.assertEqual(list(parsed.columns), ['STATION', 'ELEMENT', 'DT', 'VALUE', 'FLAG', 'QUALITY'])
        self.assertEqual(len(parsed), 3)

    def test_normalized_tenmin_output_columns(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['T'])
        parsed = parse_tenmin_csv(SAMPLE_TENMIN_CSV)
        metadata = _parse_station_metadata_csv(SAMPLE_META1)
        normalized = normalize_tenmin_observations(parsed, query, station_metadata=metadata)
        self.assertEqual(list(normalized.columns), NORMALIZED_TENMIN_COLUMNS)
        self.assertEqual(normalized.iloc[0]['station_id'], '0-20000-0-11406')
        self.assertEqual(normalized.iloc[0]['gh_id'], 'L3CHEB01')
        self.assertEqual(str(normalized.iloc[0]['timestamp']), '2024-01-01 00:00:00+00:00')

    def test_normalized_tenmin_supports_soil_temperature_element(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['T10'])
        parsed = parse_tenmin_csv(SAMPLE_TENMIN_T10_CSV)
        normalized = normalize_tenmin_observations(parsed, query)
        self.assertEqual(normalized['element'].tolist(), ['T10', 'T10', 'T10'])

    def test_normalized_tenmin_supports_sunshine_element(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['SSV10M'])
        parsed = parse_tenmin_csv(SAMPLE_TENMIN_SSV10M_CSV)
        normalized = normalize_tenmin_observations(parsed, query)
        self.assertEqual(normalized['element'].tolist(), ['SSV10M', 'SSV10M', 'SSV10M'])

    def test_tenmin_gh_id_is_nullable_without_metadata(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T00:20:00Z', elements=['T'])
        parsed = parse_tenmin_csv(SAMPLE_TENMIN_CSV)
        normalized = normalize_tenmin_observations(parsed, query)
        self.assertTrue(normalized['gh_id'].isna().all())


if __name__ == '__main__':
    unittest.main()
