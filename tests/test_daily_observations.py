import unittest
from pathlib import Path

from weatherdownload import get_dataset_spec
from weatherdownload.chmi_daily import NORMALIZED_DAILY_COLUMNS, build_daily_download_targets, normalize_daily_observations, parse_daily_csv
from weatherdownload.metadata import _parse_station_metadata_csv
from weatherdownload.queries import ObservationQuery


SAMPLE_DAILY_CSV = Path('tests/data/sample_daily_tma.csv').read_text(encoding='utf-8')
SAMPLE_META1 = Path('tests/data/sample_meta1.csv').read_text(encoding='utf-8')


class DailyObservationTests(unittest.TestCase):
    def test_query_to_download_mapping(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date='1865-06-01', end_date='1865-06-03', elements=['tas_max', 'precipitation'])
        targets = build_daily_download_targets(query)
        self.assertEqual(len(targets), 2)
        self.assertTrue(targets[0].url.endswith('/temperature/dly-0-20000-0-11406-TMA.csv'))
        self.assertTrue(targets[1].url.endswith('/precipitation/dly-0-20000-0-11406-SRA.csv'))

    def test_daily_mapping_uses_registry_endpoint_pattern(self) -> None:
        spec = get_dataset_spec('historical_csv', 'daily')
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date='1865-06-01', end_date='1865-06-03', elements=['tas_max'])
        target = build_daily_download_targets(query)[0]
        expected = spec.endpoint_pattern.format(group=spec.element_groups['TMA'], station_id='0-20000-0-11406', element='TMA')
        self.assertEqual(target.url, expected)

    def test_parse_representative_daily_sample(self) -> None:
        parsed = parse_daily_csv(SAMPLE_DAILY_CSV)
        self.assertEqual(list(parsed.columns), ['STATION', 'ELEMENT', 'TIMEFUNC', 'DT', 'VALUE', 'FLAG', 'QUALITY'])
        self.assertEqual(len(parsed), 3)

    def test_normalized_output_columns(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date='1865-06-01', end_date='1865-06-03', elements=['tas_max'])
        parsed = parse_daily_csv(SAMPLE_DAILY_CSV)
        metadata = _parse_station_metadata_csv(SAMPLE_META1)
        normalized = normalize_daily_observations(parsed, query, station_metadata=metadata)
        self.assertEqual(list(normalized.columns), NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(normalized.iloc[0]['station_id'], '0-20000-0-11406')
        self.assertEqual(normalized.iloc[0]['gh_id'], 'L3CHEB01')
        self.assertEqual(normalized.iloc[0]['element'], 'tas_max')
        self.assertEqual(normalized.iloc[0]['element_raw'], 'TMA')
        self.assertEqual(str(normalized.iloc[0]['observation_date']), '1865-06-01')


if __name__ == '__main__':
    unittest.main()
