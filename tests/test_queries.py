import unittest
from datetime import date

from weatherdownload import ObservationQuery, QueryValidationError, list_dataset_scopes, list_resolutions, list_supported_elements, validate_observation_query


class DiscoveryTests(unittest.TestCase):
    def test_list_dataset_scopes_contains_historical_csv(self) -> None:
        self.assertIn('historical_csv', list_dataset_scopes())

    def test_list_resolutions_for_scope(self) -> None:
        self.assertIn('daily', list_resolutions('historical_csv'))
        self.assertIsInstance(list_resolutions('now'), list)
        self.assertIn('10min', list_resolutions('now'))

    def test_list_supported_elements_for_daily_historical_csv(self) -> None:
        elements = list_supported_elements(resolution='daily', dataset_scope='historical_csv')
        self.assertEqual(elements, ['HS', 'P', 'RH', 'SRA', 'SSV', 'T', 'TMA', 'TMI', 'WDIR', 'WSPD'])


class ObservationQueryValidationTests(unittest.TestCase):
    def test_query_normalizes_station_ids_and_elements(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=[' 0-20000-0-11406 ', '0-20000-0-11406', '0-20000-0-11414'], start_date='2024-01-01', end_date='2024-12-31', elements=[' tma ', 'TMI', 'tma'])
        self.assertEqual(query.station_ids, ['0-20000-0-11406', '0-20000-0-11414'])
        self.assertEqual(query.elements, ['TMA', 'TMI'])

    def test_valid_but_not_implemented_combination_is_still_query_valid(self) -> None:
        query = ObservationQuery(dataset_scope='now', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T01:00:00Z')
        self.assertEqual(query.dataset_scope, 'now')
        self.assertEqual(query.resolution, '10min')

    def test_query_rejects_mixed_date_and_datetime_ranges(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-31T00:00:00Z', start_date='2024-01-01', end_date='2024-01-31')

    def test_daily_query_rejects_datetime_precision(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-31T00:00:00Z')

    def test_validate_observation_query_returns_query(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date=date(2024, 1, 1), end_date=date(2024, 12, 31))
        validated = validate_observation_query(query)
        self.assertIs(validated, query)


if __name__ == '__main__':
    unittest.main()
