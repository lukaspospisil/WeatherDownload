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

    def test_list_supported_elements_for_daily_historical_csv_returns_canonical_names(self) -> None:
        elements = list_supported_elements(resolution='daily', dataset_scope='historical_csv')
        self.assertEqual(elements, ['vapour_pressure', 'wind_speed', 'snow_depth', 'pressure', 'relative_humidity', 'precipitation', 'sunshine_duration', 'tas_mean', 'tas_max', 'tas_min', 'wind_from_direction'])

    def test_list_supported_elements_can_return_provider_raw_codes(self) -> None:
        elements = list_supported_elements(resolution='daily', dataset_scope='historical_csv', provider_raw=True)
        self.assertEqual(elements, ['E', 'F', 'HS', 'P', 'RH', 'SRA', 'SSV', 'T', 'TMA', 'TMI', 'WDIR', 'WSPD'])

    def test_list_supported_elements_can_return_mapping_table(self) -> None:
        mapping = list_supported_elements(resolution='daily', dataset_scope='historical_csv', include_mapping=True)
        self.assertEqual(list(mapping.columns), ['element', 'element_raw', 'raw_elements'])
        wind_speed = mapping[mapping['element'] == 'wind_speed'].iloc[0]
        self.assertEqual(wind_speed['element_raw'], 'F')
        self.assertEqual(wind_speed['raw_elements'], ['F', 'WSPD'])


class ObservationQueryValidationTests(unittest.TestCase):
    def test_query_normalizes_station_ids_and_translates_canonical_elements_for_cz(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=[' 0-20000-0-11406 ', '0-20000-0-11406', '0-20000-0-11414'], start_date='2024-01-01', end_date='2024-12-31', elements=[' tas_max ', 'TMI', 'tas_max'])
        self.assertEqual(query.station_ids, ['0-20000-0-11406', '0-20000-0-11414'])
        self.assertEqual(query.elements, ['TMA', 'TMI'])

    def test_query_accepts_raw_provider_codes_for_backward_compatibility(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date='2024-01-01', end_date='2024-01-02', elements=['tma', 'TMI'])
        self.assertEqual(query.elements, ['TMA', 'TMI'])

    def test_same_canonical_daily_request_shape_works_for_cz_and_de(self) -> None:
        cz_query = ObservationQuery(country='CZ', dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'sunshine_duration'])
        de_query = ObservationQuery(country='DE', dataset_scope='historical', resolution='daily', station_ids=['00044'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'sunshine_duration'])
        self.assertEqual(cz_query.elements, ['T', 'SSV'])
        self.assertEqual(de_query.elements, ['TMK', 'SDK'])

    def test_valid_but_not_implemented_combination_is_still_query_valid(self) -> None:
        query = ObservationQuery(dataset_scope='now', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T01:00:00Z')
        self.assertEqual(query.dataset_scope, 'now')
        self.assertEqual(query.resolution, '10min')

    def test_query_rejects_mixed_date_and_datetime_ranges(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-31T00:00:00Z', start_date='2024-01-01', end_date='2024-01-31')

    def test_hourly_query_rejects_date_only_precision(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(dataset_scope='historical_csv', resolution='1hour', station_ids=['0-20000-0-11406'], start_date='2024-01-01', end_date='2024-01-02', elements=['vapour_pressure'])

    def test_daily_query_rejects_datetime_precision(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-31T00:00:00Z')

    def test_de_daily_query_accepts_dwd_elements_and_canonical_names(self) -> None:
        raw_query = ObservationQuery(country='DE', dataset_scope='historical', resolution='daily', station_ids=['00003'], start_date='2024-01-01', end_date='2024-01-02', elements=['tmk', 'rsk'])
        canonical_query = ObservationQuery(country='DE', dataset_scope='historical', resolution='daily', station_ids=['00003'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_mean', 'precipitation'])
        self.assertEqual(raw_query.country, 'DE')
        self.assertEqual(raw_query.elements, ['TMK', 'RSK'])
        self.assertEqual(canonical_query.elements, ['TMK', 'RSK'])

    def test_be_hourly_query_accepts_be_elements_and_canonical_names(self) -> None:
        raw_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['temp_dry_shelter_avg', 'pressure'])
        canonical_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='1hour', station_ids=['6414'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(raw_query.country, 'BE')
        self.assertEqual(raw_query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(canonical_query.elements, ['temp_dry_shelter_avg', 'pressure'])

    def test_be_tenmin_query_accepts_be_elements_and_canonical_names(self) -> None:
        raw_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['temp_dry_shelter_avg', 'pressure'])
        canonical_query = ObservationQuery(country='BE', dataset_scope='historical', resolution='10min', station_ids=['6414'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(raw_query.country, 'BE')
        self.assertEqual(raw_query.elements, ['temp_dry_shelter_avg', 'pressure'])
        self.assertEqual(canonical_query.elements, ['temp_dry_shelter_avg', 'pressure'])

    def test_dk_hourly_query_accepts_dk_elements_and_canonical_names(self) -> None:
        raw_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['mean_temp', 'mean_pressure'])
        canonical_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='1hour', station_ids=['06180'], start='2024-01-01T01:00:00Z', end='2024-01-01T02:00:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(raw_query.country, 'DK')
        self.assertEqual(raw_query.elements, ['mean_temp', 'mean_pressure'])
        self.assertEqual(canonical_query.elements, ['mean_temp', 'mean_pressure'])


    def test_dk_tenmin_query_accepts_dk_elements_and_canonical_names(self) -> None:
        raw_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='10min', station_ids=['06180'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['temp_dry', 'pressure'])
        canonical_query = ObservationQuery(country='DK', dataset_scope='historical', resolution='10min', station_ids=['06180'], start='2024-01-01T00:10:00Z', end='2024-01-01T00:20:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(raw_query.country, 'DK')
        self.assertEqual(raw_query.elements, ['temp_dry', 'pressure'])
        self.assertEqual(canonical_query.elements, ['temp_dry', 'pressure'])
    def test_se_hourly_query_accepts_se_elements_and_canonical_names(self) -> None:
        raw_query = ObservationQuery(country='SE', dataset_scope='historical', resolution='1hour', station_ids=['98230'], start='2012-11-29T11:00:00Z', end='2012-11-29T13:00:00Z', elements=['1', '9'])
        canonical_query = ObservationQuery(country='SE', dataset_scope='historical', resolution='1hour', station_ids=['98230'], start='2012-11-29T11:00:00Z', end='2012-11-29T13:00:00Z', elements=['tas_mean', 'pressure'])
        self.assertEqual(raw_query.country, 'SE')
        self.assertEqual(raw_query.elements, ['1', '9'])
        self.assertEqual(canonical_query.elements, ['1', '9'])
    def test_query_rejects_unknown_canonical_element_for_path(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(country='DE', dataset_scope='historical', resolution='daily', station_ids=['00003'], start_date='2024-01-01', end_date='2024-01-02', elements=['tas_period_max'])

    def test_daily_query_accepts_explicit_all_history_mode(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], all_history=True, elements=['tas_mean'])
        self.assertTrue(query.all_history)
        self.assertIsNone(query.start_date)
        self.assertIsNone(query.end_date)

    def test_subdaily_query_accepts_explicit_all_history_mode(self) -> None:
        query = ObservationQuery(country='DE', dataset_scope='historical', resolution='1hour', station_ids=['00044'], all_history=True, elements=['tas_mean'])
        self.assertTrue(query.all_history)
        self.assertIsNone(query.start)
        self.assertIsNone(query.end)
        self.assertEqual(query.elements, ['TT_TU'])

    def test_all_history_is_mutually_exclusive_with_explicit_ranges(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date='2024-01-01', end_date='2024-01-02', all_history=True)

    def test_query_requires_explicit_range_without_all_history(self) -> None:
        with self.assertRaises(QueryValidationError):
            ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'])

    def test_validate_observation_query_returns_query(self) -> None:
        query = ObservationQuery(dataset_scope='historical_csv', resolution='daily', station_ids=['0-20000-0-11406'], start_date=date(2024, 1, 1), end_date=date(2024, 12, 31))
        validated = validate_observation_query(query)
        self.assertIs(validated, query)


if __name__ == '__main__':
    unittest.main()

