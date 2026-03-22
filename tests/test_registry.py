import unittest

from weatherdownload import DatasetNotImplementedError, ObservationQuery, download_observations, get_dataset_spec, list_dataset_scopes, list_resolutions, list_supported_elements
from weatherdownload.chmi_registry import list_implemented_dataset_specs


class RegistryTests(unittest.TestCase):
    def test_get_dataset_spec_for_daily_historical_csv(self) -> None:
        spec = get_dataset_spec('historical_csv', 'daily')
        self.assertEqual(spec.dataset_scope, 'historical_csv')
        self.assertEqual(spec.resolution, 'daily')
        self.assertEqual(spec.station_identifier_type, 'wsi')
        self.assertEqual(spec.time_semantics, 'date')
        self.assertTrue(spec.implemented)

    def test_get_dataset_spec_for_hourly_historical_csv(self) -> None:
        spec = get_dataset_spec('historical_csv', '1hour')
        self.assertEqual(spec.dataset_scope, 'historical_csv')
        self.assertEqual(spec.resolution, '1hour')
        self.assertEqual(spec.station_identifier_type, 'wsi')
        self.assertEqual(spec.time_semantics, 'datetime')
        self.assertTrue(spec.implemented)

    def test_get_dataset_spec_for_valid_but_not_implemented_path(self) -> None:
        spec = get_dataset_spec('now', '10min')
        self.assertEqual(spec.dataset_scope, 'now')
        self.assertEqual(spec.resolution, '10min')
        self.assertFalse(spec.implemented)

    def test_supported_elements_match_element_groups(self) -> None:
        spec = get_dataset_spec('historical_csv', 'daily')
        self.assertIsNotNone(spec.element_groups)
        self.assertEqual(tuple(spec.element_groups.keys()), spec.supported_elements)

    def test_discovery_reads_from_registry(self) -> None:
        self.assertIn('now', list_dataset_scopes())
        self.assertIn('10min', list_resolutions('now'))
        self.assertEqual(['E'], list_supported_elements('1hour', 'historical_csv'))

    def test_can_list_implemented_specs(self) -> None:
        implemented = {(spec.dataset_scope, spec.resolution) for spec in list_implemented_dataset_specs()}
        self.assertEqual(implemented, {('historical_csv', '1hour'), ('historical_csv', 'daily')})

    def test_valid_but_not_implemented_path_raises_dedicated_error_at_download_time(self) -> None:
        query = ObservationQuery(dataset_scope='now', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T01:00:00Z')
        with self.assertRaises(DatasetNotImplementedError):
            download_observations(query)


if __name__ == '__main__':
    unittest.main()
