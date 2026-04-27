import unittest

from weatherdownload import DatasetNotImplementedError, ObservationQuery, download_observations, get_dataset_spec, list_dataset_scopes, list_resolutions, list_supported_elements
from weatherdownload.elements import CANONICAL_ELEMENT_METADATA
from weatherdownload.providers.cz.registry import list_implemented_dataset_specs


class RegistryTests(unittest.TestCase):
    def test_canonical_element_metadata_contains_open_water_evaporation(self) -> None:
        metadata = CANONICAL_ELEMENT_METADATA['open_water_evaporation']
        self.assertEqual(metadata['description'], 'Daily measured evaporation from an open water surface.')
        self.assertEqual(metadata['unit'], 'mm')

    def test_get_dataset_spec_for_daily_historical_csv(self) -> None:
        spec = get_dataset_spec('historical_csv', 'daily')
        self.assertEqual(spec.dataset_scope, 'historical_csv')
        self.assertEqual(spec.resolution, 'daily')
        self.assertEqual(spec.station_identifier_type, 'wsi')
        self.assertEqual(spec.time_semantics, 'date')
        self.assertTrue(spec.implemented)
        self.assertEqual(spec.canonical_elements['tas_mean'], ('T',))
        self.assertEqual(spec.canonical_elements['open_water_evaporation'], ('VY',))

    def test_get_dataset_spec_for_hourly_historical_csv(self) -> None:
        spec = get_dataset_spec('historical_csv', '1hour')
        self.assertEqual(spec.dataset_scope, 'historical_csv')
        self.assertEqual(spec.resolution, '1hour')
        self.assertEqual(spec.station_identifier_type, 'wsi')
        self.assertEqual(spec.time_semantics, 'datetime')
        self.assertTrue(spec.implemented)
        self.assertEqual(spec.supported_elements, ('E', 'P', 'N', 'W1', 'W2', 'SSV1H'))

    def test_get_dataset_spec_for_tenmin_historical_csv(self) -> None:
        spec = get_dataset_spec('historical_csv', '10min')
        self.assertEqual(spec.dataset_scope, 'historical_csv')
        self.assertEqual(spec.resolution, '10min')
        self.assertEqual(spec.station_identifier_type, 'wsi')
        self.assertEqual(spec.time_semantics, 'datetime')
        self.assertTrue(spec.implemented)
        self.assertEqual(spec.supported_elements, ('T', 'TMA', 'TMI', 'TPM', 'T10', 'T100', 'SSV10M'))

    def test_get_dataset_spec_for_valid_but_not_implemented_path(self) -> None:
        spec = get_dataset_spec('now', '10min')
        self.assertEqual(spec.dataset_scope, 'now')
        self.assertEqual(spec.resolution, '10min')
        self.assertFalse(spec.implemented)

    def test_supported_elements_match_element_groups(self) -> None:
        spec = get_dataset_spec('historical_csv', 'daily')
        self.assertIsNotNone(spec.element_groups)
        self.assertEqual(tuple(spec.element_groups.keys()), spec.supported_elements)

    def test_discovery_reads_from_registry_with_canonical_names_by_default(self) -> None:
        self.assertIn('now', list_dataset_scopes())
        self.assertIn('10min', list_resolutions('now'))
        self.assertEqual(
            ['tas_mean', 'tas_max', 'tas_min', 'tas_period_max', 'soil_temperature_10cm', 'soil_temperature_100cm', 'sunshine_duration'],
            list_supported_elements('10min', 'historical_csv'),
        )
        self.assertEqual(
            ['vapour_pressure', 'pressure', 'cloud_cover', 'past_weather_1', 'past_weather_2', 'sunshine_duration'],
            list_supported_elements('1hour', 'historical_csv'),
        )

    def test_discovery_can_still_return_provider_raw_codes(self) -> None:
        self.assertEqual(['T', 'TMA', 'TMI', 'TPM', 'T10', 'T100', 'SSV10M'], list_supported_elements('10min', 'historical_csv', provider_raw=True))
        self.assertEqual(['E', 'P', 'N', 'W1', 'W2', 'SSV1H'], list_supported_elements('1hour', 'historical_csv', provider_raw=True))

    def test_daily_discovery_includes_open_water_evaporation_for_cz(self) -> None:
        self.assertIn('open_water_evaporation', list_supported_elements('daily', 'historical_csv'))
        self.assertIn('VY', list_supported_elements('daily', 'historical_csv', provider_raw=True))

    def test_discovery_can_return_mapping_table(self) -> None:
        mapping = list_supported_elements('10min', 'historical_csv', include_mapping=True)
        self.assertEqual(list(mapping.columns), ['element', 'element_raw', 'raw_elements'])
        tas_mean = mapping[mapping['element'] == 'tas_mean'].iloc[0]
        self.assertEqual(tas_mean['element_raw'], 'T')
        self.assertEqual(tas_mean['raw_elements'], ['T'])

    def test_can_list_implemented_specs(self) -> None:
        implemented = {(spec.dataset_scope, spec.resolution) for spec in list_implemented_dataset_specs()}
        self.assertEqual(implemented, {('historical_csv', '10min'), ('historical_csv', '1hour'), ('historical_csv', 'daily')})

    def test_valid_but_not_implemented_path_raises_dedicated_error_at_download_time(self) -> None:
        query = ObservationQuery(dataset_scope='now', resolution='10min', station_ids=['0-20000-0-11406'], start='2024-01-01T00:00:00Z', end='2024-01-01T01:00:00Z')
        with self.assertRaises(DatasetNotImplementedError):
            download_observations(query)


if __name__ == '__main__':
    unittest.main()

