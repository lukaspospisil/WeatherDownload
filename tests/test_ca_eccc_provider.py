import unittest
from pathlib import Path

from weatherdownload import (
    ObservationQuery,
    download_observations,
    get_provider,
    list_providers,
    list_resolutions,
    list_station_elements,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.ca.eccc_parser import CA_ECCC_NORMALIZED_DAILY_COLUMNS


SAMPLE_ECCC_DAILY_PATH = Path('tests/data/sample_ca_eccc_daily.json')
SAMPLE_GHCND_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')


class CanadaEcccProviderTests(unittest.TestCase):
    def test_provider_capability_metadata_includes_eccc_and_ghcnd(self) -> None:
        provider = get_provider('CA')
        self.assertEqual(provider.supported_country_codes, ('CA',))
        self.assertEqual(provider.supported_providers, ('eccc', 'ghcnd'))
        self.assertEqual(provider.supported_resolutions, ('daily',))

    def test_discovery_country_ca_includes_eccc_daily(self) -> None:
        self.assertEqual(list_providers(country='CA'), ['eccc', 'ghcnd'])
        self.assertEqual(list_resolutions(country='CA', provider='eccc'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='CA', provider='eccc', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='CA', provider='eccc', resolution='daily', provider_raw=True),
            ['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'],
        )
        mapping = list_supported_elements(country='CA', provider='eccc', resolution='daily', include_mapping=True)
        self.assertEqual(
            mapping[['element', 'element_raw']].to_dict('records'),
            [
                {'element': 'tas_mean', 'element_raw': 'MEAN_TEMPERATURE'},
                {'element': 'tas_max', 'element_raw': 'MAX_TEMPERATURE'},
                {'element': 'tas_min', 'element_raw': 'MIN_TEMPERATURE'},
                {'element': 'precipitation', 'element_raw': 'TOTAL_PRECIPITATION'},
            ],
        )

    def test_read_station_metadata_and_observation_metadata_from_eccc_fixture(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['1021330'])
        self.assertEqual(stations.iloc[0]['begin_date'], '2025-01-01T00:00Z')
        self.assertEqual(stations.iloc[0]['end_date'], '2025-01-03T23:59Z')
        self.assertEqual(stations.iloc[0]['full_name'], 'TEST STATION')
        self.assertTrue(stations['gh_id'].isna().all())

        metadata = read_station_observation_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        self.assertEqual(
            metadata[['station_id', 'element']].to_dict('records'),
            [
                {'station_id': '1021330', 'element': 'MAX_TEMPERATURE'},
                {'station_id': '1021330', 'element': 'MEAN_TEMPERATURE'},
                {'station_id': '1021330', 'element': 'MIN_TEMPERATURE'},
                {'station_id': '1021330', 'element': 'TOTAL_PRECIPITATION'},
            ],
        )

    def test_eccc_query_normalizes_canonical_and_raw_elements(self) -> None:
        canonical_query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='daily',
            station_ids=['1021330'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        raw_query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='daily',
            station_ids=['1021330'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'],
        )
        self.assertEqual(canonical_query.elements, ['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'])
        self.assertEqual(raw_query.elements, ['MEAN_TEMPERATURE', 'MAX_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'])

    def test_download_observations_reads_local_eccc_fixture_via_station_metadata_source(self) -> None:
        station_metadata = read_station_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        query = ObservationQuery(
            country='CA',
            provider='eccc',
            resolution='daily',
            station_ids=['1021330'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        observations = download_observations(query, country='CA', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), CA_ECCC_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max', 'tas_mean', 'tas_min'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['MAX_TEMPERATURE', 'MEAN_TEMPERATURE', 'MIN_TEMPERATURE', 'TOTAL_PRECIPITATION'])
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('tas_mean', query.start_date)]), 5.4)
        self.assertAlmostEqual(float(lookup[('tas_min', query.start_date)]), 1.1)
        self.assertAlmostEqual(float(lookup[('tas_max', query.end_date)]), 3.2)
        self.assertAlmostEqual(float(lookup[('precipitation', query.end_date)]), 0.0)
        self.assertNotIn(('tas_mean', query.end_date), lookup.index)

    def test_station_elements_for_eccc_fixture_are_mapping_driven(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_ECCC_DAILY_PATH))
        self.assertEqual(
            list_station_elements(stations, '1021330', 'eccc', 'daily', country='CA'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )

    def test_ca_ghcnd_metadata_path_remains_available(self) -> None:
        stations = read_station_metadata(country='CA', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['CA000000001', 'CA000000002'])


if __name__ == '__main__':
    unittest.main()
