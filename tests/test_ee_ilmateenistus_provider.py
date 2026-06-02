import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.ee.parser import (
    EE_ILMATEENISTUS_NORMALIZED_DAILY_COLUMNS,
    normalize_ee_daily_rows,
    normalize_ee_observation_metadata,
    normalize_ee_station_metadata,
    parse_ee_payload_json,
)
from weatherdownload.providers.ee.registry import EE_ILMATEENISTUS_PARAMETER_METADATA, get_dataset_spec


SAMPLE_STATION_METADATA_PATH = Path('tests/data/sample_ee_ilmateenistus_station_metadata.json')
SAMPLE_DAILY_PATH = Path('tests/data/sample_ee_ilmateenistus_daily.json')
SAMPLE_STATION_METADATA_TEXT = SAMPLE_STATION_METADATA_PATH.read_text(encoding='utf-8')
SAMPLE_DAILY_TEXT = SAMPLE_DAILY_PATH.read_text(encoding='utf-8')


class _MockTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = 'utf-8'

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class EstoniaIlmateenistusProviderTests(unittest.TestCase):
    def test_ee_discovery_exposes_official_and_ghcnd_daily(self) -> None:
        self.assertEqual(list_providers(country='EE'), ['ghcnd', 'ilmateenistus'])
        self.assertEqual(list_resolutions(country='EE', provider='ilmateenistus'), ['daily'])
        self.assertEqual(list_resolutions(country='EE', provider='ghcnd'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='EE', provider='ilmateenistus', resolution='daily'),
            [
                'tas_mean',
                'tas_max',
                'tas_min',
                'precipitation',
                'wind_speed',
                'relative_humidity',
                'sunshine_duration',
                'pressure',
                'snow_depth',
                'solar_radiation',
            ],
        )
        self.assertEqual(
            list_supported_elements(country='EE', provider='ilmateenistus', resolution='daily', provider_raw=True),
            ['DPA008', 'DPREC', 'DRH08', 'DRQS', 'DSDUR', 'DSND', 'DTAN', 'DTAX', 'DTA08', 'DWS08'],
        )

    def test_ee_parser_normalizes_station_metadata_and_observation_metadata(self) -> None:
        spec = get_dataset_spec('ilmateenistus', 'daily')
        records = parse_ee_payload_json(SAMPLE_STATION_METADATA_TEXT)
        stations = normalize_ee_station_metadata(records, spec)

        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['AJHARK01', 'AJVORU01'])
        self.assertEqual(stations['full_name'].tolist(), ['Tallinn-Harku', 'Voru'])
        self.assertEqual(stations['begin_date'].tolist(), ['1991-01-01T00:00Z', '1991-01-01T00:00Z'])
        self.assertEqual(stations['end_date'].tolist(), ['3999-12-31T23:59Z', '3999-12-31T23:59Z'])
        self.assertEqual(
            stations.attrs['station_provider_raw_elements_by_path'][('ilmateenistus', 'daily')]['AJHARK01'],
            ['DPA008', 'DPREC', 'DRH08', 'DRQS', 'DSDUR', 'DSND', 'DTA08', 'DTAN', 'DTAX', 'DWS08'],
        )

        observation_metadata = normalize_ee_observation_metadata(
            records,
            spec,
            EE_ILMATEENISTUS_PARAMETER_METADATA,
        )
        self.assertEqual(
            list(observation_metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(observation_metadata['station_id'].unique().tolist()), ['AJHARK01', 'AJVORU01'])
        self.assertEqual(
            sorted(observation_metadata['element'].unique().tolist()),
            ['DPA008', 'DPREC', 'DRH08', 'DRQS', 'DSDUR', 'DSND', 'DTA08', 'DTAN', 'DTAX', 'DWS08'],
        )

    def test_ee_parser_normalizes_daily_rows_without_unit_conversion(self) -> None:
        records = parse_ee_payload_json(SAMPLE_DAILY_TEXT)
        frame = normalize_ee_daily_rows(
            records,
            raw_code='DRQS',
            provider='ilmateenistus',
            resolution='daily',
            station_id='AJHARK01',
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
        )
        self.assertEqual(list(frame.columns), EE_ILMATEENISTUS_NORMALIZED_DAILY_COLUMNS)
        self.assertAlmostEqual(float(frame.iloc[0]['value']), 1.08)
        self.assertAlmostEqual(float(frame.iloc[1]['value']), 1.56)

    def test_read_station_metadata_and_observation_metadata_country_ee_from_fixture(self) -> None:
        stations = read_station_metadata(country='EE', source_url=str(SAMPLE_STATION_METADATA_PATH))
        observation_metadata = read_station_observation_metadata(country='EE', source_url=str(SAMPLE_STATION_METADATA_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['AJHARK01', 'AJVORU01'])
        self.assertEqual(sorted(observation_metadata['station_id'].unique().tolist()), ['AJHARK01', 'AJVORU01'])

    def test_download_daily_observations_ee_ilmateenistus_normalizes_fixture_payload(self) -> None:
        station_metadata = read_station_metadata(country='EE', source_url=str(SAMPLE_STATION_METADATA_PATH))
        query = ObservationQuery(
            country='EE',
            provider='ilmateenistus',
            resolution='daily',
            station_ids=['AJHARK01'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=[
                'pressure',
                'precipitation',
                'relative_humidity',
                'solar_radiation',
                'sunshine_duration',
                'snow_depth',
                'tas_mean',
                'tas_min',
                'tas_max',
                'wind_speed',
            ],
        )
        with patch('weatherdownload.providers.ee.daily.requests.get', return_value=_MockTextResponse(SAMPLE_DAILY_TEXT)):
            observations = download_observations(query, country='EE', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(observations['provider'].unique().tolist(), ['ilmateenistus'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())

        expected_values = {
            ('pressure', '2024-01-01'): 1019.1,
            ('pressure', '2024-01-02'): 1021.3,
            ('precipitation', '2024-01-01'): 1.3,
            ('precipitation', '2024-01-02'): 0.0,
            ('relative_humidity', '2024-01-01'): 88.0,
            ('relative_humidity', '2024-01-02'): 70.0,
            ('solar_radiation', '2024-01-01'): 1.08,
            ('solar_radiation', '2024-01-02'): 1.56,
            ('snow_depth', '2024-01-01'): 9.0,
            ('snow_depth', '2024-01-02'): 8.0,
            ('sunshine_duration', '2024-01-01'): 3.9,
            ('sunshine_duration', '2024-01-02'): 5.0,
            ('tas_mean', '2024-01-01'): -8.7,
            ('tas_mean', '2024-01-02'): -16.1,
            ('tas_max', '2024-01-01'): -5.3,
            ('tas_max', '2024-01-02'): -12.6,
            ('tas_min', '2024-01-01'): -12.6,
            ('tas_min', '2024-01-02'): -17.8,
            ('wind_speed', '2024-01-01'): 2.1,
            ('wind_speed', '2024-01-02'): 4.5,
        }
        actual_values = {
            (row.element, row.observation_date.isoformat()): round(float(row.value), 2)
            for row in observations.itertuples(index=False)
        }
        self.assertEqual(actual_values, expected_values)


if __name__ == '__main__':
    unittest.main()
