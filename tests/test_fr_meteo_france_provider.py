import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from weatherdownload import (
    ObservationQuery,
    download_observations,
    find_stations_with_elements,
    list_providers,
    list_resolutions,
    list_station_elements,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)
from weatherdownload.providers.fr.daily import build_fr_daily_download_targets
from weatherdownload.providers.fr.parser import FR_NORMALIZED_DAILY_COLUMNS

SAMPLE_STATIONS_PATH = Path('tests/data/sample_fr_meteo_france_fiches.json')
SAMPLE_DAILY_CSV_TEXT = Path('tests/data/sample_fr_meteo_france_rr_t_vent.csv').read_text(encoding='utf-8')
SAMPLE_GHCND_STATIONS_PATH = Path('tests/data/sample_ghcnd_stations.txt')


class _MockResponse:
    def __init__(self, text: str | None = None, status_code: int = 200, content: bytes | None = None) -> None:
        self.text = text or ''
        self.status_code = status_code
        self.encoding = 'utf-8'
        self.content = content if content is not None else self.text.encode('utf-8')

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')


class FranceMeteoFranceProviderTests(unittest.TestCase):
    def test_supported_countries_include_fr(self) -> None:
        self.assertIn('FR', list_supported_countries())
        self.assertEqual(list_providers(country='FR'), ['ghcnd', 'meteo_france'])
        self.assertEqual(list_resolutions(country='FR', provider='ghcnd'), ['daily'])
        self.assertEqual(list_resolutions(country='FR', provider='meteo_france'), ['daily'])

    def test_fr_discovery_keeps_ghcnd_and_adds_meteo_france(self) -> None:
        self.assertEqual(
            list_supported_elements(country='FR', provider='meteo_france', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_supported_elements(country='FR', provider='meteo_france', resolution='daily', provider_raw=True),
            ['RR', 'TN', 'TX', 'TM'],
        )
        self.assertEqual(
            list_supported_elements(country='FR', provider='ghcnd', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'snow_depth'],
        )
        self.assertNotIn(
            'open_water_evaporation',
            list_supported_elements(country='FR', provider='meteo_france', resolution='daily'),
        )
        self.assertNotIn(
            'wind_speed',
            list_supported_elements(country='FR', provider='meteo_france', resolution='daily'),
        )

    def test_read_station_metadata_country_fr_from_local_fixture(self) -> None:
        stations = read_station_metadata(country='FR', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['07005001', '13055001'])
        self.assertTrue(stations['gh_id'].isna().all())
        first = stations.iloc[0]
        self.assertEqual(first['full_name'], 'AUBENAS')
        self.assertAlmostEqual(float(first['longitude']), 4.3910)
        self.assertAlmostEqual(float(first['latitude']), 44.6210)
        self.assertAlmostEqual(float(first['elevation_m']), 242.0)
        self.assertEqual(first['begin_date'], '1990-01-01T00:00Z')
        self.assertEqual(first['end_date'], '3999-12-31T23:59Z')

    def test_read_station_observation_metadata_country_fr_from_local_fixture(self) -> None:
        metadata = read_station_observation_metadata(country='FR', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertEqual(sorted(metadata['station_id'].unique().tolist()), ['07005001', '13055001'])
        self.assertEqual(sorted(metadata['element'].unique().tolist()), ['RR', 'TM', 'TN', 'TX'])
        aubenas_tm = metadata[(metadata['station_id'] == '07005001') & (metadata['element'] == 'TM')].iloc[0]
        self.assertEqual(aubenas_tm['begin_date'], '1995-01-01T00:00Z')
        self.assertEqual(aubenas_tm['end_date'], '3999-12-31T23:59Z')

    def test_fr_daily_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(
            country='FR',
            provider='meteo_france',
            resolution='daily',
            station_ids=['07005001'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        raw_query = ObservationQuery(
            country='FR',
            provider='meteo_france',
            resolution='daily',
            station_ids=['07005001'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['TM', 'TX', 'TN', 'RR'],
        )
        self.assertEqual(canonical_query.elements, ['TM', 'TX', 'TN', 'RR'])
        self.assertEqual(raw_query.elements, ['TM', 'TX', 'TN', 'RR'])

    def test_fr_station_elements_and_finder_are_metadata_driven(self) -> None:
        stations = read_station_metadata(country='FR', source_url=str(SAMPLE_STATIONS_PATH))
        observation_metadata = read_station_observation_metadata(country='FR', source_url=str(SAMPLE_STATIONS_PATH))
        self.assertEqual(
            list_station_elements(stations, '07005001', 'meteo_france', 'daily', country='FR'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        self.assertEqual(
            list_station_elements(stations, '13055001', 'meteo_france', 'daily', country='FR'),
            ['tas_max', 'precipitation'],
        )
        matches = find_stations_with_elements(
            country='FR',
            provider='meteo_france',
            resolution='daily',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
            stations=stations,
            observation_metadata=observation_metadata,
        )
        self.assertEqual(matches['station_id'].tolist(), ['07005001'])
        self.assertEqual(matches.iloc[0]['matching_begin_date'], '1995-01-01')

    def test_build_targets_selects_only_needed_departments_and_periods(self) -> None:
        query = ObservationQuery(
            country='FR',
            provider='meteo_france',
            resolution='daily',
            station_ids=['07005001', '13055001'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean'],
        )
        targets = build_fr_daily_download_targets(query)
        self.assertEqual(
            [target.url for target in targets],
            [
                'https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_07_latest-2025-2026_RR-T-Vent.csv.gz',
                'https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/Q_13_latest-2025-2026_RR-T-Vent.csv.gz',
            ],
        )

    def test_download_daily_observations_country_fr_with_canonical_elements(self) -> None:
        station_metadata = read_station_metadata(country='FR', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('Q_07_latest-2025-2026_RR-T-Vent.csv.gz'):
                return _MockResponse(content=SAMPLE_DAILY_CSV_TEXT.encode('utf-8'))
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(
            country='FR',
            provider='meteo_france',
            resolution='daily',
            station_ids=['07005001'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['tas_mean', 'tas_max', 'tas_min', 'precipitation'],
        )
        with patch('weatherdownload.providers.fr.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='FR', station_metadata=station_metadata)
        self.assertEqual(list(observations.columns), FR_NORMALIZED_DAILY_COLUMNS)
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'tas_max', 'tas_mean', 'tas_min'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['RR', 'TM', 'TN', 'TX'])
        self.assertEqual(observations['provider'].unique().tolist(), ['meteo_france'])
        self.assertEqual(observations['resolution'].unique().tolist(), ['daily'])
        self.assertEqual(str(observations['quality'].dtype), 'Int64')
        lookup = observations.set_index(['element', 'observation_date'])['value']
        self.assertAlmostEqual(float(lookup[('precipitation', pd.Timestamp('2025-01-01').date())]), 12.3)
        self.assertAlmostEqual(float(lookup[('tas_min', pd.Timestamp('2025-01-01').date())]), 1.1)
        self.assertAlmostEqual(float(lookup[('tas_max', pd.Timestamp('2025-01-02').date())]), 8.1)
        self.assertNotIn(('tas_mean', pd.Timestamp('2025-01-02').date()), lookup.index)

    def test_fr_meteo_france_daily_missing_values_become_na(self) -> None:
        station_metadata = read_station_metadata(country='FR', source_url=str(SAMPLE_STATIONS_PATH))

        def fake_get(url: str, timeout: int = 60):
            if url.endswith('Q_07_latest-2025-2026_RR-T-Vent.csv.gz'):
                return _MockResponse(content=SAMPLE_DAILY_CSV_TEXT.encode('utf-8'))
            raise AssertionError(f'unexpected url: {url}')

        query = ObservationQuery(
            country='FR',
            provider='meteo_france',
            resolution='daily',
            station_ids=['07005001'],
            start_date='2025-01-01',
            end_date='2025-01-02',
            elements=['precipitation', 'tas_mean'],
        )
        with patch('weatherdownload.providers.fr.daily.requests.get', side_effect=fake_get):
            observations = download_observations(query, country='FR', station_metadata=station_metadata)
        self.assertEqual(
            observations[['element', 'observation_date']].to_dict('records'),
            [
                {'element': 'precipitation', 'observation_date': pd.Timestamp('2025-01-01').date()},
                {'element': 'tas_mean', 'observation_date': pd.Timestamp('2025-01-01').date()},
            ],
        )

    def test_fr_ghcnd_behavior_remains_available(self) -> None:
        stations = read_station_metadata(country='FR', source_url=str(SAMPLE_GHCND_STATIONS_PATH))
        self.assertEqual(stations['station_id'].tolist(), ['FR000000001', 'FR000000002'])


if __name__ == '__main__':
    unittest.main()
