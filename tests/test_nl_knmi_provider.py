import json
import unittest
from pathlib import Path
from unittest.mock import patch

from weatherdownload import (
    ObservationQuery,
    download_observations,
    list_providers,
    list_resolutions,
    list_supported_countries,
    list_supported_elements,
    read_station_metadata,
    read_station_observation_metadata,
)

SAMPLE_PATH = Path('tests/data/sample_knmi_daily_public.txt')
SAMPLE_TEXT = SAMPLE_PATH.read_text(encoding='utf-8')


class NetherlandsKnmiProviderTests(unittest.TestCase):
    def test_supported_country_and_discovery_expose_only_knmi_daily(self) -> None:
        self.assertIn('NL', list_supported_countries())
        self.assertEqual(list_providers(country='NL'), ['knmi'])
        self.assertEqual(list_resolutions(country='NL', provider='knmi'), ['daily'])
        self.assertEqual(
            list_supported_elements(country='NL', provider='knmi', resolution='daily'),
            ['tas_mean', 'tas_max', 'tas_min', 'precipitation', 'wind_speed', 'relative_humidity', 'pressure', 'sunshine_duration', 'solar_radiation'],
        )
        self.assertEqual(
            list_supported_elements(country='NL', provider='knmi', resolution='daily', provider_raw=True),
            ['TG', 'TX', 'TN', 'RH', 'FG', 'UG', 'PG', 'SQ', 'Q'],
        )

    def test_read_station_metadata_and_observation_metadata_from_fixture(self) -> None:
        stations = read_station_metadata(country='NL', source_url=str(SAMPLE_PATH))
        metadata = read_station_observation_metadata(country='NL', source_url=str(SAMPLE_PATH))

        self.assertEqual(
            list(stations.columns),
            ['station_id', 'gh_id', 'begin_date', 'end_date', 'full_name', 'longitude', 'latitude', 'elevation_m'],
        )
        self.assertEqual(stations['station_id'].tolist(), ['260', '310'])
        self.assertTrue(stations['gh_id'].isna().all())
        self.assertEqual(
            list(metadata.columns),
            ['obs_type', 'station_id', 'begin_date', 'end_date', 'element', 'schedule', 'name', 'description', 'height'],
        )
        self.assertIn('Q', metadata['element'].tolist())
        self.assertIn('Daily global solar radiation', metadata['name'].tolist())

    def test_knmi_query_accepts_canonical_and_raw_codes(self) -> None:
        canonical_query = ObservationQuery(
            country='NL',
            provider='knmi',
            resolution='daily',
            station_ids=['260'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['tas_mean', 'precipitation', 'solar_radiation'],
        )
        raw_query = ObservationQuery(
            country='NL',
            provider='knmi',
            resolution='daily',
            station_ids=['260'],
            start_date='2024-01-01',
            end_date='2024-01-02',
            elements=['TG', 'RH', 'Q'],
        )

        self.assertEqual(canonical_query.elements, ['TG', 'RH', 'Q'])
        self.assertEqual(raw_query.elements, ['TG', 'RH', 'Q'])

    def test_knmi_daily_download_uses_public_csv_and_normalizes_output(self) -> None:
        station_metadata = read_station_metadata(country='NL', source_url=str(SAMPLE_PATH))

        def fake_post(url, data=None, timeout=60):
            self.assertIn('start', data)
            self.assertIn('end', data)
            self.assertEqual(data['stns'], '260')
            self.assertEqual(data['vars'], 'TG:RH:Q')

            class _Response:
                text = SAMPLE_TEXT
                encoding = 'utf-8'

                def raise_for_status(self) -> None:
                    return None

            return _Response()

        with patch('weatherdownload.providers.nl.metadata.requests.post', side_effect=fake_post):
            query = ObservationQuery(
                country='NL',
                provider='knmi',
                resolution='daily',
                station_ids=['260'],
                start_date='2024-01-01',
                end_date='2024-01-02',
                elements=['tas_mean', 'precipitation', 'solar_radiation'],
            )
            observations = download_observations(query, country='NL', station_metadata=station_metadata)

        self.assertEqual(
            list(observations.columns),
            ['station_id', 'gh_id', 'element', 'element_raw', 'observation_date', 'time_function', 'value', 'flag', 'quality', 'provider', 'resolution'],
        )
        self.assertEqual(sorted(observations['element'].unique().tolist()), ['precipitation', 'solar_radiation', 'tas_mean'])
        self.assertEqual(sorted(observations['element_raw'].unique().tolist()), ['Q', 'RH', 'TG'])
        self.assertTrue(observations['gh_id'].isna().all())
        self.assertTrue(observations['time_function'].isna().all())
        self.assertTrue(observations['flag'].isna().all())
        self.assertTrue(observations['quality'].isna().all())
        lookup = {
            (row.element, row.observation_date.isoformat()): row.value
            for row in observations.itertuples(index=False)
        }
        self.assertAlmostEqual(float(lookup[('tas_mean', '2024-01-01')]), 7.4)
        self.assertAlmostEqual(float(lookup[('precipitation', '2024-01-01')]), 0.0)
        self.assertAlmostEqual(float(lookup[('solar_radiation', '2024-01-01')]), 1.42)

    def test_provider_note_and_capabilities_docs_are_updated(self) -> None:
        provider_note = Path('docs/provider_notes/nl_knmi.md').read_text(encoding='utf-8')
        capabilities = Path('docs/supported_capabilities.md').read_text(encoding='utf-8')

        self.assertIn('daggegevens.knmi.nl/klimatologie/daggegevens', provider_note)
        self.assertIn('trace precipitation', provider_note)
        self.assertIn('0.01', provider_note)
        self.assertIn('allow-derived', provider_note)
        self.assertIn(
            '| `NL` | `knmi` | `daily` | `tas_mean`, `tas_max`, `tas_min`, `precipitation`, `wind_speed`, `relative_humidity`, `pressure`, `sunshine_duration`, `solar_radiation` |'.replace('`', ''),
            capabilities.replace('`', ''),
        )


if __name__ == '__main__':
    unittest.main()
