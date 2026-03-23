import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

MODULE_PATH = Path('examples/probe_shmu_sk.py')
SPEC = importlib.util.spec_from_file_location('probe_shmu_sk_example', MODULE_PATH)
probe_shmu_sk = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(probe_shmu_sk)

SAMPLE_PAYLOAD_TEXT = Path('tests/data/sample_shmu_kli_inter_2025-01.json').read_text(encoding='utf-8')
SAMPLE_METADATA_TEXT = Path('tests/data/sample_shmu_kli_inter_metadata.json').read_text(encoding='utf-8')


class ProbeShmuSkExampleTests(unittest.TestCase):
    def test_write_shmu_cache_provenance_writes_expected_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_root = Path(tmp_dir) / 'SK' / 'recent' / 'daily'
            provenance_path = probe_shmu_sk.write_shmu_cache_provenance(
                cache_root,
                requested_date='2025-01-01',
                requested_station_id='11800',
                requested_elements=['tas_max', 'precipitation'],
                resolved_elements=['t_max', 'zra_uhrn'],
            )
            payload = json.loads(provenance_path.read_text(encoding='utf-8'))

        self.assertEqual(payload['provider_name'], 'SHMU (experimental)')
        self.assertTrue(payload['experimental'])
        self.assertEqual(payload['requested_date'], '2025-01-01')
        self.assertEqual(payload['requested_station_id'], '11800')
        self.assertEqual(payload['requested_elements'], ['tas_max', 'precipitation'])
        self.assertEqual(payload['resolved_elements'], ['t_max', 'zra_uhrn'])
        self.assertEqual(payload['source_mode'], 'recent daily')

    def test_main_writes_provenance_and_prints_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_root = Path(tmp_dir)
            cache_dir = temp_root / 'cache'
            output_path = temp_root / 'sample.csv'

            def fake_probe(timeout: int = 60) -> pd.DataFrame:
                return pd.DataFrame([
                    {
                        'dataset_scope': 'recent',
                        'resolution': 'daily',
                        'source_id': 'recent_daily_kli_inter',
                        'implemented': True,
                        'experimental': True,
                        'metadata_url': 'https://example.test/metadata.json',
                        'data_index_url': 'https://example.test/daily/',
                        'sample_url': 'https://example.test/2025-01.json',
                        'sample_dataset': 'Climatological stations',
                        'sample_interval': '1 day',
                        'sample_frequency': '1 months',
                        'sample_station_count': 2,
                        'sample_record_count': 4,
                        'metadata_field_count': 4,
                        'notes': 'sample',
                    }
                ])

            def fake_get(url: str, timeout: int = 60):
                class _Response:
                    def __init__(self, text: str) -> None:
                        self.text = text
                        self.encoding = 'utf-8'

                    def raise_for_status(self) -> None:
                        return None

                if url.endswith('kli_inter_metadata.json'):
                    return _Response(SAMPLE_METADATA_TEXT)
                if url.endswith('kli-inter - 2025-01.json'):
                    return _Response(SAMPLE_PAYLOAD_TEXT)
                raise AssertionError(f'unexpected URL: {url}')

            with patch.object(probe_shmu_sk, 'probe_shmu_observation_feeds', side_effect=fake_probe):
                with patch.object(probe_shmu_sk.requests, 'get', side_effect=fake_get):
                    with patch('sys.stdout', new_callable=io.StringIO) as stdout:
                        exit_code = probe_shmu_sk.main([
                            '--country', 'SK',
                            '--month', '2025-01',
                            '--station-id', '11800',
                            '--date', '2025-01-01',
                            '--element', 'tas_max',
                            '--element', 'precipitation',
                            '--cache-dir', str(cache_dir),
                            '--output', str(output_path),
                        ])

            self.assertEqual(exit_code, 0)
            provenance_path = cache_dir / 'SK' / 'recent' / 'daily' / 'provenance.json'
            self.assertTrue(provenance_path.exists())
            provenance = json.loads(provenance_path.read_text(encoding='utf-8'))
            self.assertEqual(provenance['provider_name'], 'SHMU (experimental)')
            self.assertEqual(provenance['requested_station_id'], '11800')
            self.assertEqual(provenance['requested_date'], '2025-01-01')
            self.assertEqual(provenance['requested_elements'], ['tas_max', 'precipitation'])
            self.assertEqual(provenance['source_mode'], 'recent daily')
            self.assertIn('Provenance summary:', stdout.getvalue())
            self.assertIn('provider=SHMU (experimental)', stdout.getvalue())
            self.assertIn('station_id=11800', stdout.getvalue())


if __name__ == '__main__':
    unittest.main()
