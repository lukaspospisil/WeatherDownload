import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path('scripts/dev/generate_supported_capabilities.py')
SPEC = importlib.util.spec_from_file_location('generate_supported_capabilities', MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class SupportedCapabilitiesDocTests(unittest.TestCase):
    def test_supported_capabilities_doc_matches_generator(self) -> None:
        expected = MODULE.render_supported_capabilities_markdown()
        actual = Path('docs/supported_capabilities.md').read_text(encoding='utf-8')
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
