"""Unit tests for tap_bigcommerce.discover — discover_streams function."""
import unittest
from unittest.mock import MagicMock

from tap_bigcommerce.discover import discover_streams
from tap_bigcommerce.streams import STREAMS


class TestDiscoverStreams(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.result = discover_streams(self.client)

    def test_returns_dict_with_streams_key(self):
        self.assertIn('streams', self.result)

    def test_returns_four_streams(self):
        self.assertEqual(len(self.result['streams']), 4)

    def test_all_stream_names_present(self):
        names = {s['stream'] for s in self.result['streams']}
        self.assertEqual(names, set(STREAMS.keys()))

    def test_each_stream_has_required_keys(self):
        for s in self.result['streams']:
            self.assertIn('stream', s)
            self.assertIn('tap_stream_id', s)
            self.assertIn('schema', s)
            self.assertIn('metadata', s)

    def test_stream_equals_tap_stream_id(self):
        for s in self.result['streams']:
            self.assertEqual(s['stream'], s['tap_stream_id'])

    def test_schema_has_properties(self):
        for s in self.result['streams']:
            self.assertIn('properties', s['schema'],
                          f"{s['stream']} schema missing 'properties'")

    def test_metadata_is_list(self):
        for s in self.result['streams']:
            self.assertIsInstance(s['metadata'], list)

    def test_metadata_has_breadcrumb_and_meta(self):
        for s in self.result['streams']:
            for entry in s['metadata']:
                self.assertIn('breadcrumb', entry)
                self.assertIn('metadata', entry)


if __name__ == '__main__':
    unittest.main()
