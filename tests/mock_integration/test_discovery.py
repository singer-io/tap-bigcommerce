"""Mock integration test: discovery produces correct catalog and metadata."""
import unittest

from singer import metadata

from .base import BigCommerceBaseTest, STREAM_CONFIG, INCREMENTAL_STREAMS


class DiscoveryIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    def test_discovery_returns_all_streams(self):
        catalog = self._run_discover()
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(stream_ids, self.ALL_STREAM_IDS)

    def test_key_properties_match_expected(self):
        catalog = self._run_discover()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                actual_keys = set(mdata.get((), {}).get('table-key-properties', []))
                self.assertEqual(
                    actual_keys,
                    expected[entry.tap_stream_id][self.PRIMARY_KEYS],
                )

    def test_schema_properties_exist(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                schema = entry.schema.to_dict()
                self.assertIn('properties', schema)
                self.assertTrue(len(schema['properties']) > 0)

    def test_key_properties_in_schema(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                schema_props = set(entry.schema.to_dict().get('properties', {}).keys())
                key_props = set(entry.key_properties or [])
                self.assertTrue(key_props.issubset(schema_props))

    def test_replication_method_set(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                rep_method = mdata.get((), {}).get('forced-replication-method')
                self.assertIn(rep_method, ('INCREMENTAL', 'FULL_TABLE'))

    def test_incremental_streams_have_valid_replication_keys(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                rep_method = mdata.get((), {}).get('forced-replication-method')
                if rep_method == 'INCREMENTAL':
                    valid_keys = mdata.get((), {}).get('valid-replication-keys')
                    self.assertIsNotNone(valid_keys)
                    self.assertTrue(len(valid_keys) > 0)
