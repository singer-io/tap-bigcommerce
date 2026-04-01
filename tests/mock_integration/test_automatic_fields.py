"""Mock integration test: automatic fields — key properties and replication
keys are marked as inclusion=automatic in metadata."""
import unittest

from singer import metadata

from .base import BigCommerceBaseTest, STREAM_CONFIG, INCREMENTAL_STREAMS


class AutomaticFieldsIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    def test_key_properties_are_automatic(self):
        catalog = self._run_discover()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                for field in expected[entry.tap_stream_id][self.PRIMARY_KEYS]:
                    inclusion = mdata.get(('properties', field), {}).get('inclusion')
                    self.assertEqual(inclusion, 'automatic')

    def test_replication_key_is_automatic_for_incremental(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            cfg = STREAM_CONFIG.get(entry.tap_stream_id, {})
            rep_key = cfg.get('replication_key')
            if rep_key:
                with self.subTest(stream=entry.tap_stream_id):
                    mdata = metadata.to_map(entry.metadata)
                    inclusion = mdata.get(('properties', rep_key), {}).get('inclusion')
                    self.assertEqual(inclusion, 'automatic')

    def test_primary_keys_are_in_schema(self):
        catalog = self._run_discover()
        expected = self.expected_metadata()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                schema_props = set(entry.schema.to_dict().get('properties', {}).keys())
                pk_fields = expected[entry.tap_stream_id][self.PRIMARY_KEYS]
                self.assertTrue(pk_fields.issubset(schema_props))

    def test_non_key_fields_are_available(self):
        catalog = self._run_discover()
        for entry in catalog.streams:
            with self.subTest(stream=entry.tap_stream_id):
                mdata = metadata.to_map(entry.metadata)
                cfg = STREAM_CONFIG[entry.tap_stream_id]
                auto_fields = {'id'}
                if cfg.get('replication_key'):
                    auto_fields.add(cfg['replication_key'])

                schema_props = set(entry.schema.to_dict().get('properties', {}).keys())
                for field in schema_props:
                    breadcrumb = ('properties', field)
                    inclusion = mdata.get(breadcrumb, {}).get('inclusion')
                    if field in auto_fields:
                        self.assertEqual(inclusion, 'automatic')
                    else:
                        self.assertEqual(
                            inclusion, 'available',
                            f"'{entry.tap_stream_id}.{field}' expected 'available', "
                            f"got '{inclusion}'",
                        )
