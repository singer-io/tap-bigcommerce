"""Mock integration test: basic sync canary — verify the full pipeline runs
and emits records for expected streams."""
import unittest
from unittest.mock import patch

import tap_bigcommerce

from .base import BigCommerceBaseTest, ALL_STREAM_IDS


class SyncCanaryIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self._make_selected_catalog()
        self.state = {'bookmarks': {}}

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_full_pipeline_emits_records(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        self.assertTrue(mock_write_record.called)
        written = {c[0][0] for c in mock_write_record.call_args_list}
        self.assertIn('orders', written)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_schemas_emitted_for_synced_streams(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        schema_streams = {c[0][0] for c in mock_write_schema.call_args_list}
        for stream_id in ALL_STREAM_IDS:
            self.assertIn(stream_id, schema_streams)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_state_emitted_after_sync(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)
        self.assertTrue(mock_write_state.called)
