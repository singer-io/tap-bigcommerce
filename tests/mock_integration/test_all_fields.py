"""Mock integration test: sync all streams with mocked API responses
and verify all fields are replicated."""
import unittest
from unittest.mock import patch

import tap_bigcommerce

from .base import BigCommerceBaseTest, STREAM_CONFIG, ALL_STREAM_IDS


class AllFieldsIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self._make_selected_catalog()
        self.state = {'bookmarks': {}}

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_sync_writes_records_for_all_streams(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        written_streams = {c[0][0] for c in mock_write_record.call_args_list}
        for stream in ALL_STREAM_IDS:
            self.assertIn(stream, written_streams)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_records_have_primary_key(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        for stream in ALL_STREAM_IDS:
            with self.subTest(stream=stream):
                recs = [c[0][1] for c in mock_write_record.call_args_list
                        if c[0][0] == stream]
                self.assertTrue(len(recs) > 0, f"No records for {stream}")
                for rec in recs:
                    self.assertIn('id', rec)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_correct_record_counts(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        for stream, cfg in STREAM_CONFIG.items():
            with self.subTest(stream=stream):
                recs = [c for c in mock_write_record.call_args_list
                        if c[0][0] == stream]
                self.assertEqual(len(recs), cfg['record_count'])

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_sync_only_orders(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        catalog = self._make_selected_catalog(stream_names=['orders'])
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, {'bookmarks': {}},
            self.default_start_date)

        written = {c[0][0] for c in mock_write_record.call_args_list}
        self.assertIn('orders', written)
        for other in ALL_STREAM_IDS - {'orders'}:
            self.assertNotIn(other, written)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_sync_only_coupons(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        catalog = self._make_selected_catalog(stream_names=['coupons'])
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, {'bookmarks': {}},
            self.default_start_date)

        written = {c[0][0] for c in mock_write_record.call_args_list}
        self.assertIn('coupons', written)
        self.assertNotIn('orders', written)
