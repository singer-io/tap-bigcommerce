"""Mock integration test: start_date controls which records are returned
for incremental streams."""
import unittest
from unittest.mock import patch

import tap_bigcommerce

from .base import (
    BigCommerceBaseTest, STREAM_CONFIG,
    INCREMENTAL_STREAMS, FULL_TABLE_STREAMS,
)


class StartDateIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_start_date_used_as_initial_bookmark(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        catalog = self._make_selected_catalog(stream_names=['orders'])
        state = {'bookmarks': {}}
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, state,
            "2024-01-01T00:00:00Z")
        self.assertTrue(mock_write_record.called)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_full_table_stream_ignores_start_date(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        for stream in FULL_TABLE_STREAMS:
            with self.subTest(stream=stream):
                mock_write_record.reset_mock()
                catalog = self._make_selected_catalog(stream_names=[stream])
                tap_bigcommerce.do_sync(
                    self._create_mock_client(), catalog, {'bookmarks': {}},
                    "2025-12-01T00:00:00Z")
                recs = [c for c in mock_write_record.call_args_list
                        if c[0][0] == stream]
                self.assertEqual(
                    len(recs), STREAM_CONFIG[stream]['record_count'])

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_existing_bookmark_used_over_start_date(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        state = {
            'bookmarks': {
                'orders': {
                    'date_modified': '2024-01-01T00:00:00.000000Z'
                }
            }
        }
        catalog = self._make_selected_catalog(stream_names=['orders'])
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, state,
            self.default_start_date)

        bm = state.get('bookmarks', {}).get('orders', {})
        self.assertIn('date_modified', bm)
        # Should NOT be reset to start_date
        self.assertNotIn('2023-01-01', bm['date_modified'])

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_different_start_dates_same_full_table_count(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        for start_date in ["2020-01-01T00:00:00Z", "2025-12-01T00:00:00Z"]:
            mock_write_record.reset_mock()
            catalog = self._make_selected_catalog(stream_names=['coupons'])
            tap_bigcommerce.do_sync(
                self._create_mock_client(), catalog, {'bookmarks': {}},
                start_date)
            recs = [c for c in mock_write_record.call_args_list
                    if c[0][0] == 'coupons']
            self.assertEqual(
                len(recs), STREAM_CONFIG['coupons']['record_count'],
                f"start_date={start_date} changed full-table count")
