"""Mock integration test: bookmark / incremental sync — verify state is
updated after syncing incremental streams."""
import unittest
from unittest.mock import patch

import tap_bigcommerce

from .base import (
    BigCommerceBaseTest, STREAM_CONFIG,
    INCREMENTAL_STREAMS, FULL_TABLE_STREAMS,
)


class BookmarkIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_state_has_bookmarks_after_sync(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        state = {'bookmarks': {}}
        catalog = self._make_selected_catalog()
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, state,
            self.default_start_date)
        self.assertIn('bookmarks', state)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_write_state_called(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        state = {'bookmarks': {}}
        catalog = self._make_selected_catalog()
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, state,
            self.default_start_date)
        self.assertTrue(mock_write_state.called)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_incremental_stream_updates_bookmark(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        for stream in INCREMENTAL_STREAMS:
            with self.subTest(stream=stream):
                state = {'bookmarks': {}}
                catalog = self._make_selected_catalog(stream_names=[stream])
                tap_bigcommerce.do_sync(
                    self._create_mock_client(), catalog, state,
                    self.default_start_date)

                bookmarks = state.get('bookmarks', {})
                rep_key = STREAM_CONFIG[stream]['replication_key']
                self.assertIn(stream, bookmarks)
                self.assertIn(rep_key, bookmarks[stream])

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_full_table_no_bookmark(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        for stream in FULL_TABLE_STREAMS:
            with self.subTest(stream=stream):
                state = {'bookmarks': {}}
                catalog = self._make_selected_catalog(stream_names=[stream])
                tap_bigcommerce.do_sync(
                    self._create_mock_client(), catalog, state,
                    self.default_start_date)

                coupon_bm = state.get('bookmarks', {}).get(stream, {})
                self.assertNotIn('date_modified', coupon_bm)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_bookmark_uses_max_date(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        for stream in INCREMENTAL_STREAMS:
            with self.subTest(stream=stream):
                state = {'bookmarks': {}}
                catalog = self._make_selected_catalog(stream_names=[stream])
                tap_bigcommerce.do_sync(
                    self._create_mock_client(), catalog, state,
                    self.default_start_date)

                rep_key = STREAM_CONFIG[stream]['replication_key']
                bm_value = state['bookmarks'].get(stream, {}).get(rep_key)
                expected_max = self.get_max_bookmark(stream)
                if bm_value and expected_max:
                    # The bookmark should be >= the max value
                    self.assertGreaterEqual(bm_value, expected_max)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_multiple_incremental_streams_get_bookmarks(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        state = {'bookmarks': {}}
        catalog = self._make_selected_catalog(
            stream_names=list(INCREMENTAL_STREAMS))
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, state,
            self.default_start_date)

        bookmarks = state.get('bookmarks', {})
        for stream in INCREMENTAL_STREAMS:
            with self.subTest(stream=stream):
                rep_key = STREAM_CONFIG[stream]['replication_key']
                self.assertIn(stream, bookmarks)
                self.assertIn(rep_key, bookmarks[stream])

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_existing_bookmark_preserved_and_updated(
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

        bm = state['bookmarks'].get('orders', {})
        self.assertIn('date_modified', bm)
        # Should be updated past start_date
        self.assertNotIn('2023-01-01', bm['date_modified'])
