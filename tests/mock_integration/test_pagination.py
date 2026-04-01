"""Mock integration test: pagination — verify the tap handles all records
from mocked API responses."""
import unittest
from unittest.mock import patch

import tap_bigcommerce

from .base import BigCommerceBaseTest, STREAM_CONFIG, ALL_STREAM_IDS


class PaginationIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self._make_selected_catalog()
        self.state = {'bookmarks': {}}

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_all_records_returned_per_stream(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        """Verify all mock records are written for each stream."""
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        for stream, cfg in STREAM_CONFIG.items():
            with self.subTest(stream=stream):
                recs = [c for c in mock_write_record.call_args_list
                        if c[0][0] == stream]
                self.assertEqual(
                    len(recs), cfg['record_count'],
                    f"{stream}: expected {cfg['record_count']}, got {len(recs)}")

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_each_stream_individually(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        """Sync each stream individually and verify record counts."""
        for stream, cfg in STREAM_CONFIG.items():
            with self.subTest(stream=stream):
                mock_write_record.reset_mock()
                catalog = self._make_selected_catalog(stream_names=[stream])
                tap_bigcommerce.do_sync(
                    self._create_mock_client(), catalog,
                    {'bookmarks': {}}, self.default_start_date)

                recs = [c for c in mock_write_record.call_args_list
                        if c[0][0] == stream]
                self.assertEqual(len(recs), cfg['record_count'])
