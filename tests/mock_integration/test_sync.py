"""Mock integration test: do_sync() end-to-end pipeline writes schemas,
records, and state correctly."""
import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import tap_bigcommerce

from .base import BigCommerceBaseTest, STREAM_CONFIG, ALL_STREAM_IDS


class DoSyncIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    def setUp(self):
        self.client = self._create_mock_client()
        self.catalog = self._make_selected_catalog()
        self.state = {'bookmarks': {}}

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_full_pipeline_emits_schemas_and_records(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        schema_streams = {c[0][0] for c in mock_write_schema.call_args_list}
        record_streams = {c[0][0] for c in mock_write_record.call_args_list}

        for stream_id in ALL_STREAM_IDS:
            self.assertIn(stream_id, schema_streams)
            self.assertIn(stream_id, record_streams)

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
    def test_schema_emitted_before_records(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        call_order = []
        mock_write_schema.side_effect = lambda *a, **k: call_order.append(('schema', a[0]))
        mock_write_record.side_effect = lambda *a, **k: call_order.append(('record', a[0]))

        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        for stream in ALL_STREAM_IDS:
            schema_idx = [i for i, (t, s) in enumerate(call_order)
                          if t == 'schema' and s == stream]
            record_idx = [i for i, (t, s) in enumerate(call_order)
                          if t == 'record' and s == stream]
            if schema_idx and record_idx:
                self.assertLess(schema_idx[0], record_idx[0],
                                f"Schema for {stream} must come before records")

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_schema_includes_key_properties(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        expected = self.expected_metadata()
        for call in mock_write_schema.call_args_list:
            stream_name = call[0][0]
            key_props = call[0][2]
            if stream_name in expected:
                self.assertEqual(
                    set(key_props),
                    expected[stream_name][self.PRIMARY_KEYS])

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_only_selected_streams_synced(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        catalog = self._make_selected_catalog(stream_names=['orders'])
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, {'bookmarks': {}},
            self.default_start_date)

        record_streams = {c[0][0] for c in mock_write_record.call_args_list}
        self.assertIn('orders', record_streams)
        for other in ALL_STREAM_IDS - {'orders'}:
            self.assertNotIn(other, record_streams)

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_no_streams_selected_writes_nothing(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        catalog = self._make_selected_catalog(stream_names=[])
        tap_bigcommerce.do_sync(
            self._create_mock_client(), catalog, {'bookmarks': {}},
            self.default_start_date)

        mock_write_schema.assert_not_called()
        mock_write_record.assert_not_called()

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_empty_api_responses_no_crash(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        client = MagicMock()
        client.is_authorized.return_value = True
        client.authorized = True
        client.orders = lambda **kw: iter([])
        client.products = lambda **kw: iter([])
        client.customers = lambda **kw: iter([])
        client.coupons = lambda: iter([])

        catalog = self._make_selected_catalog()
        tap_bigcommerce.do_sync(
            client, catalog, {'bookmarks': {}}, self.default_start_date)

        self.assertTrue(mock_write_schema.called)
        mock_write_record.assert_not_called()

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    def test_record_fields_have_correct_types(
        self, mock_write_schema, mock_write_state, mock_write_record,
    ):
        tap_bigcommerce.do_sync(
            self.client, self.catalog, self.state, self.default_start_date)

        for call in mock_write_record.call_args_list:
            stream = call[0][0]
            record = call[0][1]
            with self.subTest(stream=stream):
                self.assertIsInstance(record['id'], int)


class MainEntryPointIntegrationTest(BigCommerceBaseTest, unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tmpdir, 'config.json')
        with open(self.config_path, 'w') as f:
            json.dump(self.default_config, f)

        self.state_path = os.path.join(self.tmpdir, 'state.json')
        with open(self.state_path, 'w') as f:
            json.dump({}, f)

        catalog = self._make_selected_catalog()
        self.catalog_path = os.path.join(self.tmpdir, 'catalog.json')
        with open(self.catalog_path, 'w') as f:
            json.dump(catalog.to_dict(), f)

    @patch("tap_bigcommerce.BigCommerce")
    def test_main_discover_mode(self, MockBC):
        MockBC.return_value = self._create_mock_client()
        with patch('sys.argv', ['tap-bigcommerce',
                                '--config', self.config_path,
                                '--discover']):
            with patch('sys.stdout'):
                tap_bigcommerce.main()

    @patch("tap_bigcommerce.sync.singer.write_record")
    @patch("tap_bigcommerce.sync.singer.write_state")
    @patch("tap_bigcommerce.singer.write_schema")
    @patch("tap_bigcommerce.BigCommerce")
    def test_main_sync_with_catalog(
        self, MockBC, mock_write_schema,
        mock_write_state, mock_write_record,
    ):
        MockBC.return_value = self._create_mock_client()
        with patch('sys.argv', ['tap-bigcommerce',
                                '--config', self.config_path,
                                '--state', self.state_path,
                                '--catalog', self.catalog_path]):
            tap_bigcommerce.main()
