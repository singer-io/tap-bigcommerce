"""Unit tests for tap_bigcommerce.__init__ — helper functions."""
import unittest
from unittest.mock import MagicMock, patch

from singer import Catalog, metadata

from tap_bigcommerce import (
    stream_is_selected,
    get_selected_streams,
    populate_class_schemas,
    ensure_credentials_are_authorized,
)
from tap_bigcommerce.streams import STREAMS


class TestStreamIsSelected(unittest.TestCase):

    def test_returns_true_when_selected(self):
        mdata = {(): {'selected': True}}
        self.assertTrue(stream_is_selected(mdata))

    def test_returns_false_when_not_selected(self):
        mdata = {(): {'selected': False}}
        self.assertFalse(stream_is_selected(mdata))

    def test_returns_false_when_missing(self):
        mdata = {(): {}}
        self.assertFalse(stream_is_selected(mdata))

    def test_returns_false_when_empty_mdata(self):
        mdata = {}
        self.assertFalse(stream_is_selected(mdata))


class TestGetSelectedStreams(unittest.TestCase):

    def _make_catalog(self, selected_names):
        streams = []
        for name in ('orders', 'products', 'customers', 'coupons'):
            mdata = [
                {
                    'breadcrumb': (),
                    'metadata': {'selected': name in selected_names},
                }
            ]
            streams.append({
                'stream': name,
                'tap_stream_id': name,
                'schema': {'type': 'object', 'properties': {}},
                'metadata': mdata,
            })
        return Catalog.from_dict({'streams': streams})

    def test_returns_only_selected(self):
        catalog = self._make_catalog(['orders', 'coupons'])
        selected = get_selected_streams(catalog)
        self.assertEqual(set(selected), {'orders', 'coupons'})

    def test_returns_empty_when_none_selected(self):
        catalog = self._make_catalog([])
        selected = get_selected_streams(catalog)
        self.assertEqual(selected, [])

    def test_returns_all_when_all_selected(self):
        catalog = self._make_catalog(['orders', 'products', 'customers', 'coupons'])
        selected = get_selected_streams(catalog)
        self.assertEqual(len(selected), 4)


class TestPopulateClassSchemas(unittest.TestCase):

    def setUp(self):
        # Save originals and clear after test
        self._originals = {}
        for name, cls in STREAMS.items():
            self._originals[name] = getattr(cls, 'stream', None)

    def tearDown(self):
        for name, cls in STREAMS.items():
            cls.stream = self._originals[name]

    def _make_catalog(self, names):
        streams = []
        for name in names:
            streams.append({
                'stream': name,
                'tap_stream_id': name,
                'schema': {'type': 'object', 'properties': {}},
                'metadata': [],
            })
        return Catalog.from_dict({'streams': streams})

    def test_populates_selected_stream_class(self):
        catalog = self._make_catalog(['orders', 'products', 'customers', 'coupons'])
        populate_class_schemas(catalog, ['orders'])
        self.assertIsNotNone(STREAMS['orders'].stream)

    def test_does_not_populate_unselected(self):
        catalog = self._make_catalog(['orders', 'products', 'customers', 'coupons'])
        # Reset all before this test
        for name in STREAMS:
            STREAMS[name].stream = None
        populate_class_schemas(catalog, ['orders'])
        # products was not selected, ensure stream isn't set
        self.assertIsNone(STREAMS['products'].stream)


class TestEnsureCredentialsAreAuthorized(unittest.TestCase):

    def test_raises_when_not_authorized(self):
        client = MagicMock()
        client.is_authorized.return_value = False
        with self.assertRaises(Exception) as ctx:
            ensure_credentials_are_authorized(client)
        self.assertIn('not authorized', str(ctx.exception))

    def test_passes_when_authorized(self):
        client = MagicMock()
        client.is_authorized.return_value = True
        # Should not raise
        ensure_credentials_are_authorized(client)


if __name__ == '__main__':
    unittest.main()
