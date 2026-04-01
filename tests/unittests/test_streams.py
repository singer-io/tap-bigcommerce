import unittest
from unittest.mock import MagicMock, patch, PropertyMock

from tap_bigcommerce.streams import Stream, Orders, Products, Coupons, Customers, STREAMS
from tap_bigcommerce.client import Client
from singer import metadata

from datetime import datetime, timedelta

BIGCOMMERCE_OLD_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S %z"
BIGCOMMERCE_NEW_DATE_FORMAT = "%Y-%m-%d %H:%M:%S %z"


class MockClient(Client):
    pass


# ------------------------------------------------------------------ #
# Stream class attributes / subclass config
# ------------------------------------------------------------------ #

class TestStreamSubclasses(unittest.TestCase):
    """Verify each concrete stream class has correct attributes."""

    def test_orders_attributes(self):
        s = Orders(MockClient)
        self.assertEqual(s.name, 'orders')
        self.assertEqual(s.replication_method, 'INCREMENTAL')
        self.assertEqual(s.replication_key, 'date_modified')
        self.assertEqual(s.key_properties, ['id'])

    def test_products_attributes(self):
        s = Products(MockClient)
        self.assertEqual(s.name, 'products')
        self.assertEqual(s.replication_method, 'INCREMENTAL')
        self.assertEqual(s.replication_key, 'date_modified')

    def test_customers_attributes(self):
        s = Customers(MockClient)
        self.assertEqual(s.name, 'customers')
        self.assertEqual(s.replication_method, 'INCREMENTAL')
        self.assertEqual(s.replication_key, 'date_modified')

    def test_coupons_attributes(self):
        s = Coupons(MockClient)
        self.assertEqual(s.name, 'coupons')
        self.assertEqual(s.replication_method, 'FULL_TABLE')
        self.assertIsNone(s.replication_key)

    def test_streams_dict_has_all_four(self):
        self.assertEqual(set(STREAMS.keys()), {'orders', 'products', 'customers', 'coupons'})

    def test_streams_dict_values_are_classes(self):
        for name, cls in STREAMS.items():
            self.assertTrue(issubclass(cls, Stream))


# ------------------------------------------------------------------ #
# is_bookmark_old
# ------------------------------------------------------------------ #

class TestIsBookmarkOld(unittest.TestCase):

    def setUp(self):
        self.stream = Stream(MockClient)

    def test_value_newer_than_bookmark(self):
        today = datetime.now()
        old_date = today - timedelta(1)
        self.assertTrue(
            self.stream.is_bookmark_old(
                value=today.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
                bookmark=old_date.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
            )
        )

    def test_value_older_than_bookmark(self):
        today = datetime.now()
        old_date = today - timedelta(1)
        self.assertFalse(
            self.stream.is_bookmark_old(
                value=old_date.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
                bookmark=today.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
            )
        )

    def test_cross_format_comparison(self):
        today = datetime.now()
        old_date = today - timedelta(1)
        self.assertTrue(
            self.stream.is_bookmark_old(
                value=today.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
                bookmark=old_date.strftime(BIGCOMMERCE_OLD_DATE_FORMAT),
            )
        )
        self.assertTrue(
            self.stream.is_bookmark_old(
                value=today.strftime(BIGCOMMERCE_OLD_DATE_FORMAT),
                bookmark=old_date.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
            )
        )

    def test_none_value_returns_false(self):
        self.assertFalse(self.stream.is_bookmark_old(None, 'anything'))

    def test_none_bookmark_returns_true(self):
        self.assertTrue(
            self.stream.is_bookmark_old('2024-01-01 00:00:00 +0000', None)
        )

    def test_both_none_returns_false(self):
        self.assertFalse(self.stream.is_bookmark_old(None, None))

    def test_non_date_replication_key_uses_direct_comparison(self):
        """When replication_key is 'id', direct > comparison is used."""
        self.stream.replication_key = 'id'
        self.assertTrue(self.stream.is_bookmark_old(100, 50))
        self.assertFalse(self.stream.is_bookmark_old(50, 100))
        self.assertFalse(self.stream.is_bookmark_old(50, 50))


# ------------------------------------------------------------------ #
# get_bookmark
# ------------------------------------------------------------------ #

class TestGetBookmark(unittest.TestCase):

    def test_returns_bookmark_when_present(self):
        stream = Orders(MockClient)
        state = {'bookmarks': {'orders': {'date_modified': '2024-01-01'}}}
        self.assertEqual(stream.get_bookmark(state), '2024-01-01')

    def test_returns_none_when_absent(self):
        stream = Orders(MockClient)
        state = {'bookmarks': {}}
        self.assertIsNone(stream.get_bookmark(state))

    def test_returns_none_for_empty_state(self):
        stream = Orders(MockClient)
        self.assertIsNone(stream.get_bookmark({}))


# ------------------------------------------------------------------ #
# update_session_bookmark_if_old
# ------------------------------------------------------------------ #

class TestUpdateSessionBookmark(unittest.TestCase):

    def test_sets_initial_session_bookmark(self):
        stream = Stream(MockClient)
        self.assertIsNone(stream.session_bookmark)
        stream.update_session_bookmark_if_old('2024-06-01 00:00:00 +0000')
        self.assertEqual(stream.session_bookmark, '2024-06-01 00:00:00 +0000')

    def test_updates_when_newer(self):
        stream = Stream(MockClient)
        stream.session_bookmark = '2024-01-01 00:00:00 +0000'
        stream.update_session_bookmark_if_old('2024-06-01 00:00:00 +0000')
        self.assertEqual(stream.session_bookmark, '2024-06-01 00:00:00 +0000')

    def test_does_not_update_when_older(self):
        stream = Stream(MockClient)
        stream.session_bookmark = '2024-06-01 00:00:00 +0000'
        stream.update_session_bookmark_if_old('2024-01-01 00:00:00 +0000')
        self.assertEqual(stream.session_bookmark, '2024-06-01 00:00:00 +0000')


# ------------------------------------------------------------------ #
# update_bookmark_if_old
# ------------------------------------------------------------------ #

class TestUpdateBookmarkIfOld(unittest.TestCase):

    def test_writes_bookmark_when_session_is_newer(self):
        stream = Orders(MockClient)
        stream.session_bookmark = '2024-06-01T00:00:00.000000Z'
        state = {'bookmarks': {'orders': {'date_modified': '2024-01-01T00:00:00.000000Z'}}}
        stream.update_bookmark_if_old(state)
        self.assertEqual(
            state['bookmarks']['orders']['date_modified'],
            '2024-06-01T00:00:00.000000Z',
        )

    def test_does_not_write_when_session_is_older(self):
        stream = Orders(MockClient)
        stream.session_bookmark = '2024-01-01T00:00:00.000000Z'
        state = {'bookmarks': {'orders': {'date_modified': '2024-06-01T00:00:00.000000Z'}}}
        stream.update_bookmark_if_old(state)
        self.assertEqual(
            state['bookmarks']['orders']['date_modified'],
            '2024-06-01T00:00:00.000000Z',
        )


# ------------------------------------------------------------------ #
# is_selected
# ------------------------------------------------------------------ #

class TestIsSelected(unittest.TestCase):

    def test_not_selected_when_stream_is_none(self):
        stream = Stream(MockClient)
        self.assertFalse(stream.is_selected())

    def test_selected_when_stream_set(self):
        stream = Stream(MockClient)
        stream.stream = MagicMock()
        self.assertTrue(stream.is_selected())


# ------------------------------------------------------------------ #
# load_schema / load_metadata
# ------------------------------------------------------------------ #

class TestLoadSchema(unittest.TestCase):

    def test_load_orders_schema_has_properties(self):
        stream = Orders(MockClient)
        schema = stream.load_schema()
        self.assertIn('properties', schema)
        self.assertIn('id', schema['properties'])
        self.assertIn('date_modified', schema['properties'])

    def test_load_products_schema(self):
        stream = Products(MockClient)
        schema = stream.load_schema()
        self.assertIn('properties', schema)
        self.assertIn('id', schema['properties'])

    def test_load_customers_schema(self):
        stream = Customers(MockClient)
        schema = stream.load_schema()
        self.assertIn('properties', schema)
        self.assertIn('id', schema['properties'])

    def test_load_coupons_schema(self):
        stream = Coupons(MockClient)
        schema = stream.load_schema()
        self.assertIn('properties', schema)
        self.assertIn('id', schema['properties'])


class TestLoadMetadata(unittest.TestCase):

    def test_orders_metadata_has_key_properties(self):
        stream = Orders(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertEqual(mdata[()]['table-key-properties'], ['id'])

    def test_orders_metadata_has_replication_method(self):
        stream = Orders(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertEqual(mdata[()]['forced-replication-method'], 'INCREMENTAL')

    def test_orders_metadata_has_valid_replication_keys(self):
        stream = Orders(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertEqual(mdata[()]['valid-replication-keys'], ['date_modified'])

    def test_coupons_metadata_no_replication_keys(self):
        stream = Coupons(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertNotIn('valid-replication-keys', mdata[()])

    def test_orders_id_field_is_automatic(self):
        stream = Orders(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertEqual(
            mdata[('properties', 'id')]['inclusion'], 'automatic'
        )

    def test_orders_date_modified_is_automatic(self):
        stream = Orders(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertEqual(
            mdata[('properties', 'date_modified')]['inclusion'], 'automatic'
        )

    def test_orders_non_key_field_is_available(self):
        stream = Orders(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertEqual(
            mdata[('properties', 'status')]['inclusion'], 'available'
        )

    def test_selected_by_default_is_true(self):
        stream = Orders(MockClient)
        mdata_list = stream.load_metadata()
        mdata = metadata.to_map(mdata_list)
        self.assertTrue(mdata[()]['selected-by-default'])


# ------------------------------------------------------------------ #
# sync (unit-level: mocked client)
# ------------------------------------------------------------------ #

class TestStreamSyncIncremental(unittest.TestCase):

    def _make_stream_with_mock_client(self):
        client = MagicMock()
        client.orders = MagicMock(return_value=iter([
            {'id': 1, 'date_modified': '2024-03-01T00:00:00.000000Z'},
            {'id': 2, 'date_modified': '2024-06-01T00:00:00.000000Z'},
        ]))
        stream = Orders(client)
        stream.stream = MagicMock()
        return stream

    def test_yields_records_newer_than_bookmark(self):
        stream = self._make_stream_with_mock_client()
        state = {'bookmarks': {'orders': {'date_modified': '2024-01-01T00:00:00.000000Z'}}}
        results = list(stream.sync(state))
        self.assertEqual(len(results), 2)

    def test_filters_records_at_or_before_bookmark(self):
        stream = self._make_stream_with_mock_client()
        state = {'bookmarks': {'orders': {'date_modified': '2024-05-01T00:00:00.000000Z'}}}
        results = list(stream.sync(state))
        # Only the record with 2024-06-01 is newer than 2024-05-01
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1]['id'], 2)

    def test_updates_session_bookmark(self):
        stream = self._make_stream_with_mock_client()
        state = {'bookmarks': {'orders': {'date_modified': '2024-01-01T00:00:00.000000Z'}}}
        list(stream.sync(state))
        self.assertEqual(stream.session_bookmark, '2024-06-01T00:00:00.000000Z')

    def test_updates_state_bookmark(self):
        stream = self._make_stream_with_mock_client()
        state = {'bookmarks': {'orders': {'date_modified': '2024-01-01T00:00:00.000000Z'}}}
        list(stream.sync(state))
        self.assertEqual(
            state['bookmarks']['orders']['date_modified'],
            '2024-06-01T00:00:00.000000Z',
        )

    def test_yields_stream_and_record_tuple(self):
        stream = self._make_stream_with_mock_client()
        state = {'bookmarks': {'orders': {'date_modified': '2024-01-01T00:00:00.000000Z'}}}
        results = list(stream.sync(state))
        for s, record in results:
            self.assertIs(s, stream.stream)
            self.assertIn('id', record)

    def test_handles_exception_in_record_gracefully(self):
        """If a record is missing the replication_key, the error is caught."""
        client = MagicMock()
        client.orders = MagicMock(return_value=iter([
            {'id': 1},  # missing date_modified
            {'id': 2, 'date_modified': '2024-06-01T00:00:00.000000Z'},
        ]))
        stream = Orders(client)
        stream.stream = MagicMock()
        state = {'bookmarks': {'orders': {'date_modified': '2024-01-01T00:00:00.000000Z'}}}
        results = list(stream.sync(state))
        # Only the second record should be yielded
        self.assertEqual(len(results), 1)


class TestStreamSyncFullTable(unittest.TestCase):

    def test_yields_all_records(self):
        client = MagicMock()
        client.coupons = MagicMock(return_value=iter([
            {'id': 1, 'name': 'A'},
            {'id': 2, 'name': 'B'},
        ]))
        stream = Coupons(client)
        stream.stream = MagicMock()
        state = {}
        results = list(stream.sync(state))
        self.assertEqual(len(results), 2)

    def test_yields_stream_and_record_tuple(self):
        client = MagicMock()
        client.coupons = MagicMock(return_value=iter([
            {'id': 1, 'name': 'A'},
        ]))
        stream = Coupons(client)
        stream.stream = MagicMock()
        results = list(stream.sync({}))
        s, record = results[0]
        self.assertIs(s, stream.stream)
        self.assertEqual(record['id'], 1)


class TestStreamSyncUnknownMethod(unittest.TestCase):

    def test_raises_on_unknown_replication_method(self):
        client = MagicMock()
        stream = Stream(client)
        stream.name = 'test'
        stream.replication_method = 'UNKNOWN'
        stream.stream = MagicMock()
        with self.assertRaises(Exception) as ctx:
            list(stream.sync({}))
        self.assertIn('Replication method not defined', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()