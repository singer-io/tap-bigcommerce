import unittest
from unittest.mock import MagicMock

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
        """When replication_key is 'id', direct >= comparison is used.
        Equal values return True (bookmark is considered old) to avoid missing
        records at the boundary."""
        self.stream.replication_key = 'id'
        self.assertTrue(self.stream.is_bookmark_old(100, 50))
        self.assertFalse(self.stream.is_bookmark_old(50, 100))
        self.assertTrue(self.stream.is_bookmark_old(50, 50))


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
# load_field_metadata
# ------------------------------------------------------------------ #

class TestLoadFieldMetadata(unittest.TestCase):
    """
    Direct unit tests for Stream.load_field_metadata.
    """

    def _flat_schema(self, *fields):
        """Return a minimal schema dict whose properties are the given field names."""
        return {'properties': {f: {'type': 'string'} for f in fields}}

    # ── inclusion correctness ──────────────────────────────────────────────

    def test_key_property_marked_automatic(self):
        """A field listed in key_properties must receive inclusion='automatic'."""
        stream = Orders(MockClient)  # key_properties=['id']
        mdata = metadata.new()
        result = stream.load_field_metadata(mdata, self._flat_schema('id', 'status'))
        mdata_map = metadata.to_map(metadata.to_list(result))
        self.assertEqual(mdata_map[('properties', 'id')]['inclusion'], 'automatic')

    def test_replication_key_marked_automatic(self):
        """The replication_key field must receive inclusion='automatic'."""
        stream = Orders(MockClient)  # replication_key='date_modified'
        mdata = metadata.new()
        result = stream.load_field_metadata(
            mdata, self._flat_schema('id', 'date_modified', 'status')
        )
        mdata_map = metadata.to_map(metadata.to_list(result))
        self.assertEqual(mdata_map[('properties', 'date_modified')]['inclusion'], 'automatic')

    def test_non_key_field_marked_available(self):
        """Regular fields (not key / replication) must receive inclusion='available'."""
        stream = Orders(MockClient)
        mdata = metadata.new()
        result = stream.load_field_metadata(
            mdata, self._flat_schema('id', 'date_modified', 'status', 'total_inc_tax')
        )
        mdata_map = metadata.to_map(metadata.to_list(result))
        self.assertEqual(mdata_map[('properties', 'status')]['inclusion'], 'available')
        self.assertEqual(mdata_map[('properties', 'total_inc_tax')]['inclusion'], 'available')

    def test_all_properties_receive_inclusion_entry(self):
        """Every property in the schema must get an inclusion metadata entry."""
        stream = Orders(MockClient)
        fields = ['id', 'date_modified', 'status', 'customer_id', 'cart_id']
        mdata = metadata.new()
        result = stream.load_field_metadata(mdata, self._flat_schema(*fields))
        mdata_map = metadata.to_map(metadata.to_list(result))
        for f in fields:
            with self.subTest(field=f):
                self.assertIn(('properties', f), mdata_map,
                              msg=f"Field '{f}' missing from mdata")
                self.assertIn('inclusion', mdata_map[('properties', f)])

    def test_full_table_stream_only_key_property_is_automatic(self):
        """
        Coupons has replication_key=None.
        Only key_properties entries should be automatic; everything else available.
        """
        stream = Coupons(MockClient)  # key_properties=['id'], replication_key=None
        self.assertIsNone(stream.replication_key)
        mdata = metadata.new()
        result = stream.load_field_metadata(mdata, self._flat_schema('id', 'code', 'amount'))
        mdata_map = metadata.to_map(metadata.to_list(result))
        self.assertEqual(mdata_map[('properties', 'id')]['inclusion'], 'automatic')
        self.assertEqual(mdata_map[('properties', 'code')]['inclusion'], 'available')
        self.assertEqual(mdata_map[('properties', 'amount')]['inclusion'], 'available')

    def test_none_replication_key_does_not_accidentally_mark_fields(self):
        """
        When replication_key is None, the comparison `field_name == None`
        must never mark a real string field as automatic.
        """
        stream = Coupons(MockClient)
        mdata = metadata.new()
        result = stream.load_field_metadata(mdata, self._flat_schema('id', 'code'))
        mdata_map = metadata.to_map(metadata.to_list(result))
        self.assertEqual(mdata_map[('properties', 'code')]['inclusion'], 'available')

    def test_multiple_key_properties_all_automatic(self):
        """All fields in key_properties must be marked automatic."""
        stream = Orders(MockClient)
        stream.key_properties = ['id', 'customer_id']
        mdata = metadata.new()
        result = stream.load_field_metadata(
            mdata, self._flat_schema('id', 'customer_id', 'status')
        )
        mdata_map = metadata.to_map(metadata.to_list(result))
        self.assertEqual(mdata_map[('properties', 'id')]['inclusion'], 'automatic')
        self.assertEqual(mdata_map[('properties', 'customer_id')]['inclusion'], 'automatic')
        self.assertEqual(mdata_map[('properties', 'status')]['inclusion'], 'available')

    # ── behaviour of no recursion ─────────────────────────────────

    def test_nested_object_fields_not_recursed_into(self):
        """
        The new implementation does NOT recurse into nested
        object schemas.  Sub-fields of nested objects must have no breadcrumb
        entries in mdata.
        """
        stream = Orders(MockClient)
        mdata = metadata.new()
        schema = {
            'properties': {
                'id': {'type': 'integer'},
                'billing_address': {
                    'type': 'object',
                    'properties': {
                        'street_1': {'type': 'string'},
                        'city':     {'type': 'string'},
                    },
                },
            }
        }
        result = stream.load_field_metadata(mdata, schema)
        mdata_map = metadata.to_map(metadata.to_list(result))

        # Top-level breadcrumbs must exist
        self.assertIn(('properties', 'id'), mdata_map)
        self.assertIn(('properties', 'billing_address'), mdata_map)

        # Nested breadcrumbs must NOT exist (new flat behaviour)
        self.assertNotIn(
            ('properties', 'billing_address', 'properties', 'street_1'), mdata_map
        )
        self.assertNotIn(
            ('properties', 'billing_address', 'properties', 'city'), mdata_map
        )

    def test_array_field_not_recursed_into(self):
        """
        The new implementation does NOT recurse into array-typed
        fields; no items-level breadcrumbs must be written.
        """
        stream = Orders(MockClient)
        mdata = metadata.new()
        schema = {
            'properties': {
                'id': {'type': 'integer'},
                'products': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'product_id': {'type': 'integer'},
                        },
                    },
                },
            }
        }
        result = stream.load_field_metadata(mdata, schema)
        mdata_map = metadata.to_map(metadata.to_list(result))

        self.assertIn(('properties', 'products'), mdata_map)
        # items-level breadcrumb must NOT be present
        self.assertNotIn(
            ('properties', 'products', 'items', 'properties', 'product_id'), mdata_map
        )

    def test_schema_without_type_key_is_handled(self):
        """
        The new implementation does not gate on schema['type'], so schemas
        that omit the type key at root level must not raise an error.
        """
        stream = Orders(MockClient)
        mdata = metadata.new()
        schema = {'properties': {'id': {}, 'status': {}}}  # no 'type' key
        result = stream.load_field_metadata(mdata, schema)
        mdata_map = metadata.to_map(metadata.to_list(result))
        self.assertEqual(mdata_map[('properties', 'id')]['inclusion'], 'automatic')
        self.assertEqual(mdata_map[('properties', 'status')]['inclusion'], 'available')

    def test_parent_argument_ignored_breadcrumb_always_root(self):
        """
        The parent parameter is kept in the signature for compatibility but
        is no longer used.  Passing a non-empty parent must not change the
        resulting breadcrumbs — they are always written at root level.
        """
        stream = Orders(MockClient)
        mdata = metadata.new()
        schema = self._flat_schema('id', 'status')
        result = stream.load_field_metadata(mdata, schema, parent=('properties', 'nested'))
        mdata_map = metadata.to_map(metadata.to_list(result))
        # Breadcrumbs are still at root level, not under the provided parent
        self.assertIn(('properties', 'id'), mdata_map)
        self.assertIn(('properties', 'status'), mdata_map)
        self.assertNotIn(('properties', 'nested', 'properties', 'id'), mdata_map)

    # ── return value / mdata preservation ─────────────────────────────────

    def test_returns_updated_mdata(self):
        """load_field_metadata must return a non-None mdata object."""
        stream = Orders(MockClient)
        mdata = metadata.new()
        result = stream.load_field_metadata(mdata, self._flat_schema('id'))
        self.assertIsNotNone(result)
        self.assertIn(('properties', 'id'), metadata.to_map(metadata.to_list(result)))

    def test_existing_mdata_entries_are_preserved(self):
        """
        Pre-existing entries in the passed mdata must not be removed when new
        field-level entries are added.
        """
        stream = Orders(MockClient)
        mdata = metadata.new()
        mdata = metadata.write(mdata, (), 'table-key-properties', ['id'])
        result = stream.load_field_metadata(mdata, self._flat_schema('id', 'status'))
        mdata_map = metadata.to_map(metadata.to_list(result))
        # Pre-existing root entry must still be present
        self.assertEqual(mdata_map[()]['table-key-properties'], ['id'])
        # New field entries must also be present
        self.assertIn(('properties', 'id'), mdata_map)
        self.assertIn(('properties', 'status'), mdata_map)


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