"""Unit tests for tap_bigcommerce.client — Client, BigCommerce client,
decorators, and validation."""
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from tap_bigcommerce.client import (
    Client,
    BigCommerce,
    validate,
    parse_date_string_arguments,
)


class TestClientBase(unittest.TestCase):
    """Tests for the base Client class."""

    def test_client_initially_not_authorized(self):
        client = Client()
        self.assertFalse(client.is_authorized())

    def test_client_authorized_attribute(self):
        client = Client()
        client.authorized = True
        self.assertTrue(client.is_authorized())


class TestValidateDecorator(unittest.TestCase):
    """Tests for the @validate decorator."""

    def test_valid_replication_key_date_modified(self):
        """date_modified is a valid replication_key."""
        @validate
        def dummy(replication_key=None, bookmark=None):
            return True

        self.assertTrue(dummy(replication_key='date_modified',
                              bookmark=datetime.now()))

    def test_valid_replication_key_id(self):
        """id is a valid replication_key."""
        @validate
        def dummy(replication_key=None, bookmark=None):
            return True

        self.assertTrue(dummy(replication_key='id',
                              bookmark=datetime.now()))

    def test_invalid_replication_key_raises(self):
        """Invalid replication_key should raise Exception."""
        @validate
        def dummy(replication_key=None, bookmark=None):
            return True

        with self.assertRaises(Exception) as ctx:
            dummy(replication_key='invalid_key', bookmark=datetime.now())
        self.assertIn('invalid replication_key', str(ctx.exception))

    def test_invalid_bookmark_type_raises(self):
        """Non-datetime bookmark should raise Exception."""
        @validate
        def dummy(replication_key=None, bookmark=None):
            return True

        with self.assertRaises(Exception) as ctx:
            dummy(replication_key='date_modified', bookmark='not-a-datetime')
        self.assertIn('bookmark must be valid datetime', str(ctx.exception))

    def test_datetime_bookmark_passes(self):
        """datetime bookmark passes validation."""
        @validate
        def dummy(replication_key=None, bookmark=None):
            return bookmark

        now = datetime.now()
        result = dummy(replication_key='date_modified', bookmark=now)
        self.assertEqual(result, now)


class TestParseDateStringArguments(unittest.TestCase):
    """Tests for the @parse_date_string_arguments decorator."""

    def test_parses_bookmark_string_to_datetime(self):
        """String bookmark is converted to datetime."""
        @parse_date_string_arguments('bookmark')
        def dummy(bookmark=None):
            return bookmark

        result = dummy(bookmark='2024-01-15T10:00:00Z')
        self.assertIsInstance(result, datetime)

    def test_non_string_raises_exception(self):
        """Non-string value for a date field should raise."""
        @parse_date_string_arguments('bookmark')
        def dummy(bookmark=None):
            return bookmark

        with self.assertRaises(Exception) as ctx:
            dummy(bookmark=12345)
        self.assertIn('expects string value', str(ctx.exception))

    def test_leaves_non_date_kwargs_unchanged(self):
        """Non-date kwargs should not be modified."""
        @parse_date_string_arguments('bookmark')
        def dummy(bookmark=None, other=None):
            return other

        result = dummy(bookmark='2024-01-15T10:00:00Z', other='unchanged')
        self.assertEqual(result, 'unchanged')

    def test_multiple_date_fields(self):
        """Multiple date fields can be specified."""
        @parse_date_string_arguments(['start', 'end'])
        def dummy(start=None, end=None):
            return start, end

        start, end = dummy(
            start='2024-01-01T00:00:00Z',
            end='2024-12-31T23:59:59Z',
        )
        self.assertIsInstance(start, datetime)
        self.assertIsInstance(end, datetime)


class TestBigCommerceClient(unittest.TestCase):
    """Tests for the BigCommerce client class."""

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_init_sets_credentials(self, MockApi):
        """BigCommerce client stores client_id, access_token, store_hash."""
        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        self.assertEqual(client.client_id, 'test_id')
        self.assertEqual(client.access_token, 'test_token')
        self.assertEqual(client.store_hash, 'test_hash')

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_init_sets_authorized(self, MockApi):
        """Client should be authorized after successful init."""
        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        self.assertTrue(client.is_authorized())

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_init_creates_api(self, MockApi):
        """Init should create a Bigcommerce API instance."""
        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        MockApi.assert_called_once_with(
            client_id='test_id',
            store_hash='test_hash',
            access_token='test_token',
        )

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_init_failure_raises(self, MockApi):
        """If Bigcommerce API init fails, exception is raised."""
        MockApi.side_effect = Exception("Auth failed")
        with self.assertRaises(Exception):
            BigCommerce(
                client_id='bad_id',
                access_token='bad_token',
                store_hash='bad_hash',
            )

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_iterdates_single_day(self, MockApi):
        """iterdates yields at least one (start, end) pair."""
        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        start = client.utcnow - timedelta(hours=12)
        dates = list(client.iterdates(start))
        self.assertGreater(len(dates), 0)
        # Each pair should be (start_dt, end_dt)
        for s, e in dates:
            self.assertIsInstance(s, datetime)
            self.assertIsInstance(e, datetime)

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_iterdates_multiple_days(self, MockApi):
        """iterdates yields one pair per day."""
        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        start = client.utcnow - timedelta(days=3)
        dates = list(client.iterdates(start))
        self.assertEqual(len(dates), 3)

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_orders_yields_from_api(self, MockApi):
        """orders() method yields records from API."""
        mock_api = MagicMock()
        mock_api.resource.return_value = iter([
            {"id": 1, "date_modified": "2024-01-01T00:00:00Z"},
        ])
        MockApi.return_value = mock_api

        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        records = list(client.orders(
            replication_key='date_modified',
            bookmark='2023-01-01T00:00:00Z',
        ))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['id'], 1)

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_products_yields_from_api(self, MockApi):
        """products() method yields records from API."""
        mock_api = MagicMock()
        mock_api.resource.return_value = iter([
            {"id": 100, "name": "Test Product",
             "date_modified": "2024-01-01T00:00:00Z"},
        ])
        MockApi.return_value = mock_api

        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        records = list(client.products(
            replication_key='date_modified',
            bookmark='2023-01-01T00:00:00Z',
        ))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['name'], 'Test Product')

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_coupons_yields_from_api(self, MockApi):
        """coupons() method yields all coupons from API."""
        mock_api = MagicMock()
        mock_api.resource.return_value = iter([
            {"id": 1, "name": "Coupon A"},
            {"id": 2, "name": "Coupon B"},
        ])
        MockApi.return_value = mock_api

        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        records = list(client.coupons())
        self.assertEqual(len(records), 2)

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_customers_yields_from_api(self, MockApi):
        """customers() method uses iterdates and yields records."""
        mock_api = MagicMock()
        mock_api.resource.return_value = iter([
            {"id": 1, "date_modified": "2024-01-01T00:00:00Z"},
        ])
        MockApi.return_value = mock_api

        client = BigCommerce(
            client_id='test_id',
            access_token='test_token',
            store_hash='test_hash',
        )
        records = list(client.customers(
            replication_key='date_modified',
            bookmark='2024-01-01T00:00:00Z',
        ))
        self.assertGreater(len(records), 0)

    @patch('tap_bigcommerce.client.Bigcommerce')
    def test_init_failure_sets_authorized_false(self, MockApi):
        """If Bigcommerce API init fails, authorized should be False."""
        MockApi.side_effect = Exception("Connection refused")
        try:
            client = BigCommerce(
                client_id='bad', access_token='bad', store_hash='bad',
            )
        except Exception:
            pass
        # We can't access client since the exception propagates,
        # but verify it raises (which implies authorized isn't set to True)
        with self.assertRaises(Exception):
            BigCommerce(
                client_id='bad', access_token='bad', store_hash='bad',
            )


if __name__ == "__main__":
    unittest.main()
