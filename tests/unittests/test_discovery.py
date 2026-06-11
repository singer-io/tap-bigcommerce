"""
Unit tests for tap_bigcommerce discovery access-check logic.
"""
import unittest
from unittest.mock import MagicMock, patch
from requests.exceptions import HTTPError

from tap_bigcommerce.bigcommerce import BigCommerceForbiddenError, Bigcommerce
from tap_bigcommerce.discover import _apply_access_checks, discover_streams
from tap_bigcommerce.streams import STREAMS, Orders, Products, Coupons, Customers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_stream(name, accessible=True):
    """Return a MagicMock that mimics a Stream instance."""
    s = MagicMock()
    s.name = name
    s.check_access.return_value = accessible
    return s


def _make_mock_client(forbidden_streams=None):
    """
    Return a MagicMock BigCommerce client.

    `forbidden_streams` is a set of stream names whose api.get() will raise
    BigCommerceForbiddenError; all others succeed.
    """
    forbidden_streams = forbidden_streams or set()

    client = MagicMock()

    # Replicate the real endpoints dict so make_url / endpoints lookups work
    client.api.endpoints = {
        'orders': {'version': 2, 'path': 'orders'},
        'products': {'version': 3, 'path': 'catalog/products'},
        'customers': {'version': 2, 'path': 'customers'},
        'coupons': {'version': 2, 'path': 'coupons'},
    }

    # make_url returns a predictable string
    client.api.make_url.side_effect = lambda version, path: (
        f"https://api.bigcommerce.com/stores/test/v{version}/{path}"
    )

    return client


# ---------------------------------------------------------------------------
# _apply_access_checks()
# ---------------------------------------------------------------------------

class TestApplyAccessChecks(unittest.TestCase):

    def _all_accessible(self):
        return [_make_mock_stream(name, accessible=True) for name in STREAMS]

    def _with_forbidden(self, *forbidden_names):
        return [
            _make_mock_stream(name, accessible=(name not in forbidden_names))
            for name in STREAMS
        ]

    # 1. All accessible → all returned
    def test_all_accessible_returns_all_instances(self):
        client = MagicMock()
        instances = self._all_accessible()
        result = _apply_access_checks(client, instances)
        self.assertEqual(len(result), len(instances))
        self.assertEqual({s.name for s in result}, set(STREAMS.keys()))

    # 2. One inaccessible → excluded
    def test_one_inaccessible_is_excluded(self):
        client = MagicMock()
        instances = self._with_forbidden('orders')
        result = _apply_access_checks(client, instances)
        result_names = {s.name for s in result}
        self.assertNotIn('orders', result_names)
        self.assertEqual(len(result), len(STREAMS) - 1)

    # 3. Multiple inaccessible → all excluded
    def test_multiple_inaccessible_all_excluded(self):
        client = MagicMock()
        instances = self._with_forbidden('orders', 'products')
        result = _apply_access_checks(client, instances)
        result_names = {s.name for s in result}
        self.assertNotIn('orders', result_names)
        self.assertNotIn('products', result_names)
        self.assertEqual(len(result), len(STREAMS) - 2)

    # 4. All inaccessible → BigCommerceForbiddenError
    def test_all_inaccessible_raises_forbidden_error(self):
        client = MagicMock()
        instances = [_make_mock_stream(name, accessible=False) for name in STREAMS]
        with self.assertRaises(BigCommerceForbiddenError):
            _apply_access_checks(client, instances)

    # 5. Returns only accessible instances (identity check)
    def test_returns_accessible_instances_in_order(self):
        client = MagicMock()
        instances = self._with_forbidden('coupons')
        result = _apply_access_checks(client, instances)
        # Accessible instances should be the exact objects (same mock identity)
        accessible_originals = [s for s in instances if s.name != 'coupons']
        self.assertEqual(result, accessible_originals)

    # 6. Warning logged for excluded stream
    def test_warning_logged_for_excluded_stream(self):
        client = MagicMock()
        instances = self._with_forbidden('customers')
        with patch('tap_bigcommerce.discover.LOGGER') as mock_logger:
            _apply_access_checks(client, instances)
            mock_logger.warning.assert_called_once()
            warning_msg = ' '.join(str(a) for a in mock_logger.warning.call_args[0])
            self.assertIn('customers', warning_msg)

    # 7. No warning when all accessible
    def test_no_warning_when_all_accessible(self):
        client = MagicMock()
        instances = self._all_accessible()
        with patch('tap_bigcommerce.discover.LOGGER') as mock_logger:
            _apply_access_checks(client, instances)
            mock_logger.warning.assert_not_called()

    # Forbidden error message contains useful text
    def test_forbidden_error_message_mentions_permissions(self):
        client = MagicMock()
        instances = [_make_mock_stream(name, accessible=False) for name in STREAMS]
        with self.assertRaises(BigCommerceForbiddenError) as ctx:
            _apply_access_checks(client, instances)
        self.assertIn('403', str(ctx.exception))


# ---------------------------------------------------------------------------
# discover_streams()
# ---------------------------------------------------------------------------

class TestDiscoverStreams(unittest.TestCase):
    """discover_streams() must respect access checks and return valid catalog."""

    # 8. All accessible → all 4 streams in result
    def test_all_accessible_returns_all_streams(self):
        client = MagicMock()
        with patch('tap_bigcommerce.discover._apply_access_checks',
                   side_effect=lambda c, instances: instances):
            result = discover_streams(client)
        self.assertEqual(len(result['streams']), 4)

    # 9. Inaccessible stream excluded
    def test_inaccessible_stream_excluded_from_catalog(self):
        client = MagicMock()

        def _filter_orders(c, instances):
            return [s for s in instances if s.name != 'orders']

        with patch('tap_bigcommerce.discover._apply_access_checks', side_effect=_filter_orders):
            result = discover_streams(client)

        names = {s['stream'] for s in result['streams']}
        self.assertNotIn('orders', names)
        self.assertEqual(len(result['streams']), 3)

    # 10. All inaccessible raises BigCommerceForbiddenError
    def test_all_inaccessible_raises(self):
        client = MagicMock()
        with patch('tap_bigcommerce.discover._apply_access_checks',
                   side_effect=BigCommerceForbiddenError("all forbidden")):
            with self.assertRaises(BigCommerceForbiddenError):
                discover_streams(client)

    # 11. Result structure has required keys
    def test_result_has_required_keys(self):
        client = MagicMock()
        with patch('tap_bigcommerce.discover._apply_access_checks',
                   side_effect=lambda c, instances: instances):
            result = discover_streams(client)
        self.assertIn('streams', result)
        for s in result['streams']:
            with self.subTest(stream=s['stream']):
                self.assertIn('stream', s)
                self.assertIn('tap_stream_id', s)
                self.assertIn('schema', s)
                self.assertIn('metadata', s)

    # stream equals tap_stream_id
    def test_stream_equals_tap_stream_id(self):
        client = MagicMock()
        with patch('tap_bigcommerce.discover._apply_access_checks',
                   side_effect=lambda c, instances: instances):
            result = discover_streams(client)
        for s in result['streams']:
            self.assertEqual(s['stream'], s['tap_stream_id'])

    # _apply_access_checks is called with client and stream instances
    def test_apply_access_checks_called_with_client(self):
        client = MagicMock()
        with patch('tap_bigcommerce.discover._apply_access_checks',
                   side_effect=lambda c, instances: instances) as mock_check:
            discover_streams(client)
        mock_check.assert_called_once()
        call_client, call_instances = mock_check.call_args[0]
        self.assertIs(call_client, client)
        self.assertEqual(len(call_instances), len(STREAMS))


# ---------------------------------------------------------------------------
# Stream.check_access()
# ---------------------------------------------------------------------------

class TestStreamCheckAccess(unittest.TestCase):
    """Stream.check_access() must probe the API and handle 403 gracefully."""

    def _make_stream_instance(self, stream_cls, forbidden=False, error=None):
        """Create a stream instance with a mock client."""
        client = MagicMock()
        client.api.endpoints = {
            'orders': {'version': 2, 'path': 'orders'},
            'products': {'version': 3, 'path': 'catalog/products'},
            'customers': {'version': 2, 'path': 'customers'},
            'coupons': {'version': 2, 'path': 'coupons'},
        }
        client.api.make_url.side_effect = lambda v, p: f"https://api.test/v{v}/{p}"

        if forbidden:
            client.api.get.side_effect = BigCommerceForbiddenError("403")
        elif error:
            client.api.get.side_effect = error
        else:
            client.api.get.return_value = MagicMock()

        return stream_cls(client)

    # 12. Returns True when API succeeds
    def test_returns_true_when_accessible(self):
        for stream_cls in [Orders, Products, Coupons, Customers]:
            with self.subTest(stream=stream_cls.name):
                instance = self._make_stream_instance(stream_cls, forbidden=False)
                self.assertTrue(instance.check_access())

    # 13. Returns False on BigCommerceForbiddenError
    def test_returns_false_on_forbidden(self):
        for stream_cls in [Orders, Products, Coupons, Customers]:
            with self.subTest(stream=stream_cls.name):
                instance = self._make_stream_instance(stream_cls, forbidden=True)
                self.assertFalse(instance.check_access())

    # 14. Re-raises non-forbidden exceptions (ConnectionError)
    def test_reraises_non_forbidden_exception(self):
        instance = self._make_stream_instance(
            Orders, error=ConnectionError("network failure")
        )
        with self.assertRaises(ConnectionError):
            instance.check_access()

    # 14b. Re-raises non-403 HTTPError (e.g. 500) — must not be swallowed
    def test_reraises_non_403_http_error(self):
        """A 500 HTTPError from the API must propagate; only 403 is caught."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_err = HTTPError(response=mock_response)
        instance = self._make_stream_instance(Orders, error=http_err)
        with self.assertRaises(HTTPError):
            instance.check_access()

    # 15. api.get is called with resolve=True
    def test_get_called_with_resolve_true(self):
        instance = self._make_stream_instance(Orders)
        instance.check_access()
        # Verify api.get was called and resolve=True was passed
        instance.client.api.get.assert_called_once()
        _, kwargs_or_args = instance.client.api.get.call_args[0], instance.client.api.get.call_args
        # resolve=True can be positional or keyword
        call_args = instance.client.api.get.call_args
        # positional: get(url, params, resolve)
        pos_args = call_args[0]
        kw_args = call_args[1]
        resolve_value = pos_args[2] if len(pos_args) > 2 else kw_args.get('resolve')
        self.assertTrue(resolve_value)

    # warning logged on forbidden
    def test_warning_logged_on_forbidden(self):
        instance = self._make_stream_instance(Orders, forbidden=True)
        with patch('tap_bigcommerce.streams.logger') as mock_logger:
            instance.check_access()
            mock_logger.warning.assert_called_once()
            msg = ' '.join(str(a) for a in mock_logger.warning.call_args[0])
            self.assertIn('orders', msg)

    # check_access uses correct endpoint URL
    def test_uses_correct_endpoint_for_each_stream(self):
        expected_paths = {
            'orders': 'orders',
            'products': 'catalog/products',
            'customers': 'customers',
            'coupons': 'coupons',
        }
        for stream_cls in [Orders, Products, Coupons, Customers]:
            with self.subTest(stream=stream_cls.name):
                instance = self._make_stream_instance(stream_cls)
                instance.check_access()
                call_args = instance.client.api.get.call_args[0]
                url = call_args[0]
                self.assertIn(expected_paths[stream_cls.name], url)


class TestBigCommerceEndpointsCompleteness(unittest.TestCase):
    """All streams in STREAMS must have a corresponding entry in Bigcommerce.endpoints
    so that check_access() can build a valid probe URL for every stream."""

    def test_all_streams_have_endpoint_config(self):
        """Every stream name in STREAMS must appear in Bigcommerce.endpoints."""
        for stream_name in STREAMS:
            with self.subTest(stream=stream_name):
                self.assertIn(
                    stream_name,
                    Bigcommerce.endpoints,
                    f"Stream '{stream_name}' is missing from Bigcommerce.endpoints — "
                    "check_access() would fall back to an incorrect URL.",
                )

    def test_all_endpoint_configs_have_path(self):
        """Every stream endpoint config must define a 'path' key for URL construction."""
        for stream_name in STREAMS:
            with self.subTest(stream=stream_name):
                config = Bigcommerce.endpoints.get(stream_name, {})
                self.assertIn(
                    'path',
                    config,
                    f"Stream '{stream_name}' endpoint config is missing 'path'.",
                )

    def test_all_endpoint_configs_have_version(self):
        """Every stream endpoint config must define a 'version' key."""
        for stream_name in STREAMS:
            with self.subTest(stream=stream_name):
                config = Bigcommerce.endpoints.get(stream_name, {})
                self.assertIn(
                    'version',
                    config,
                    f"Stream '{stream_name}' endpoint config is missing 'version'.",
                )
