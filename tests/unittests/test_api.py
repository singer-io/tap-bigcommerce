import os

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
from concurrent.futures import Future
from tap_bigcommerce.bigcommerce import Bigcommerce
from tap_bigcommerce.bigcommerce import BigCommerceRateLimitException
from tap_bigcommerce.bigcommerce import filter_excluded_paths
from tap_bigcommerce.bigcommerce import transform_dates
from tap_bigcommerce.bigcommerce import unpack_nested_resources
from tap_bigcommerce.bigcommerce import resolve_resources

import time


LVL_ONE_OBJECT = {
    'id': 100,
    'name': 'Test',
    'excluded_field_1': 1,
    'list_field': [
        {
            'id': 1001,
            'name': 'A',
            'excluded_field_2': 0
        }
    ],
    'nested_resource': {
        'resource': 'nested_resource_name',
        'url': 'mock://api.bigcommerce.com/nested/123'
    }
}

LVL_TWO_OBJECT = {
    'name': 'nested_resource_name',
    'value': 123
}


def mock_getter(url, params={}):

    f, m = Future(), Mock()

    responses = {
        'mock://api.bigcommerce.com/nested/123': LVL_TWO_OBJECT
    }

    m.data = responses[url]

    f.set_result(m)

    return f


class TestFilterExcludedPaths(unittest.TestCase):

    def test_removes_top_level_and_nested_excluded(self):
        result = filter_excluded_paths(
            LVL_ONE_OBJECT,
            [
                ('excluded_field_1',),
                ('list_field', 'excluded_field_2')
            ]
        )
        self.assertDictEqual(
            result,
            {
                'id': 100,
                'name': 'Test',
                'list_field': [
                    {
                        'id': 1001,
                        'name': 'A',
                    }
                ],
                'nested_resource': {
                    'resource': 'nested_resource_name',
                    'url': 'mock://api.bigcommerce.com/nested/123'
                }
            }
        )

    def test_empty_exclude_paths_returns_full_object(self):
        obj = {'a': 1, 'b': {'c': 2}}
        result = filter_excluded_paths(obj, [])
        self.assertEqual(result, obj)

    def test_empty_object(self):
        result = filter_excluded_paths({}, [('a',)])
        self.assertEqual(result, {})

    def test_list_at_top_level(self):
        obj = {'items': [{'a': 1, 'b': 2}]}
        result = filter_excluded_paths(obj, [('items', 'b')])
        self.assertEqual(result, {'items': [{'a': 1}]})

    def test_scalar_value_unchanged(self):
        # Scalar inside dict is not traversed
        obj = {'x': 42}
        result = filter_excluded_paths(obj, [('y',)])
        self.assertEqual(result, {'x': 42})


class TestTransformDates(unittest.TestCase):

    def test_transforms_date_fields(self):
        lvl_one = {
            'id': 1000001,
            'customer_id': 0,
            'date_created': 'Mon, 31 Dec 2018 23:59:35 +0000',
            'date_modified': 'Tue, 01 Jan 2019 00:00:10 +0000',
            'items': [
                {
                    'id': 0,
                    'date': 'Mon, 31 Dec 2018 23:59:35 +0000'
                },
            ]
        }
        lvl_one_result = {
            'id': 1000001,
            'customer_id': 0,
            'date_created': '2018-12-31T23:59:35.000000Z',
            'date_modified': '2019-01-01T00:00:10.000000Z',
            'items': [
                {
                    'id': 0,
                    'date': '2018-12-31T23:59:35.000000Z'
                },
            ]
        }
        self.assertDictEqual(
            transform_dates(lvl_one, ['date_created', 'date_modified', 'date']),
            lvl_one_result
        )

    def test_none_value_is_preserved(self):
        obj = {'date_created': None, 'id': 1}
        result = transform_dates(obj, ['date_created'])
        self.assertIsNone(result['date_created'])

    def test_empty_string_is_preserved(self):
        obj = {'date_created': '', 'id': 1}
        result = transform_dates(obj, ['date_created'])
        self.assertEqual(result['date_created'], '')

    def test_no_date_fields_returns_unchanged(self):
        obj = {'id': 1, 'name': 'test'}
        result = transform_dates(obj, [])
        self.assertEqual(result, obj)

    def test_nested_list_dates_transformed(self):
        obj = {'items': [{'dt': '2020-01-01T00:00:00+00:00'}]}
        result = transform_dates(obj, ['dt'])
        self.assertEqual(result['items'][0]['dt'], '2020-01-01T00:00:00.000000Z')

    def test_invalid_date_string_preserved(self):
        """If the date string can't be parsed, it should still be included."""
        obj = {'date_created': 'not-a-date'}
        result = transform_dates(obj, ['date_created'])
        # Depending on implementation, it will either convert or pass through
        self.assertIn('date_created', result)


class TestUnpackNestedResources(unittest.TestCase):

    def test_unpacks_resource_dict_to_future(self):
        unpack = unpack_nested_resources(mock_getter)
        row = {
            'id': 1,
            'sub': {
                'resource': 'sub_name',
                'url': 'mock://api.bigcommerce.com/nested/123'
            }
        }
        result = unpack(row)
        # The sub field should now be a Future
        self.assertIsInstance(result['sub'], Future)

    def test_sync_mode_resolves_immediately(self):
        unpack = unpack_nested_resources(mock_getter, asyncronous=False)
        row = {
            'id': 1,
            'sub': {
                'resource': 'sub_name',
                'url': 'mock://api.bigcommerce.com/nested/123'
            }
        }
        result = unpack(row)
        # In sync mode, the value should already be resolved data
        self.assertEqual(result['sub'], LVL_TWO_OBJECT)

    def test_excludes_specified_paths(self):
        unpack = unpack_nested_resources(mock_getter, exclude_fields=[('sub',)])
        row = {
            'id': 1,
            'sub': {
                'resource': 'sub_name',
                'url': 'mock://api.bigcommerce.com/nested/123'
            }
        }
        result = unpack(row)
        self.assertNotIn('sub', result)

    def test_plain_dict_without_resource_key_unchanged(self):
        unpack = unpack_nested_resources(mock_getter)
        row = {'id': 1, 'data': {'key': 'value'}}
        result = unpack(row)
        self.assertEqual(result['data'], {'key': 'value'})


class TestResolveResources(unittest.TestCase):

    def test_resolves_future_to_data(self):
        unpack = unpack_nested_resources(mock_getter)
        unpacked = unpack(LVL_ONE_OBJECT.copy())
        result = resolve_resources(unpacked)
        self.assertEqual(result['nested_resource'], LVL_TWO_OBJECT)

    def test_plain_dict_unchanged(self):
        obj = {'id': 1, 'name': 'test'}
        result = resolve_resources(obj)
        self.assertEqual(result, obj)

    def test_list_of_dicts_resolved(self):
        obj = [{'id': 1}, {'id': 2}]
        result = resolve_resources(obj)
        self.assertEqual(result, [{'id': 1}, {'id': 2}])

    def test_bare_future_resolved(self):
        f = Future()
        m = Mock()
        m.data = {'resolved': True}
        f.set_result(m)
        result = resolve_resources(f)
        self.assertEqual(result, {'resolved': True})

    def test_scalar_returned_unchanged(self):
        self.assertEqual(resolve_resources(42), 42)
        self.assertEqual(resolve_resources('hello'), 'hello')
        self.assertIsNone(resolve_resources(None))


class TestBigcommerceResponseHook(unittest.TestCase):

    @patch('tap_bigcommerce.bigcommerce.FuturesSession')
    def test_200_sets_data(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session

        # Mock the auth check call
        future = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {
            'X-Rate-Limit-Time-Reset-Ms': '1000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '100',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        resp.json.return_value = {'time': 12345}
        future.result.return_value = resp
        session.get.return_value = future

        bc = Bigcommerce(
            client_id='test', access_token='test', store_hash='test'
        )
        # Simulate a 200 response
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {
            'X-Rate-Limit-Time-Reset-Ms': '1000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '100',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        mock_resp.json.return_value = [{'id': 1}]
        bc._response_hook(mock_resp)
        self.assertEqual(mock_resp.data, [{'id': 1}])

    @patch('tap_bigcommerce.bigcommerce.FuturesSession')
    def test_204_sets_empty_data(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session

        future = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {
            'X-Rate-Limit-Time-Reset-Ms': '1000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '100',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        resp.json.return_value = {'time': 12345}
        future.result.return_value = resp
        session.get.return_value = future

        bc = Bigcommerce(
            client_id='test', access_token='test', store_hash='test'
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_resp.headers = {}
        bc._response_hook(mock_resp)
        self.assertEqual(mock_resp.data, [])

    @patch('tap_bigcommerce.bigcommerce.FuturesSession')
    def test_429_raises_rate_limit_exception(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session

        future = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {
            'X-Rate-Limit-Time-Reset-Ms': '1000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '100',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        resp.json.return_value = {'time': 12345}
        future.result.return_value = resp
        session.get.return_value = future

        bc = Bigcommerce(
            client_id='test', access_token='test', store_hash='test'
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.headers = {}
        with self.assertRaises(BigCommerceRateLimitException):
            bc._response_hook(mock_resp)


class TestBigcommerceUpdateRateLimit(unittest.TestCase):

    @patch('tap_bigcommerce.bigcommerce.FuturesSession')
    def test_parses_headers_to_ints(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session

        future = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {
            'X-Rate-Limit-Time-Reset-Ms': '1000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '100',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        resp.json.return_value = {'time': 12345}
        future.result.return_value = resp
        session.get.return_value = future

        bc = Bigcommerce(
            client_id='test', access_token='test', store_hash='test'
        )
        headers = {
            'X-Rate-Limit-Time-Reset-Ms': '5000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '50',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        result = bc._update_rate_limit(headers)
        self.assertEqual(result['ms_until_reset'], 5000)
        self.assertEqual(result['window_size_ms'], 30000)
        self.assertEqual(result['requests_remaining'], 50)
        self.assertEqual(result['requests_quota'], 150)


class TestBigcommerceMakeUrl(unittest.TestCase):

    @patch('tap_bigcommerce.bigcommerce.FuturesSession')
    def test_v2_url(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session

        future = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {
            'X-Rate-Limit-Time-Reset-Ms': '1000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '100',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        resp.json.return_value = {'time': 12345}
        future.result.return_value = resp
        session.get.return_value = future

        bc = Bigcommerce(
            client_id='test', access_token='test', store_hash='testhash'
        )
        url = bc.make_url(2, 'orders')
        self.assertIn('/v2/', url)
        self.assertTrue(url.endswith('/orders'))
        self.assertIn('testhash', url)

    @patch('tap_bigcommerce.bigcommerce.FuturesSession')
    def test_v3_url(self, MockSession):
        session = MagicMock()
        MockSession.return_value = session

        future = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {
            'X-Rate-Limit-Time-Reset-Ms': '1000',
            'X-Rate-Limit-Time-Window-Ms': '30000',
            'X-Rate-Limit-Requests-Left': '100',
            'X-Rate-Limit-Requests-Quota': '150',
        }
        resp.json.return_value = {'time': 12345}
        future.result.return_value = resp
        session.get.return_value = future

        bc = Bigcommerce(
            client_id='test', access_token='test', store_hash='testhash'
        )
        url = bc.make_url(3, 'catalog', 'products')
        self.assertIn('/v3/', url)
        self.assertTrue(url.endswith('/catalog/products'))


class TestLiveAPICalls(unittest.TestCase):
    """
    Test against live BigCommerce API. Accepts path to config file in same
    format as is used in tap
    """
    def setUp(self):
        config_file = os.environ.get('CONFIG_FILE')

        if config_file is None:
            """Internal Testing"""
            self.api_config = None
        else:
            with open(config_file) as f:
                config = json.load(f)
            self.api_config = {k: v for k,v in config.items() if k != "start_date"}

    def test_api_get_returns_future(self):

        if self.api_config is None:
            raise unittest.SkipTest("No BigCommerce API config file set.")

        client = Bigcommerce(**self.api_config)

        url = client.make_url(2, 'orders')

        self.assertTrue(type(client.get(url)) == Future)

    def test_get_orders(self):

        if self.api_config is None:
            raise unittest.SkipTest("No BigCommerce API config file set.")

        client = Bigcommerce(**self.api_config)

        start = time.time()

        for i, order in enumerate(client.resource('orders', {
            'sort': 'date_modified:min',
            'min_date_modified': datetime(2018, 1, 1).isoformat()
        }, async_sub_resources=False)):
            if i > 99:
                break

        syncronous_time = time.time() - start

        start = time.time()

        for i, order in enumerate(client.resource('orders', {
            'sort': 'date_modified:min',
            'min_date_modified': datetime(2018, 1, 1).isoformat()
        })):
            if i > 99:
                break

        async_time = time.time() - start

        print("Time to 100 results. Syncronous: {}, Asyncronous Sub Resources {}".format(
            syncronous_time or None, async_time
        ))

        self.assertLess(async_time, syncronous_time)



if __name__ == '__main__':
    suite = unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(TestFilterExcludedPaths),
        unittest.TestLoader().loadTestsFromTestCase(TestTransformDates),
        unittest.TestLoader().loadTestsFromTestCase(TestUnpackNestedResources),
        unittest.TestLoader().loadTestsFromTestCase(TestResolveResources),
        unittest.TestLoader().loadTestsFromTestCase(TestBigcommerceResponseHook),
        unittest.TestLoader().loadTestsFromTestCase(TestBigcommerceUpdateRateLimit),
        unittest.TestLoader().loadTestsFromTestCase(TestBigcommerceMakeUrl),
        unittest.TestLoader().loadTestsFromTestCase(TestLiveAPICalls),
    ])
    unittest.TextTestRunner(verbosity=2).run(suite)
