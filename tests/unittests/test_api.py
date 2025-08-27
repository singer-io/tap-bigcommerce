import os

import unittest
from unittest.mock import Mock, patch
import json
from datetime import datetime
from concurrent.futures import Future
#from tap_bigcommerce.bigcommerce import BigcommerceResource
from tap_bigcommerce.bigcommerce import Bigcommerce
from tap_bigcommerce.bigcommerce import filter_excluded_paths
from tap_bigcommerce.bigcommerce import transform_dates
from tap_bigcommerce.bigcommerce import unpack_nested_resources
from tap_bigcommerce.bigcommerce import resolve_resources

from concurrent.futures import Future
from pprint import pprint
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


class TestResourceResolution(unittest.TestCase):

    def test_filter_excluded_paths(self):

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


    def test_resource_resolution(self):

        unpack_resources = unpack_nested_resources(mock_getter)

        result = resolve_resources(unpack_resources(LVL_ONE_OBJECT))

        test_result = LVL_ONE_OBJECT
        test_result['nested_resource'] = LVL_TWO_OBJECT

        self.assertDictEqual(
            result,
            test_result
        )

    def test_transform_dates(self):

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
            raise unittest.SkipTest("No BigCommmerce API config file set.")

        client = Bigcommerce(**self.api_config)

        url = client.make_url(2, 'orders')

        self.assertTrue(type(client.get(url)) == Future)

    def test_get_orders(self):

        if self.api_config is None:
            raise unittest.SkipTest("No BigCommmerce API config file set.")

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


    # def test_get_orders(self):

    #     client = Bigcommerce(**self.api_config)

    #     client.resource('products', {'sort': 'date_modified:asc'})

    #     for i, order in enumerate(client.resource('orders', {
    #         'sort': 'date_modified:min',
    #         'min_date_modified': datetime(2018,1,1).isoformat()
    #     })):
    #         print(order)

    #     print("\n")
    #     print(client.rate_limit)



if __name__ == '__main__':
    suite = unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(TestResourceResolution),
        unittest.TestLoader().loadTestsFromTestCase(TestLiveAPICalls)
    ]) 
    unittest.TextTestRunner(verbosity=2).run(suite)
