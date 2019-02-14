import os

import unittest
import json
from datetime import datetime
from tap_bigcommerce.bigcommerce import Bigcommerce
from concurrent.futures import Future
from pprint import pprint

class BigCommerceAPITests(unittest.TestCase):


    def setUp(self, *args, **kwargs):
        
        config_file = os.environ.get('CONFIG_FILE')

        with open(config_file) as f:
            config = json.load(f)

        self.api_config = {k: v for k,v in config.items() if k != "start_date"}


    def test_client_get(self):

        client = Bigcommerce(**self.api_config)

        url = client.make_url(2)('orders')

        self.assertTrue(type(client.get(url)) == Future)

    def test_get_orders(self):

        client = Bigcommerce(**self.api_config)

        for i, order in enumerate(client.resource('orders', {
            'sort': 'date_modified:min',
            'min_date_modified': datetime(2018,1,1).isoformat()
        })):
            print(order)

        print("\n")
        print(client.rate_limit)


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

def run():

    unittest.main()


if __name__ == '__main__':

    run()
