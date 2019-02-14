"""
API wrapper for BigCommerce library.

Existing Python library for BigCommerce only supports version 2 of
the API. Some endpoints and filters are only available in version 3
of the API.

BigCommerce class gives a basic wrapper around the 4 required resources:
orders, products, customers and coupons.

Rate-Limiting

BigCommerce provides rate limiting information in their responses. This
wrapper limits requests by pausing after a request is made for a period
determinded by taking the number of milliseconds left in the window and
dividing by the number of requests left in the window. The idea being
to maintain the maximum constant rate of requests rather than using all
requests up and then waiting until the window resets.

This may need to change however once we introduce asyncronous nested
resource fetching.

@TODO Asyncronous Nested Resource Requests

Standard resources request 50 results per request. However, each result
contains a number of nested resources (Orders, for example, has OrderProducts,
OrderCoupons and ShippingAddress resources).

This creates a problem of extremely slow throughput, because each primary
resource result page will generate a mininum of 150 requests - all executed
syncronously, delaying iteration through the primary resource.


------- Solution Design -------


Requirements:

Respect API rate limit.


Concept A:

Primary resources run in the primary thread, but for each page of results,
request the nested resources asyncrounously, but wait for those nested results
to complete before paginating on the primary resource.

* Can a syncrounous method be called f


Concept B:

BigCommerce's API doesn't have a limit on the number of concurrent requests,
instead it uses a window-based rate limit.

If the number of nested requests can be predicted, could the primary thread/
resource wait until there are sufficient requests available for all nested
requests to be executed in one go. Then requests-futures can be used to get
all results. 

Recurively run through each property to get the nested resources.


"""

import requests.exceptions 
from concurrent.futures import Future
from requests_futures.sessions import FuturesSession
import urllib
from singer.utils import strptime_to_utc, strftime
from singer import get_logger
import time
import math

from functools import partial

logger = get_logger().getChild('tap-bigcommerce')


class Bigcommerce():

    auth_check_url = "https://api.bigcommerce.com/store"

    base_url = "https://api.bigcommerce.com/stores/"

    results_per_page = 50

    max_retries = 5

    retry_window = 300

    """
    Mapping to standardize between the two BigCommerce API
    versions.
    """
    endpoints = {
        'orders': {
            'version': 2,
            'path': 'orders',
            'transform_date_fields': [
                'date_modified',
                'date_created',
                'date_shipped'
            ],
            'sub_resources': 3,
            # deprecated or non-functioning fields
            'exclude_paths': [
                ('credit_card_type',),
                ('shipping_addresses', 'shipping_quotes'),
                ('products', 'configurable_fields'),
                ('products', 'fulfillment_source')
            ]
        },
        'customers': {
            'version': 2,
            'path': 'customers',
            'transform_date_fields': [
                'date_modified',
                'date_created'
            ],
            'sub_resources': 1,
            'exclude_paths': [
                ('addresses',)
            ]
        },
        'products': {
            'version': 3,
            'path': 'catalog/products',
        },
        'coupons': {
            'version': 2,
            'path': 'coupons',
            'transform_date_fields': [
                'date_created',
                'expires'
            ]
        }
    }

    rate_limit = {
        "ms_until_reset": None,
        "window_size_ms": None,
        "requests_remaining": None,
        "requests_quota": None
    }

    def __init__(self, client_id, access_token, store_hash):

        self.retries = 0
        self.last_retry = None
        self.client_id = client_id
        self.access_token = access_token
        self.store_hash = store_hash

        self.session = FuturesSession()

        self.session.hooks['response'] = self._response_hook

        self.headers = {
            'accept': "application/json",
            'content-type': "application/json",
            'x-auth-client': self.client_id,
            'x-auth-token': self.access_token
        }

        self.base_url = self.base_url + self.store_hash + '/v{version}'

        # auth check and get rate limit window
        self.get(self.make_url(2)('time'), resolve=True)

    def _response_hook(self, resp, *args, **kwargs):
        if 'X-Rate-Limit-Time-Reset-Ms' in resp.headers:
            self.update_rate_limit(resp.headers)

        if resp.status_code != 200:
            if resp.status_code == 204:
                resp.data = None
            else:
                raise requests.exceptions.HTTPError(resp)
        else:
            resp.data = resp.json()

    def update_rate_limit(self, headers):
        print(headers)
        ref = {
            'ms_until_reset': 'X-Rate-Limit-Time-Reset-Ms',
            'window_size_ms': 'X-Rate-Limit-Time-Window-Ms',
            'requests_remaining': 'X-Rate-Limit-Requests-Left',
            'requests_quota': 'X-Rate-Limit-Requests-Quota'
        }

        for key, header in ref.items():
            _temp = int(headers[header])
            if self.rate_limit[key] is None:
                self.rate_limit[key] = _temp
            elif self.rate_limit[key] > _temp:
                self.rate_limit[key] = _temp

    def make_url(self, version=2):

        def build(*res):
            url = self.base_url.format(version=version)
            for r in res:
                url = '{}/{}'.format(url, r)
            return url

        return build

    def get(self, url, params={}, resolve=False):
        future = self.session.get(url, params=params, headers=self.headers)

        if resolve:
            return future.result()
        else:
            return future



    def resource(self, name, params={}):
        resource = self.endpoints.get(name, {})
        version = resource.get('version', 3)
        path = resource.get('path', name)
        transform_date_fields = resource.get('transform_date_fields', [])
        exclude_paths = resource.get('exclude_paths', [])
        url = self.make_url(version)(path)

        # recursively unpack nested resources
        # watch for recursion depth - but most APIs only go 2/3 levels deep
        def unpack_resources(row, parent_key=()):
            if type(row) == dict:
                obj = {}
                for key, value in row.items():
                    path = parent_key + (key,)
                    if path in exclude_paths:
                        continue
                    # exclude_paths
                    if type(value) == dict and 'resource' in value:
                        value = self.get(value['url'], {})
                    if key in transform_date_fields:
                        try:
                            value = strftime(strptime_to_utc(value))
                        except Exception as e:
                            pass
                    obj[key] = unpack_resources(value, path)
                return obj
            elif type(row) == list:
                return [unpack_resources(el, parent_key) for el in row]
            else:
                return row

        def resolve_resources(row, parent_key=()):
            if type(row) == Future:
                r = row.result()
                return r.data
            if type(row) == dict:
                obj = {}
                for key, value in row.items():
                    path = parent_key + (key,)
                    obj[key] = resolve_resources(value, path)
                return obj
            elif type(row) == list:
                return [resolve_resources(el, parent_key) for el in row]
            else:
                return row

        sub_resources = resource.get('sub_resources', 0)

        if sub_resources > 0:
            self.results_per_page = min(
                self.results_per_page,
                math.floor(self.rate_limit['requests_quota'] / sub_resources) - 5
            )

        requests_need = self.results_per_page * sub_resources

        print("results per page: {}".format(self.results_per_page))

        page = 0
        while True:
            page += 1
            print("page {}".format(page))
            r = self.get(url, {
                **params,
                **{
                    'page': page,
                    'limit': self.results_per_page
                }
            }).result()

            if self.rate_limit['requests_remaining'] is not None:
                print("{} requests remaining".format(self.rate_limit['requests_remaining'] - requests_need))
                if (self.rate_limit['requests_remaining'] - requests_need) < 1:
                    sec = self.rate_limit['ms_until_reset'] / 1000
                    print("Rate limit exhausted. Waiting {:.2f} seconds".format(sec))
                    time.sleep(sec)

            resp = r.data

            data = resp if version == 2 else resp.get('data', [])

            for row in data \
                    if version == 2 else resp.get('data', []):
                yield resolve_resources(unpack_resources(row))
            if len(data) < self.results_per_page:
                break

            #break
    # def resource(self, name, params={}):
    #     resource = self.endpoints.get(name, {})
    #     version = resource.get('version', 3)
    #     path = resource.get('path', name)
    #     transform_date_fields = resource.get('transform_date_fields', [])
    #     exclude_paths = resource.get('exclude_paths', [])
    #     url = self.make_url(version)(path)

    #     # recursively unpack nested resources
    #     # watch for recursion depth - but most APIs only go 2/3 levels deep
    #     def unpack_resources(row, parent_key=()):
    #         if type(row) == dict:
    #             obj = {}
    #             for key, value in row.items():
    #                 path = parent_key + (key,)
    #                 if path in exclude_paths:
    #                     continue
    #                 # exclude_paths
    #                 if type(value) == dict and 'resource' in value:
    #                     value = self.get(value['url'], {})
    #                 if key in transform_date_fields:
    #                     try:
    #                         value = strftime(strptime_to_utc(value))
    #                     except Exception as e:
    #                         pass
    #                 obj[key] = unpack_resources(value, path)
    #             return obj
    #         elif type(row) == list:
    #             return [unpack_resources(el, parent_key) for el in row]
    #         else:
    #             return row

    #     page = 0
    #     while True:
    #         page += 1
    #         resp = self.get(url, {
    #             **params,
    #             **{'page': page}
    #         })
    #         data = resp if version == 2 else resp.get('data', [])
    #         for row in data \
    #                 if version == 2 else resp.get('data', []):
    #             yield unpack_resources(row)
    #         if len(data) < 50:
    #             break
