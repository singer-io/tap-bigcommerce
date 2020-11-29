#!/usr/bin/env python
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

Asyncronous Nested Resource Requests

Standard resources request 50 results per request. However, each result
contains a number of nested resources (Orders, for example, has OrderProducts,
OrderCoupons and ShippingAddress resources).

This creates a problem of extremely slow throughput, because each primary
resource result page will generate a mininum of 150 requests - all executed
syncronously, delaying iteration through the primary resource.

To solve this, sub-resources are requested asyncronously using
requests-futures. A page of results is requested syncronously from the API,
then all of the nested resources for that page of results are requested at
once. Then, in a final loop, the nested resource result is resolved.

In testing, this created a 10-fold increase in speed.

API Rate Limit:

In order to accomodate rate limit restrictions for asyncrounous requests,
at the start of a resource request, the number of requests that will be
needed for the entire page or results is calculated. That number is then
compared to the `X-Rate-Limit-Requests-Left` header. If not enough requests
are available, the script will wait until the next window.

For BigCommerce enterprise plans, the API rate limit is very higher than
the possible throughput of this script. However for low cost plans, the
rate limit is very low (150 requests per 30 seconds), which means that many
resources couldn't be extracted within a single window. To accomodate this,
the initial authorization request checks the rate limit window quota, and
compares this to the number of requests needed. If more requests are required
than are available, the results_per_page number is lowered to a point that will
allow an entire page of results to be processed within one window.

"""

import time
import math

from concurrent.futures import Future
from requests_futures.sessions import FuturesSession
from requests.exceptions import HTTPError
from singer.utils import strptime_to_utc, strftime
from singer import get_logger


logger = get_logger().getChild('tap-bigcommerce')


def filter_excluded_paths(obj, exclude_paths=[]):
    """
    Recurisvely traverse an object and remove fields
    if they match a tuple path (parent, child) provided
    in the list of exclude_paths
    """
    def _filter(o, parent_key=()):
        if type(o) == dict:
            obj = {}
            for key, value in o.items():
                path = parent_key + (key,)
                if path not in exclude_paths:
                    obj[key] = _filter(value, path)
            return obj
        elif type(o) == list:
            return [_filter(el, parent_key) for el in o]
        else:
            return o

    return _filter(obj)


def transform_dates(obj, date_fields=[]):
    """
    Transform dates if the field key is in the provided
    list of fields `date_fields`
    """
    def _transform(o):
        if type(o) == dict:
            obj = {}
            for key, value in o.items():
                if (key in date_fields) and (value not in (None, "")):
                    try:
                        value = strftime(strptime_to_utc(value))
                    except Exception as e:
                        pass
                obj[key] = _transform(value)
            return obj
        elif type(o) == list:
            return [_transform(el) for el in o]
        else:
            return o

    return _transform(obj)


def unpack_nested_resources(get, exclude_fields=[], asyncronous=True):
    """
    Returns a function that will recursively "unpack" an object
    for nested resources by making an asyncrounous request (if
    asyncronous is True). Value of the field will be a Future.
    """

    def unpack(row, parent_key=()):
        if type(row) == dict:
            obj = {}
            for key, value in row.items():
                path = parent_key + (key,)
                if path not in exclude_fields:
                    if type(value) == dict and 'resource' in value:
                        value = get(value['url'], {})
                        if asyncronous is False:
                            value = value.result().data
                    obj[key] = unpack(value, path)
            return obj
        elif type(row) == list:
            return [unpack(el, parent_key) for el in row]
        else:
            return row

    return unpack


def resolve_resources(row, parent_key=()):
    """
    Recurisvely traverse object and Resolve any field values
    that are Futures.
    """
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


class BigCommerceRateLimitException(Exception):
    pass


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
                ('products', 'configurable_fields'),
                ('products', 'fulfillment_source'),
                ('shipping_addresses', 'shipping_quotes')
            ]
        },
        'customers': {
            'version': 2,
            'path': 'customers',
            'transform_date_fields': [
                'date_modified',
                'date_created'
            ],
            'sub_resources': 0,
            'exclude_paths': [
                ('addresses',)
            ]
        },
        'customersv3': {
            'version': 3,
            'path': 'customers'
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
        },
        'attribute-values': {
           'version': 3,
           'path': 'customers/attribute-values'
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

        self.base_url = self.base_url + self.store_hash + '/v{version}'

        self._reset_session()

    def _reset_session(self):
        """
        Sets the self.session object to a new FutureSession
        instance and sets default headers and responce hook.

        Called when class instantiated as well as if there is
        an API error and the session needs to be reset.
        """
        self.request_count = 0
        self.session = FuturesSession()

        self.session.hooks['response'] = self._response_hook

        self.headers = {
            'accept': "application/json",
            'content-type': "application/json",
            'x-auth-client': self.client_id,
            'x-auth-token': self.access_token
        }

        # auth check and get rate limit window
        r = self.get(self.make_url(2, 'time'), resolve=True)
        if r.status_code:
            logger.info("BigCommerce API Authorized.")

    def _response_hook(self, resp, *args, **kwargs):
        self.request_count += 1
        if 'X-Rate-Limit-Time-Reset-Ms' in resp.headers:
            self.rate_limit = self._update_rate_limit(resp.headers)

        if resp.status_code != 200:
            if resp.status_code == 204:
                resp.data = []
            elif resp.status_code == 429:
                raise BigCommerceRateLimitException(resp)
            else:
                raise HTTPError(resp)
        else:
            resp.data = resp.json()

    def _update_rate_limit(self, headers):
        """
        Parse header object and return a clean dictionary
        of integer values.
        """
        ref = {
            'ms_until_reset': 'X-Rate-Limit-Time-Reset-Ms',
            'window_size_ms': 'X-Rate-Limit-Time-Window-Ms',
            'requests_remaining': 'X-Rate-Limit-Requests-Left',
            'requests_quota': 'X-Rate-Limit-Requests-Quota'
        }
        rate_limit = {}
        for key, header in ref.items():
            rate_limit[key] = int(headers[header])
        return rate_limit

    def make_url(self, version=2, *res):
        """
        Make a valid URL based on the API version and paths provided
        """
        url = self.base_url.format(version=version)
        for r in res:
            url = '{}/{}'.format(url, r)
        return url

    def get(self, url, params={}, resolve=False):
        """
        Make a get request.

        Args:
            url (str): full URL to request
            params (dict): dict of key values for request parameters
            resolve (bool): if True, resolve future before returning
                            (making method blocking), otherwise
                            return Future

        Returns:
            requests.Response
            OR
            concurrent.futures.Future
        """
        future = self.session.get(url, params=params, headers=self.headers)

        if resolve:
            return future.result()
        else:
            return future

    def resource(self, name, params={}, async_sub_resources=True):
        resource = self.endpoints.get(name, {})
        version = resource.get('version', 3)
        path = resource.get('path', name)
        date_fields = resource.get('transform_date_fields', [])
        exclude_paths = resource.get('exclude_paths', [])
        url = self.make_url(version, path)

        unpack_resources = unpack_nested_resources(
            self.get,
            exclude_paths,
            async_sub_resources
        )

        sub_resources = resource.get('sub_resources', 0)

        # adjust results per page based on number of sub resources and
        # the request quota set by the initial authorization check request
        if sub_resources > 0:
            self.results_per_page = min(
                self.results_per_page,
                math.floor(
                    self.rate_limit['requests_quota'] / sub_resources
                ) - 5
            )

        requests_need = self.results_per_page * sub_resources

        page = 0
        while True:
            error_count = 0
            page += 1

            params = {**params, **{
                'page': page,
                'limit': self.results_per_page
            }}

            try:
                r = self.get(url, params).result()
            except BigCommerceRateLimitException as e:
                delay = (self.rate_limit['window_size_ms'] / 1000)
                logger.error((
                    "BigCommerce rate limit exceeded. "
                    "Waiting {:.2f}"
                ).format(delay))
                time.sleep(delay + 1)
                # retry the same page
                page -= 1
                continue

            if self.rate_limit['requests_remaining'] is not None:
                if (self.rate_limit['requests_remaining'] - requests_need) < 1:
                    sec = self.rate_limit['ms_until_reset'] / 1000
                    logger.warning((
                        "Not enough requests available to complete request. "
                        "Waiting {:.2f} sec"
                    ).format(sec))
                    self.request_count = 0
                    time.sleep(sec)

            data = r.data if version == 2 else r.data.get('data', [])
            # unpack nested resources for entire page of results
            data = unpack_resources(data)

            try:
                for row in data:
                    yield transform_dates(
                        filter_excluded_paths(
                            resolve_resources(row),
                            exclude_paths),
                        date_fields)
            except BigCommerceRateLimitException as e:
                delay = (self.rate_limit['window_size_ms'] / 1000)
                logger.error((
                    "BigCommerce rate limit exceeded. "
                    "Waiting {:.2f}"
                ).format(delay))
                time.sleep(delay + 1)
                # retry the same page
                page -= 1
                continue
            except Exception as e:
                error_count += 1
                logger.warning(
                    "Error {} occurred. Sleeping for 10 seconds.".format(e)
                )
                time.sleep(10)
                # max 4 errors in any single page of results
                if error_count > 3:
                    logger.error("{} errors, ending".format(error_count))
                    raise e

            # assume results page with fewer values than `results_per_page` =
            # no more results
            if len(data) < self.results_per_page:
                break
