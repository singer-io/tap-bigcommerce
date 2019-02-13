import requests
import urllib
from singer.utils import strptime_to_utc, strftime
from singer import get_logger
import time
import math

logger = get_logger().getChild('tap-bigcommerce')


class Bigcommerce():

    base_url = "https://api.bigcommerce.com/stores/"

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

    def __init__(self, client_id, access_token, store_hash):

        self.retries = 0
        self.last_retry = None
        self.client_id = client_id
        self.access_token = access_token
        self.store_hash = store_hash

        self.session = requests.Session()

        self.headers = {
            'accept': "application/json",
            'content-type': "application/json",
            'x-auth-client': self.client_id,
            'x-auth-token': self.access_token
        }

        self.base_url = self.base_url + self.store_hash + '/v{version}'

    def make_url(self, version=2):

        def build(*res):
            # print(self.base_url.format(version=version))
            url = self.base_url.format(version=version)
            for r in res:
                url = '{}/{}'.format(url, r)
            return url

        return build

    def get(self, url, params):

        try:

            r = self.session.get(url, params=params, headers=self.headers)

            if r.status_code != 200:
                if r.status_code == 204:
                    return None
                else:
                    raise requests.exceptions.HTTPError(
                        "HTTP {}: {}, {}".format(r.status_code, url, r.text)
                    )

            if 'X-Rate-Limit-Time-Reset-Ms' in r.headers:
                ms_until_reset = int(r.headers['X-Rate-Limit-Time-Reset-Ms'])
                requests_remaining = int(
                    r.headers['X-Rate-Limit-Requests-Left'])

                # sleep based on requests left within window remaining
                sleep_sec = round(
                    math.ceil(ms_until_reset / requests_remaining) / 1000,
                    14
                )
                time.sleep(sleep_sec)

            return r.json()

        except Exception as e:
            logger.error(e)

            self.retries += 1

            if self.retries > self.max_retries:
                logger.error("Max retries reached. Raising Critical Error")
                raise(e)

            if self.last_retry is None:
                self.last_retry = time.time()
            else:
                if time.time() - self.last_retry > self.retry_window:
                    self.retries = 0

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

        page = 0
        while True:
            page += 1
            resp = self.get(url, {
                **params,
                **{'page': page}
            })
            data = resp if version == 2 else resp.get('data', [])
            for row in data \
                    if version == 2 else resp.get('data', []):
                yield unpack_resources(row)
            if len(data) < 50:
                break
