from functools import wraps

import json
import bigcommerce
import singer
from datetime import datetime
from dateutil.parser import parse

# Exception classes
from requests.packages.urllib3.exceptions import ReadTimeoutError
from requests.exceptions import ConnectionError
from bigcommerce.exception import ClientRequestException


logger = singer.get_logger().getChild('tap-bigcommerce')


def validate(method):

    @wraps(method)
    def _validate(*args, **kwargs):
        if 'replication_key' in kwargs and \
                kwargs['replication_key'] != 'date_modified':
                raise Exception("Client Error - invalid replication_key")

        if 'bookmark' in kwargs and \
                type(kwargs['bookmark']) is not datetime:
                raise Exception(
                    "Client Error - bookmark must be valid datetime"
                )

        return method(*args, **kwargs)

    return _validate


def parse_date_string_arguments(fields):

    if type(fields) is not list:
        fields = [fields]

    def decorator(method):
        @wraps(method)
        def parse_dt(*args, **kwargs):
            logger.debug(kwargs)
            for key, value in kwargs.items():
                if key in fields:
                    if type(value) != str:
                        raise Exception("parse_date_string_arguments expects string value.")
                    kwargs[key] = parse(value)
            return method(*args, **kwargs)
        return parse_dt

    return decorator


class Client():

    authorized = False

    def is_authorized(self):
        return self.authorized is True


class BigCommerce(Client):

    api_version = 2

    sort_order = 'asc'

    def __init__(self, client_id, access_token, store_hash):
        self.client_id = client_id
        self.access_token = access_token
        self.store_hash = store_hash

        self._reset_session()

    def _reset_session(self):
        try:
            self.api = bigcommerce.api.BigcommerceApi(
                client_id=self.client_id,
                store_hash=self.store_hash,
                access_token=self.access_token,
                rate_limiting_management={
                    'min_requests_remaining': 2,
                    'wait': True,
                    'callback_function': None
                }
            )
            self.authorized = True
        except Exception as e:
            self.authorized = False
            raise e

    def prepare(self, obj, default=None):
        if obj is None:
            return default
        if type(obj) == list:
            return [self.prepare(o, default) for o in obj]
        else:
            return {
                k: v for k, v in obj.items() if k != '_connection'
            }

    # @singer.utils.backoff(
    #     (ClientRequestException,
    #      ConnectionError,
    #      ClientRequestException,
    #      ReadTimeoutError)
    # )
    @parse_date_string_arguments('bookmark')
    @validate
    def orders(self, replication_key, bookmark):

        order_invalid_fields = ['credit_card_type']
        products_invalid_fields = ['configurable_fields', 'fulfillment_source']

        for order in self.api.Orders.iterall(
            min_date_modified=bookmark.isoformat(),
            sort='date_modified:'+self.sort_order
        ):
            obj = {k: v for k, v in self.prepare(order).items() if k not in order_invalid_fields}
            obj['products'] = [{k: v for k, v in p.items() if k not in products_invalid_fields} for p in self.prepare(order.products(), [])]
            obj['coupons'] = self.prepare(order.coupons(), None)
            if obj['coupons'] == {}:
                obj['coupons'] = None
            yield obj
            #logger.debug(json.dumps(obj, indent=2))

    @parse_date_string_arguments('bookmark')
    @validate
    def products(self, replication_key, bookmark):

        for product in self.api. 
        Products.iterall(
            date_modified=bookmark.isoformat(),
            sort='date_modified',
            direction=self.sort_order
        ):
            yield self.prepare(product)

    @parse_date_string_arguments('bookmark')
    @validate
    def customers(self, replication_key, bookmark):

        for customer in self.api.Customers.iterall(
            min_date_modified=bookmark.isoformat(),
            sort='date_modified',
            direction=self.sort_order
        ):
            yield self.prepare(customer)
