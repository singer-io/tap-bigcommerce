"""
Base test class for tap-tester integration tests for tap-bigcommerce.

These tests use the tap-tester framework to run the tap against the
real BigCommerce API.  They require valid API credentials set in the
environment (via tap-tester sandbox).
"""
import os

from tap_tester.base_suite_tests.base_case import BaseCase


class BigCommerceBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams.  Shared tap-specific methods used
    by tap-tester tests.
    """

    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        return "tap-bigcommerce"

    @staticmethod
    def get_type():
        return "platform.bigcommerce"

    @classmethod
    def expected_metadata(cls):
        return {
            "orders": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.RESPECTS_START_DATE: True,
                cls.API_LIMIT: 50,
            },
            "products": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.RESPECTS_START_DATE: True,
                cls.API_LIMIT: 50,
            },
            "customers": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"date_modified"},
                cls.RESPECTS_START_DATE: True,
                cls.API_LIMIT: 50,
            },
            "coupons": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.RESPECTS_START_DATE: False,
                cls.API_LIMIT: 50,
            },
        }

    @staticmethod
    def get_credentials():
        return {
            'client_id': os.getenv('TAP_BIGCOMMERCE_CLIENT_ID'),
            'access_token': os.getenv('TAP_BIGCOMMERCE_ACCESS_TOKEN'),
        }

    def get_properties(self, original: bool = True):
        return {
            'client_id': os.getenv('TAP_BIGCOMMERCE_CLIENT_ID'),
            'access_token': os.getenv('TAP_BIGCOMMERCE_ACCESS_TOKEN'),
            'store_hash': os.getenv('TAP_BIGCOMMERCE_STORE_HASH'),
            'start_date': self.start_date,
        }
