"""Tap-tester integration test: pagination."""
from base import BigCommerceBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest


class BigCommercePaginationTest(PaginationTest, BigCommerceBaseTest):
    """Test that the tap handles paginated API responses."""

    @staticmethod
    def name():
        return "tap_tester_bigcommerce_pagination_test"

    def streams_to_test(self):
        return self.expected_stream_names()
