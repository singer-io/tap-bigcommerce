"""Tap-tester integration test: automatic fields."""
from base import BigCommerceBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class BigCommerceAutomaticFieldsTest(MinimumSelectionTest, BigCommerceBaseTest):
    """Test that automatic fields (PKs and replication keys)
    are always replicated even with minimum field selection."""

    @staticmethod
    def name():
        return "tap_tester_bigcommerce_automatic_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
