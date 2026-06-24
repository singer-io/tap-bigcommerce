"""Tap-tester integration test: start date."""
from base import BigCommerceBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class BigCommerceStartDateTest(StartDateTest, BigCommerceBaseTest):
    """Test that start_date config controls initial replication."""

    @staticmethod
    def name():
        return "tap_tester_bigcommerce_start_date_test"

    def streams_to_test(self):
        return self.expected_stream_names()
