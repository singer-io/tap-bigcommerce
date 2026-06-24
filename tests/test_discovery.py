"""Tap-tester integration test: discovery."""
from base import BigCommerceBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class BigCommerceDiscoveryTest(DiscoveryTest, BigCommerceBaseTest):
    """Test tap discovery mode conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_bigcommerce_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
