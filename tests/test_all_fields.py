"""Tap-tester integration test: all fields."""
import unittest

from base import BigCommerceBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class BigCommerceAllFieldsTest(AllFieldsTest, BigCommerceBaseTest):
    """Test that all fields for each stream are replicated."""

    @staticmethod
    def name():
        return "tap_tester_bigcommerce_all_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
