"""Tap-tester integration test: sync canary (duplicate kept for compatibility)."""
import unittest

from base import BigCommerceBaseTest
from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest


class BigCommerceSyncCanaryTest(SyncCanaryTest, BigCommerceBaseTest):
    """Basic smoke test — run the full sync pipeline."""

    @staticmethod
    def name():
        return "tap_tester_bigcommerce_sync_canary_test"

    def streams_to_test(self):
        return self.expected_stream_names()
