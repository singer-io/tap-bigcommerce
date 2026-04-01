"""Tap-tester integration test: bookmarks."""
import unittest

from base import BigCommerceBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class BigCommerceBookmarkTest(BookmarkTest, BigCommerceBaseTest):
    """Test that incremental streams update bookmarks correctly."""

    @staticmethod
    def name():
        return "tap_tester_bigcommerce_bookmark_test"

    def streams_to_test(self):
        return self.expected_stream_names()
