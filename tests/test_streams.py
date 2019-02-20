import unittest

from tap_bigcommerce.streams import Stream, STREAMS
from tap_bigcommerce.client import Client

from datetime import datetime, timedelta

BIGCOMMERCE_OLD_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S %z"
BIGCOMMERCE_NEW_DATE_FORMAT = "%Y-%m-%d %H:%M:%S %z"


class MockClient(Client):
    pass

class TestStreams(unittest.TestCase):

    def test_is_bookmark_old(self):
        """
        Should return true when value is greater than bookmark
        Dates already converted from different formats returned
        from the BigCommerce API
        """

        client = MockClient

        stream = Stream(client)

        today = datetime.now()

        old_date = today - timedelta(1)

        self.assertTrue(
            Stream.is_bookmark_old(
                self=stream,
                value=today.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
                bookmark=old_date.strftime(BIGCOMMERCE_NEW_DATE_FORMAT)
            )
        )

        self.assertTrue(
            Stream.is_bookmark_old(
                self=stream,
                value=today.strftime(BIGCOMMERCE_OLD_DATE_FORMAT),
                bookmark=old_date.strftime(BIGCOMMERCE_OLD_DATE_FORMAT)
            )
        )

        self.assertTrue(
            Stream.is_bookmark_old(
                self=stream,
                value=today.strftime(BIGCOMMERCE_NEW_DATE_FORMAT),
                bookmark=old_date.strftime(BIGCOMMERCE_OLD_DATE_FORMAT)
            )
        )

        self.assertTrue(
            Stream.is_bookmark_old(
                self=stream,
                value=today.strftime(BIGCOMMERCE_OLD_DATE_FORMAT),
                bookmark=old_date.strftime(BIGCOMMERCE_NEW_DATE_FORMAT)
            )
        )

    def test_load_metadata(self):

        client = MockClient

        orders = STREAMS['orders'](client)
        # print(orders.load_schema())
        # print(orders.load_metadata())





if __name__ == '__main__':
    unittest.main()