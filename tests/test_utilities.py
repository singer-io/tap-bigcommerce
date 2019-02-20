import unittest
from pprint import pprint

from tap_bigcommerce import utilities

class TestUtilities(unittest.TestCase):

    def test_abs_loader(self):

        print(utilities.get_abs_path('test'))


    def test_schema_loader(self):

        loader = utilities.SchemaLoader()

        schema = loader.load('orders')

        pprint(schema)


if __name__ == '__main__':
    unittest.main()