import unittest
import os
from datetime import datetime

from tap_bigcommerce import utilities


class TestToUtc(unittest.TestCase):

    def test_adds_utc_timezone(self):
        dt = datetime(2024, 1, 15, 12, 0, 0)
        result = utilities.to_utc(dt)
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(str(result.tzinfo), 'UTC')

    def test_replaces_existing_timezone(self):
        import pytz
        eastern = pytz.timezone('US/Eastern')
        dt = eastern.localize(datetime(2024, 1, 15, 12, 0, 0))
        result = utilities.to_utc(dt)
        self.assertEqual(str(result.tzinfo), 'UTC')

    def test_preserves_datetime_fields(self):
        dt = datetime(2024, 6, 15, 8, 30, 45)
        result = utilities.to_utc(dt)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.hour, 8)


class TestGetAbsPath(unittest.TestCase):

    def test_returns_absolute_path(self):
        result = utilities.get_abs_path('schemas')
        self.assertTrue(os.path.isabs(result))

    def test_path_ends_with_argument(self):
        result = utilities.get_abs_path('schemas')
        self.assertTrue(result.endswith('schemas'))

    def test_custom_file_parameter(self):
        result = utilities.get_abs_path('test', file='/fake/dir/module.py')
        self.assertEqual(result, '/fake/dir/test')


class TestSchemaLoader(unittest.TestCase):

    def setUp(self):
        self.loader = utilities.SchemaLoader()

    def test_load_orders_returns_dict(self):
        schema = self.loader.load('orders')
        self.assertIsInstance(schema, dict)

    def test_load_orders_has_properties(self):
        schema = self.loader.load('orders')
        self.assertIn('properties', schema)

    def test_load_all_four_schemas(self):
        for name in ('orders', 'products', 'customers', 'coupons'):
            schema = self.loader.load(name)
            self.assertIn('properties', schema, f"Schema for {name} missing properties")

    def test_shared_refs_are_resolved(self):
        """$ref entries should be resolved - no $ref keys remain in top-level properties."""
        schema = self.loader.load('orders')
        for prop, defn in schema['properties'].items():
            self.assertNotIn('$ref', defn, f"Unresolved $ref in {prop}")

    def test_invalid_schema_raises(self):
        with self.assertRaises(FileNotFoundError):
            self.loader.load('nonexistent')

    def test_schema_contains_id_property(self):
        for name in ('orders', 'products', 'customers', 'coupons'):
            schema = self.loader.load(name)
            self.assertIn('id', schema['properties'], f"{name} missing 'id' property")


if __name__ == '__main__':
    unittest.main()