"""Generic mock data generator for Singer tap integration tests.

Reads JSON schema files and generates mock API response data with
deterministic, type-conformant values.  Adapted for BigCommerce's
$ref shared-schema pattern — resolves $ref before generating values.
"""
import copy
import json
import os
from datetime import datetime, timedelta


class MockDataGenerator:
    """Generates mock records from JSON schema files.

    Usage::

        gen = MockDataGenerator('/path/to/tap_bigcommerce/schemas')
        record = gen.generate_record('orders', seed=0)
        records = gen.generate_records('orders', count=3)
    """

    BASE_DATE = datetime(2024, 6, 15, 10, 0, 0)

    def __init__(self, schemas_dir):
        self.schemas_dir = schemas_dir
        self._schema_cache = {}
        self._shared_refs = self._load_shared_refs()

    # -------------------------------------------------------------- #
    #  Shared $ref loading
    # -------------------------------------------------------------- #

    def _load_shared_refs(self):
        """Load all shared schema files from schemas/shared/."""
        refs = {}
        shared_dir = os.path.join(self.schemas_dir, 'shared')
        if os.path.isdir(shared_dir):
            for fname in os.listdir(shared_dir):
                if fname.endswith('.json'):
                    with open(os.path.join(shared_dir, fname)) as f:
                        refs[fname] = json.load(f)
        return refs

    def _resolve_ref(self, field_schema):
        """Resolve a $ref to a shared schema, returning the resolved schema."""
        if '$ref' in field_schema:
            ref_file = field_schema['$ref']
            if ref_file in self._shared_refs:
                return self._shared_refs[ref_file]
        return field_schema

    # -------------------------------------------------------------- #
    #  Schema loading
    # -------------------------------------------------------------- #

    def load_schema(self, stream_name):
        """Load and cache a JSON schema file for *stream_name*."""
        if stream_name not in self._schema_cache:
            path = os.path.join(self.schemas_dir, f'{stream_name}.json')
            with open(path) as f:
                self._schema_cache[stream_name] = json.load(f)
        return self._schema_cache[stream_name]

    # -------------------------------------------------------------- #
    #  Value generation
    # -------------------------------------------------------------- #

    @staticmethod
    def resolve_type(type_spec):
        """Return the first non-null type from a JSON-Schema *type* field."""
        if isinstance(type_spec, list):
            for t in type_spec:
                if t != 'null':
                    return t
            return 'string'
        return type_spec

    def generate_value(self, field_name, field_schema, seed=0):
        """Return a deterministic value that conforms to *field_schema*."""
        # Resolve $ref before processing
        field_schema = self._resolve_ref(field_schema)

        type_str = self.resolve_type(field_schema.get('type', 'string'))
        fmt = field_schema.get('format')

        if fmt == 'date-time':
            dt = self.BASE_DATE + timedelta(days=seed)
            return dt.strftime('%Y-%m-%dT%H:%M:%S.000000Z')

        if type_str == 'string':
            return f"mock-{field_name.lower()}-{seed}"
        if type_str == 'number':
            return round(42.5 + seed * 1.1, 2)
        if type_str == 'integer':
            return 100 + seed
        if type_str == 'boolean':
            return seed % 2 == 0
        if type_str == 'array':
            items_schema = field_schema.get('items', {})
            if items_schema.get('properties'):
                # Array of objects — generate one item
                return [self.generate_value(field_name + '_item',
                                            items_schema, seed)]
            return []
        if type_str == 'object':
            # Generate nested object from properties if available
            props = field_schema.get('properties', {})
            if props:
                obj = {}
                for k, v in props.items():
                    obj[k] = self.generate_value(k, v, seed)
                return obj
            return {}
        return f"mock-{seed}"

    # -------------------------------------------------------------- #
    #  Record generation
    # -------------------------------------------------------------- #

    def generate_record(self, stream_name, seed=0, overrides=None,
                        exclude_fields=None):
        """Generate one mock record for *stream_name* from its schema.

        Parameters
        ----------
        stream_name : str
            Must match a ``<stream_name>.json`` file in *schemas_dir*.
        seed : int
            Deterministic seed so the same call always produces the
            same values.
        overrides : dict, optional
            Field values to force (applied after generation).
        exclude_fields : set, optional
            Fields to omit from the record.
        """
        schema = self.load_schema(stream_name)
        record = {}
        exclude = exclude_fields or set()
        for field_name, field_schema in schema.get('properties', {}).items():
            if field_name in exclude:
                continue
            resolved = self._resolve_ref(field_schema)
            record[field_name] = self.generate_value(
                field_name, resolved, seed)
        if overrides:
            record.update(overrides)
        return record

    def generate_records(self, stream_name, count=1, base_seed=0,
                         overrides=None, exclude_fields=None):
        """Generate *count* mock records with incrementing seeds."""
        return [
            self.generate_record(stream_name, seed=base_seed + i,
                                 overrides=overrides,
                                 exclude_fields=exclude_fields)
            for i in range(count)
        ]
