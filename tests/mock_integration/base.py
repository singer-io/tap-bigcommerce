"""
Base test class for mock integration tests for tap-bigcommerce.

These tests run the real tap code against mocked API responses — no external
tap-tester dependency required.  Mock data is generated dynamically from
the JSON schema files via MockDataGenerator.
"""
import copy
import os
from unittest.mock import MagicMock

from singer import metadata, Catalog

import tap_bigcommerce
from tap_bigcommerce.discover import discover_streams

from .mock_data_generator import MockDataGenerator


# ------------------------------------------------------------------ #
#  Paths
# ------------------------------------------------------------------ #

SCHEMAS_DIR = os.path.join(
    os.path.dirname(os.path.realpath(tap_bigcommerce.__file__)),
    'schemas',
)

# ------------------------------------------------------------------ #
#  Stream configuration — tap-specific
# ------------------------------------------------------------------ #

STREAM_CONFIG = {
    'orders': {
        'replication_method': 'INCREMENTAL',
        'replication_key': 'date_modified',
        'record_count': 2,
    },
    'products': {
        'replication_method': 'INCREMENTAL',
        'replication_key': 'date_modified',
        'record_count': 2,
    },
    'customers': {
        'replication_method': 'INCREMENTAL',
        'replication_key': 'date_modified',
        'record_count': 2,
    },
    'coupons': {
        'replication_method': 'FULL_TABLE',
        'replication_key': None,
        'record_count': 1,
    },
}

ALL_STREAM_IDS = set(STREAM_CONFIG.keys())

INCREMENTAL_STREAMS = {
    name for name, cfg in STREAM_CONFIG.items()
    if cfg['replication_method'] == 'INCREMENTAL'
}

FULL_TABLE_STREAMS = {
    name for name, cfg in STREAM_CONFIG.items()
    if cfg['replication_method'] == 'FULL_TABLE'
}


class BigCommerceBaseTest:
    """Shared helpers and metadata expectations for mock integration tests."""

    default_start_date = "2023-01-01T00:00:00Z"
    PRIMARY_KEYS = "primary_keys"

    default_config = {
        "client_id": "test_client_id",
        "access_token": "test_access_token",
        "store_hash": "test_store_hash",
        "start_date": "2023-01-01T00:00:00Z",
    }

    ALL_STREAM_IDS = ALL_STREAM_IDS

    # ------------------------------------------------------------------ #
    #  Dynamic mock data
    # ------------------------------------------------------------------ #

    _generator = MockDataGenerator(SCHEMAS_DIR)

    @classmethod
    def _get_mock_records(cls, stream_name):
        """Return dynamically generated mock records for a stream."""
        cfg = STREAM_CONFIG[stream_name]
        count = cfg['record_count']
        rep_key = cfg['replication_key']
        overrides = {}

        # For incremental streams, ensure date_modified values are
        # after the default start_date and distinguishable
        if rep_key:
            records = []
            for i in range(count):
                rec = cls._generator.generate_record(
                    stream_name, seed=i, overrides=overrides)
                # Set deterministic, increasing date_modified values
                rec[rep_key] = f"2024-0{i + 1}-15T10:00:00.000000Z"
                if 'date_created' in rec:
                    rec['date_created'] = f"2023-0{i + 1}-01T00:00:00.000000Z"
                records.append(rec)
            return records

        return cls._generator.generate_records(
            stream_name, count=count, overrides=overrides)

    # ------------------------------------------------------------------ #
    #  Expected metadata
    # ------------------------------------------------------------------ #

    @classmethod
    def expected_metadata(cls):
        return {
            name: {cls.PRIMARY_KEYS: {"id"}}
            for name in ALL_STREAM_IDS
        }

    # ------------------------------------------------------------------ #
    #  Mock client
    # ------------------------------------------------------------------ #

    @classmethod
    def _create_mock_client(cls):
        """Create a mock BigCommerce client with dynamically generated data."""
        client = MagicMock()
        client.is_authorized.return_value = True
        client.authorized = True

        def _make_incremental_generator(stream_name):
            def gen(replication_key=None, bookmark=None):
                for rec in copy.deepcopy(cls._get_mock_records(stream_name)):
                    yield rec
            return gen

        def _make_full_table_generator(stream_name):
            def gen():
                for rec in copy.deepcopy(cls._get_mock_records(stream_name)):
                    yield rec
            return gen

        for name, cfg in STREAM_CONFIG.items():
            if cfg['replication_method'] == 'INCREMENTAL':
                setattr(client, name, _make_incremental_generator(name))
            else:
                setattr(client, name, _make_full_table_generator(name))

        return client

    # ------------------------------------------------------------------ #
    #  Catalog helpers
    # ------------------------------------------------------------------ #

    @classmethod
    def _run_discover(cls):
        """Run discover_streams() and return a Catalog."""
        client = cls._create_mock_client()
        raw = discover_streams(client)
        return Catalog.from_dict(raw)

    @classmethod
    def _make_selected_catalog(cls, stream_names=None):
        """Build a catalog with selected=True for the given streams.
        If stream_names is None, select all streams."""
        catalog = cls._run_discover()
        for entry in catalog.streams:
            is_selected = stream_names is None or entry.tap_stream_id in stream_names
            mdata = metadata.to_map(entry.metadata)
            mdata = metadata.write(mdata, (), 'selected', is_selected)
            entry.metadata = metadata.to_list(mdata)
        return catalog

    # ------------------------------------------------------------------ #
    #  Bookmark helpers
    # ------------------------------------------------------------------ #

    @classmethod
    def get_max_bookmark(cls, stream_name):
        """Return the max replication_key value from mock records."""
        cfg = STREAM_CONFIG[stream_name]
        rep_key = cfg['replication_key']
        if not rep_key:
            return None
        records = cls._get_mock_records(stream_name)
        return max(rec[rep_key] for rec in records)

    @classmethod
    def get_initial_bookmark_date(cls, stream_name):
        """Return a date between default_start_date and the first record."""
        records = cls._get_mock_records(stream_name)
        cfg = STREAM_CONFIG[stream_name]
        rep_key = cfg['replication_key']
        if not rep_key or not records:
            return None
        # Return a date before all mock records
        return "2024-01-01T00:00:00.000000Z"
