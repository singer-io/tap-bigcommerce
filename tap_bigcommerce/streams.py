#!/usr/bin/env python
from singer import metadata
from singer import utils
import os
import singer
import tap_bigcommerce.utilities as tap_utils


logger = singer.get_logger().getChild('tap-bigcommerce')

schema_loader = tap_utils.SchemaLoader()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


class Stream():
    name = None
    replication_method = 'INCREMENTAL'
    replication_key = 'date_modified'
    stream = None
    key_properties = ['id']
    bookmark_start = None
    session_bookmark = None
    filter_records_by_bookmark = False
    auto_select_fields = True
    sync_full_table_every = 24

    def __init__(self, client):
        self.client = client

    def get_bookmark(self, state):
        return singer.get_bookmark(state, self.name, self.replication_key)

    def is_bookmark_old(self, value, bookmark):
        if value is None:
            return False
        if bookmark is None:
            return True

        if self.replication_key in ['date_modified', 'date_created']:
            return utils.strptime_with_tz(
                value) > utils.strptime_with_tz(bookmark)
        else:
            return value > bookmark

    def update_session_bookmark_if_old(self, value):
        if self.session_bookmark is None:
            self.session_bookmark = value

        if self.is_bookmark_old(value, self.session_bookmark):
            self.session_bookmark = value

    def update_bookmark_if_old(self, state):
        if self.is_bookmark_old(
            self.session_bookmark,
            self.get_bookmark(state)
        ):
            singer.write_bookmark(
                state,
                self.name,
                self.replication_key,
                self.session_bookmark
            )

    def load_schema(self):
        return schema_loader.load(self.name)

    def load_field_metadata(self, mdata, schema, parent=()):
        if 'object' in schema.get('type', []):
            for field_name, field_schema in schema['properties'].items():
                inclusion = 'automatic' if (
                    field_name in self.key_properties or
                    field_name == self.replication_key
                ) and (
                    parent == ()
                ) else 'available'

                breadcrumb = parent + ('properties', field_name)

                mdata = metadata.write(
                    mdata,
                    breadcrumb,
                    'inclusion',
                    inclusion
                )

                mdata = self.load_field_metadata(
                    mdata, field_schema, breadcrumb)

        elif 'array' in schema.get('type', []):
            mdata = self.load_field_metadata(
                mdata, schema.get('items', {}), parent + ('items',))

        return mdata

    def load_metadata(self):
        schema = self.load_schema()

        mdata = metadata.new()

        mdata = metadata.write(
            mdata, (), 'table-key-properties', self.key_properties
        )

        mdata = metadata.write(
            mdata, (), 'forced-replication-method', self.replication_method
        )

        mdata = metadata.write(
            mdata, (), 'selected-by-default', True
        )

        if self.replication_key:
            mdata = metadata.write(
                mdata, (), 'valid-replication-keys', [self.replication_key]
            )

        mdata = self.load_field_metadata(mdata, schema)

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None

    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)

        if self.replication_method == "INCREMENTAL":
            self.bookmark_start = self.get_bookmark(state)
            res = get_data(
                replication_key=self.replication_key,
                bookmark=self.bookmark_start
            )
            for i, item in enumerate(res):
                try:
                    replication_value = item[self.replication_key]

                    if self.is_bookmark_old(replication_value,
                                            self.bookmark_start):

                        yield (self.stream, item)

                        self.update_session_bookmark_if_old(replication_value)
                        self.update_bookmark_if_old(state)

                except Exception as e:
                    logger.error(
                        'Handled exception: {error}'.format(error=str(e))
                    )
                    pass

        elif self.replication_method == "FULL_TABLE":
            res = get_data()

            for item in res:
                yield (self.stream, item)

        else:
            raise Exception(
                'Replication method not defined for {stream}'.format(
                    stream=self.name
                )
            )


class Orders(Stream):
    name = "orders"


class Products(Stream):
    name = "products"


class Coupons(Stream):
    name = "coupons"
    replication_method = "FULL_TABLE"
    replication_key = None


class Customers(Stream):
    name = "customers"


STREAMS = {
    'products': Products,
    'coupons': Coupons,
    'customers': Customers,
    'orders': Orders
}
