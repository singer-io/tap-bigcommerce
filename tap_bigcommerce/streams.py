
import os
import json

import singer
from singer import metadata
from singer import utils
from singer.transform import Transformer, NO_INTEGER_DATETIME_PARSING

from tap_bigcommerce.client import Client

logger = singer.get_logger().getChild('tap-bigcommerce')

KEY_PROPERTIES = ['id']
DEFAULT_REPLICATION_KEY = 'date_modified'
DEFAULT_REPLICATION_METHOD = 'INCREMENTAL'


def parse_datetime(value):
    transformer = Transformer(NO_INTEGER_DATETIME_PARSING)
    return transformer._transform_datetime(value)


class TapSchemaException(Exception):
    pass


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


class Stream():
    name = None
    replication_method = DEFAULT_REPLICATION_METHOD
    replication_key = DEFAULT_REPLICATION_KEY
    stream = None
    key_properties = KEY_PROPERTIES
    session_bookmark = None

    bookmark_default = None

    def __init__(self, client: Client):
        self.client = client

    def is_session_bookmark_old(self, value):
        if self.session_bookmark is None:
            return True
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(
            self.session_bookmark)

    def update_session_bookmark_if_old(self, value):
        if self.is_session_bookmark_old(value):
            self.session_bookmark = value

    # def set_bookmark_default(self, default):
    #     self.bookmark_default = default

    def get_bookmark(self, state):
        return singer.get_bookmark(state, self.name, self.replication_key)

    def update_bookmark_if_old(self, state, value):
        if self.is_bookmark_old(state, value):
            singer.write_bookmark(
                state, self.name, self.replication_key, value
            )

    def is_bookmark_old(self, state, value):
        current_bookmark = self.get_bookmark(state)
        if current_bookmark is None:
            return True
        if value is None:
            return False
        return utils.strptime_with_tz(value) > utils.strptime_with_tz(
            current_bookmark)

    def load_schema(self):
        """
        Load schema from JSON and resolve shared $ref
        """
        shared_schemas_path = get_abs_path('schemas/shared')
        shared_file_names = [
            f for f in os.listdir(shared_schemas_path) if os.path.isfile(
                os.path.join(shared_schemas_path, f)
            )
        ]

        refs = {}
        for shared_file in shared_file_names:
            with open(
                os.path.join(shared_schemas_path, shared_file)
            ) as data_file:
                refs[shared_file] = json.load(data_file)

        schema_file = "schemas/{}.json".format(self.name)

        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)

        schema = singer.resolve_schema_references(schema, refs)

        return schema

    def load_metadata(self):
        schema = self.load_schema()
        mdata = metadata.new()

        mdata = metadata.write(
            mdata, (), 'table-key-properties', self.key_properties)

        mdata = metadata.write(
            mdata, (), 'forced-replication-method', self.replication_method)

        mdata = metadata.write(
            mdata, (), 'selected-by-default', True)

        if self.replication_key:
            mdata = metadata.write(
                mdata, (), 'valid-replication-keys', [self.replication_key])

        for field_name in schema['properties'].keys():
            if field_name in self.key_properties or \
                    field_name == self.replication_key:
                mdata = metadata.write(
                    mdata,
                    ('properties', field_name),
                    'inclusion',
                    'automatic'
                )
            else:
                mdata = metadata.write(
                    mdata,
                    ('properties', field_name),
                    'inclusion',
                    'available'
                )

            mdata = metadata.write(
                mdata,
                ('properties', field_name),
                'selected',
                True
            )

        return metadata.to_list(mdata)

    def is_selected(self):
        return self.stream is not None

    # The main sync function.
    def sync(self, state):
        get_data = getattr(self.client, self.name)
        bookmark = self.get_bookmark(state)
        res = get_data(replication_key=self.replication_key, bookmark=bookmark)

        if self.replication_method == "INCREMENTAL":
            for i, item in enumerate(res):
                try:
                    if self.is_bookmark_old(state, item[self.replication_key]):
                        # must update bookmark when the entire stream is
                        # consumed instead, we use a temporary
                        # `session_bookmark`.
                        self.update_session_bookmark_if_old(
                            parse_datetime(item[self.replication_key])
                        )

                        yield (self.stream, item)

                    # write state every 100 rows
                    if i % 100:
                        self.update_bookmark_if_old(
                            state, self.session_bookmark
                        )

                except Exception as e:
                    logger.error(
                        'Handled exception: {error}'.format(error=str(e))
                    )
                    pass

        elif self.replication_method == "FULL_TABLE":
            for item in res:
                yield (self.stream, item)

        else:
            raise Exception(
                'Replication method not defined for {stream}'.format(stream=self.name)
            )

        # After the sync, then set the bookmark based off session_bookmark.
        self.update_bookmark_if_old(
            state, self.session_bookmark
        )

    def __repr__(self):
        properties = {}
        for prop in [a for a in dir(self) if not a.startswith('__')]:
            if type(getattr(self, prop)) in (str,int,bool):
                properties[prop] = getattr(self, prop)
            # else:
            #     properties[prop] = getattr(self, prop).__str__()

        return(json.dumps(properties, indent=2))


class Orders(Stream):
    name = "orders"


class Products(Stream):
    name = "products"


class Coupons(Stream):
    name = "coupons"


class Customers(Stream):
    name = "customers"


STREAMS = {
    'orders': Orders,
    'products': Products,
    'coupons': Coupons,
    'customers': Customers
}
