
import os
import json
import singer
from singer import metadata
from singer import utils

logger = singer.get_logger().getChild('tap-bigcommerce')


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
        """
        Load schema from JSON and resolve shared $ref
        """
        refs = {}
        schema_file = "schemas/{}.json".format(self.name)
        shared_schemas_path = get_abs_path('schemas/shared')
        shared_file_names = [
            f for f in os.listdir(shared_schemas_path) if os.path.isfile(
                os.path.join(shared_schemas_path, f)
            )
        ]

        for shared_file in shared_file_names:
            with open(
                os.path.join(shared_schemas_path, shared_file)
            ) as data_file:
                refs[shared_file] = json.load(data_file)

        with open(get_abs_path(schema_file)) as f:
            schema = json.load(f)

        schema = singer.resolve_schema_references(schema, refs)

        return schema

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

        if self.auto_select_fields:
            # automatically include every property
            mdata = metadata.write(
                mdata, (), 'selected', True
            )

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

            if self.auto_select_fields:
                # automatically include every property
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

        self.bookmark_start = self.get_bookmark(state)

        res = get_data(
            replication_key=self.replication_key,
            bookmark=self.bookmark_start
        )

        if self.replication_method == "INCREMENTAL":
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
    replication_key = "id"


class Customers(Stream):
    name = "customers"


STREAMS = {
    'orders': Orders,
    'products': Products,
    'coupons': Coupons,
    'customers': Customers
}
