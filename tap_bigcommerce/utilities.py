#!/usr/bin/env python
import os
import json
import pytz
from singer import resolve_schema_references


def to_utc(dtime):
    return dtime.replace(tzinfo=pytz.UTC)


def get_abs_path(path, file=None):
    if file is None:
        file = __file__
    return os.path.join(
        os.path.dirname(os.path.realpath(file)), path)


class SchemaLoader():

    def __init__(
            self,
            schema_path=get_abs_path('schemas'),
            shared_schemas_path=get_abs_path('schemas/shared')):

        self.schema_path = schema_path
        self.shared_schemas_path = shared_schemas_path

    def load(self, name):
        """
        Load schema from JSON and resolve shared $ref
        """
        refs = {}
        shared_file_names = [
            f for f in os.listdir(self.shared_schemas_path) if os.path.isfile(
                os.path.join(self.shared_schemas_path, f)
            )
        ]

        for shared_file in shared_file_names:
            with open(
                os.path.join(self.shared_schemas_path, shared_file)
            ) as data_file:
                refs[shared_file] = json.load(data_file)

        schema_file = self.schema_path + "/{}.json".format(name)
        with open(schema_file) as f:
            schema = json.load(f)

        schema = resolve_schema_references(schema, refs)

        return schema
