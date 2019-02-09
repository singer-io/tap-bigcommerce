"""
Discovery Mode: describe data streams available and creates a
catalog.json object

https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md

Schemas can either be hardcoded or defined from an API.

TODO:

1)  Develop generic template for discover mode that can work with
    either hard-coded schema files or a API-defined

2)  Command-line utility for customizing catalog.json based on stream
    field selection / inclusion


"""

import os
import singer

from tap_bigcommerce.streams import STREAMS


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def discover_streams(client):
    streams = []

    for s in STREAMS.values():
        s = s(client)
        schema = singer.resolve_schema_references(s.load_schema())
        streams.append({
            'stream': s.name,
            'tap_stream_id': s.name,
            'schema': schema,
            'replication_key': s.replication_key,
            'metadata': s.load_metadata()
        })

    return streams
