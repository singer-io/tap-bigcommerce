#!/usr/bin/env python3
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
            'metadata': s.load_metadata()
        })

    return {"streams": streams}
