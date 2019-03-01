#!/usr/bin/env python3
import sys
import json
import singer
from singer import utils, metadata, Catalog

from tap_bigcommerce.client import BigCommerce
from tap_bigcommerce.discover import discover_streams
from tap_bigcommerce.streams import STREAMS
from tap_bigcommerce.sync import sync_stream

REQUIRED_CONFIG_KEYS = [
    "start_date", "client_id", "access_token", "store_hash"
]

logger = singer.get_logger().getChild('tap-bigcommerce')


def do_discover(client):
    logger.info("Starting discover")
    catalog = discover_streams(client)
    json.dump(catalog, sys.stdout, indent=2)
    logger.info("Finished discover")


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names


def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream


def ensure_credentials_are_authorized(client):
    if client.is_authorized() is False:
        raise Exception("BigCommerce Client not authorized.")


def do_sync(client, catalog, state, start_date):
    ensure_credentials_are_authorized(client)
    selected_stream_names = get_selected_streams(catalog)
    populate_class_schemas(catalog, selected_stream_names)

    if state.get('bookmarks') is None:
        state = {'bookmarks': {}}

    for stream in catalog.streams:
        stream_name = stream.tap_stream_id

        mdata = metadata.to_map(stream.metadata)

        if stream_name not in selected_stream_names:
            logger.info("%s: Skipping - not selected", stream_name)
            continue

        singer.write_schema(
            stream_name,
            stream.schema.to_dict(),
            metadata.get(mdata, (), 'table-key-properties')
        )

        logger.info("%s: Starting sync", stream_name)

        instance = STREAMS[stream_name](client)
        instance.stream = stream
        if instance.replication_method == "INCREMENTAL":
            if state['bookmarks'].get(
                stream.tap_stream_id, {}
            ).get(instance.replication_key) is None:
                state['bookmarks'][stream.tap_stream_id] = {
                    instance.replication_key: start_date
                }

        counter_value = sync_stream(state, instance)

        singer.write_state(state)

        logger.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    logger.info("Finished sync")


@utils.handle_top_exception(logger)
def main():

    # DEBUG
    # logger.setLevel('DEBUG')

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    # create instance of BigCommerce client
    bigcommerce = BigCommerce(
        client_id=config['client_id'],
        access_token=config['access_token'],
        store_hash=config['store_hash']
    )

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        do_discover(bigcommerce)
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = Catalog.from_dict(discover_streams(bigcommerce))

        do_sync(
            client=bigcommerce,
            catalog=catalog,
            state=args.state,
            start_date=config['start_date']
        )


if __name__ == "__main__":
    main()
