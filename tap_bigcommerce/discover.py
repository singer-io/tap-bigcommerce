#!/usr/bin/env python3
import singer

from tap_bigcommerce.streams import STREAMS
from tap_bigcommerce.bigcommerce import BigCommerceForbiddenError

LOGGER = singer.get_logger()


def _apply_access_checks(stream_instances):
    """
    Probe each stream for read access and return only the accessible ones.

    Streams whose credentials return HTTP 403 are excluded from the catalog
    and a warning is logged for each. Raises BigCommerceForbiddenError if no
    streams are accessible, since discovery would produce an empty catalog.

    Args:
        stream_instances: list of Stream instances to probe.

    Returns:
        list of Stream instances that are accessible.
    """
    accessible_streams = []
    inaccessible_streams = []

    for s in stream_instances:
        if s.check_access():
            accessible_streams.append(s)
        else:
            inaccessible_streams.append(s.name)

    if not accessible_streams:
        raise BigCommerceForbiddenError(
            "403 Forbidden: No read access to supported streams. Data collection cannot start."
        )

    if inaccessible_streams:
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following "
            "stream(s): %s. These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )

    return accessible_streams


def discover_streams(client):
    """
    Run discovery, probe each stream for access, and return the catalog dict.

    Streams the credentials cannot access (HTTP 403) are excluded from the
    returned catalog instead of raising an error, allowing partial discovery.
    """
    stream_instances = [s(client) for s in STREAMS.values()]
    accessible_instances = _apply_access_checks(stream_instances)

    streams = []
    for s in accessible_instances:
        schema = singer.resolve_schema_references(s.load_schema())
        streams.append({
            'stream': s.name,
            'tap_stream_id': s.name,
            'schema': schema,
            'metadata': s.load_metadata()
        })

    return {"streams": streams}
