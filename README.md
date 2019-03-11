# tap-bigcommerce

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data following the
[Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
for [BigCommerce](https://developer.bigcommerce.com/).


## Quick Start

1. Set up a virtual environment and install this tap. See the
    [Singer-io Getting Started Guide](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap)
    for help.

1. Create the `config.json` file.

    ```
    {
      "client_id": "xxxxxxxxx",
      "access_token": "xxxxxxxxx",
      "store_hash": "xxxxxxxxx",
      "start_date": "2017-01-01T00:00:00Z"
    }
    ```

1. Run the tap in
   [Discovery Mode](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode)
   to create the
   [Catalog](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#the-catalog).

    ```
    $ tap-bigcommerce --config config.json --discover > catalog.json
    ```

1. Select the fields you wish to sync. See
   [Field Selection](#field-selection) below for more information.

1. Run the tap in Sync Mode.

    ```
    $ tap-bigcommerce --config config.json --catalog catalog.json
    ```

## About this tap

### The Config

The config file is used to authenticate into BigCommerce. The Legacy API
credentials not accepted.

* Orders stream requires _read_ permission on Orders
* Customers stream requires _read_ permission on Customers
* Products stream requires _read_ permission on Products
* Coupons stream requires _read_ permission on Marketing

`start_date` is used for resources that can be filtered by
`date_modified` - `orders`, `customers` and `products`

### Discovery mode

This command returns a JSON that describes the schema of each table.

```
$ tap-bigcommerce --config config.json --discover
```

To save this to `catalog.json`:

```
$ tap-bigcommerce --config config.json --discover > catalog.json
```

### Field selection

You can tell the tap to extract specific fields by editing `catalog.json`
to make selections (or use the
[Singer Discover](https://github.com/chrisgoddard/singer-discover)
utility. You can change metadata for specific fields or tables and change
the "selected" field value to false.

```
"metadata": [
  {
    "breadcrumb": [],
    "metadata": {
      "table-key-properties": [
        "id"
      ],
      "forced-replication-method": "INCREMENTAL",
      "selected-by-default": true,
      "valid-replication-keys": [
        "date_modified"
      ]
    }
  },
  {
    "breadcrumb": [
      "properties",
      "id"
    ],
    "metadata": {
      "inclusion": "automatic",
      "selected": true
    }
  },
  ...
```

### Sync Mode

With an annotated `catalog.json`, the tap can be invoked in sync mode:

```
$ tap-bigcommerce --config config.json --catalog catalog.json
```

Messages are written to standard output following the Singer
specification. The resultant stream of JSON data can be consumed by a
Singer target.


## Replication Methods and State File

Use the following command to pipe the tap output into your Singer target
of choice and update the state file in one go.

```
tap-bigcommerce --config config.json --catalog catalog.json --state state.json | target > state.json.tmp && tail -1 state.json.tmp > state.json
```

---

Copyright &copy; 2019 Stitch
