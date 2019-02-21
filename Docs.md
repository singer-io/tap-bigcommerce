# BigCommerce

## Connecting to BigCommerce

To set up BigCommerce in Stitch, you will need BigCommerce API credentials.:

Credentials can be generated from the administrator account at [login.bigcommerce.com](https://login.bigcommerce.com). Note, legacy API accounts are not supported.

* Configuration requires the following parameters: `client_id`, `access_token` and `store_hash`. 
* API account requires the following permissions for each resource:
	* Orders stream requires _read_ permission on Orders
	* Customers stream requires _read_ permission on Customers
	* Products stream requires _read_ permission on Products
	* Coupons stream requires _read_ permission on Marketing



## Replication

Orders and Customers are updated incrementally based on date_modified parameter.

Products table also uses incremental replication, but does not use the `start_date` parameter on first invocation, so that all products are replicated on first run, with the oldest `date_modified` field being saved in the state file for subsequent invocations. 

#### Nested Resources

Some BigCommerce objects contain nested resources. For instance, the Orders table contains `products`, `coupons` and `shipping_address`. These resources require additional API requests to fetch.

Primary endpoints request 

TODO: Asyncrounous requests of nested resources while respecting rate limit responce


## BigCommerce Table Schemas

### Orders
Replicates BigCommerce Orders resource incrementally based on the `date_modified` parameter. This is due to the fact that an order will be updated serveral times after creation. It also means that multiple records may appear in the final database for a single order `id`.

Primary Key: `id`

Replication Method: INCREMENTAL

Bookmark Column: `date_modified`


### Customers

Primary Key: `id`

Replication Method: INCREMENTAL

Bookmark Column: `date_modified`


### Products

Primary Key: `id`

Replication Method: INCREMENTAL

Bookmark Column: `date_modified`

### Coupons

Coupons do not have a `date_modified` field, and replicated on the `id` field could mean edits to coupons after creation would not be synced. Instead, we use a full table replication, but store the timestamp of the last sync in the state file. Then, within the configuration, we can select the minimum number of hours that must have elapsed before the Coupons table is replicated again - default is 24 hours - that way you only replicate the table once a day and not on every tap run.


Primary Key: `id`

Replication Method: FULL_TABLE

Bookmark Column: `last_sync`



## Troubleshooting

