# BigCommerce

## Connecting to BigCommerce

To set up BigCommerce in Stitch, you will need BigCommerce API credentials.:

Credentials can be generated from the administrator account at [login.bigcommerce.com](https://login.bigcommerce.com).

**Note: legacy API accounts are not supported.**

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

Some BigCommerce objects contain nested resources. For instance, the Orders table contains `products`, `coupons` and `shipping_address`. These resources are requested asyncronously, though the tap should still respect the API rate limit.

#### API Quota

BigCommerce uses a window-based request quota that is set based on the plan level you have. Enterpise plans have an extremely generous quota (7mil / 30sec) while Plus and Standard plans accounts are very limited (150 / 30sec).

The Standard Plan limit is low enough that some tables with a lot of nested resources (like products) have to have their results-per-page lowered.

In this case you will see a logged message like: ```Warning: Rate Limit Exhausted. Waiting 28.3 seconds```

This does mean that large backfills on low-tier accounts may take quite a long time.


## BigCommerce Table Schemas

### Orders

Endpoint: [/v2/orders/](https://developer.bigcommerce.com/api-reference/orders/orders-api)

Replicates BigCommerce Orders resource incrementally based on the `date_modified` parameter. This is due to the fact that an order will be updated serveral times after creation. It also means that multiple records may appear in the final database for a single order `id`.

* Primary Key: `id`
* Replication Method: INCREMENTAL
* Bookmark Column: `date_modified`


### Customers
Endpoint: [/v2/customers/](https://developer.bigcommerce.com/api-reference/customer-subscribers/customers-api/customers/getallcustomers)

* Primary Key: `id`
* Replication Method: INCREMENTAL
* Bookmark Column: `date_modified`

_**Note**: Customer table is replicated independently from Order table. An order may be replicated without a corresponding customer entry if the customer data was not modified when a new order was created._

### Products

Endpoint: [/v3/catalog/products](https://developer.bigcommerce.com/api-reference/catalog/catalog-api/products/getproducts)

* Primary Key: `id`
* Replication Method: INCREMENTAL
* Bookmark Column: `date_modified`

### Coupons
Endpoint: [/v2/coupons](https://developer.bigcommerce.com/api-reference/catalog/catalog-api/products/getproducts)

The Coupons table does not have a `date_modified` field, and replicated on the `id` field could mean edits to coupons after creation would not be synced. Therefore we use FULL_TABLE replication.

To avoid creating too many duplicative rows, it is recommended that you create a separate instance of this tap that syncs less often - for instance once ever 12 or 24 hours.

* Replication Method: FULL_TABLE