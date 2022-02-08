This is an implementation of [go-datastore](https://github.com/ipfs/go-datastore) that is backed by DynamoDB.

ddbds includes support for optimized prefix queries. When you setup your table's key schema correctly and register it with ddbds, then incoming queries that match the schema will be converted into DynamoDB queries instead of table scans, enabling high performance, ordered, high-cardinality prefix queries.

Note that ddbds currently only stores values up to 400 kb (the DynamoDB maximum item size). This makes ddbds inappropriate for block storage. It could be extended to fall back to S3, but that is not yet implemented. Within the InterPlanetary ecosystem, it's designed for storing DHT records, IPNS records, peerstore records, etc.

## Setup ##

### Simple Setup with Unoptimized Queries ###
ddbds can be used as a simple key-value store, without optimized queries.

In this case, queries will result in full table scans using the ParallelScan API, and filtering/ordering/etc. will be performed client-side.

This is a good option if your table is small or your data and access patterns would not benefit.

### Advanced Setup with Optimized Prefix Queries ###
The datastore interface supports "prefix queries", which filters entries in the datastore by their key prefix. By default, ddbds will perform full table scans for prefix queries and filter the keys client-side.

If you know your application's prefix query patterns, then you can setup ddbds to use the Query API instead of ParallelScan. This uses DynamoDB indices to filter and sort your data server-side, resulting in reduced read capacity consumption and improved latency and scalability.

For example, IPFS stores DHT provider records with the schema `/providers/<content_multihash>/<peer_id>`. When IPFS searches for providers of a given CID, it performs a prefix query on `/providers/<content_multihash>` to find the peer IDs that provide the CID.

With the "simple setup" above, such a query would scan _the entire table_, and the client-side would need to filter by multihash, which is slow and costly. 

If we split the key into a partition and sort key, then we can perform DynamoDB Queries instead of Scans:

- Partition Key: `/providers/<content_multihash>`
- Sort Key: `<peer_id>`

Then ddbds can issue a Query over the partition key `/providers/<content_multihash>`, returning only the peers for that multihash, which is much faster and cheaper.

To do this:

- Setup your table with the partition and sort keys (they always have the "string" type)
- Define two regexes
  - Key Pattern
    - Takes a datastore key like `/providers/<content_multihash>/<peer_id>` and captures the partition and sort key in the first and second group, respectively
	- Example: `^(/providers/.*?)/(.*)$`
      - First group (partition key): `/providers/<content_multihash>`
	  - Second group (sort key): `<peer_id>`
    - On a Put, Get, etc., if the datastore key matches this pattern, then the DS key is converted into the index key attributes and added to the item
  - Prefix Pattern
    - Takes a datastore query prefix like `/providers/<content_multihash>` and determines if the prefix matches the partition key schema of your DynamoDB table
    - If the regex matches, a DynamoDB Query is performed
    - Example: `^/providers/[^/]*$`

### TTL ###
ddbds implements the TTL datastore interface. To use this, you need to enable TTL support in your table and set the TTL field to `Expiration`. This field is auto-populated by ddbds and is the correct Unix epoch millisecond timestamp.


### Transactions ###
ddbds does not implement the Txn interface for datastore transactions. This is feasible to implement with DynamoDB Transactions, it just hasn't happened yet.

### Batching ###
ddbds uses naive batching and doesn't yet use DynamoDB batch operations. This is feasible to implement.

## Caveats ##
Optimized queries use Global Secondary Indexes, which are eventually-consistent. This means if you Put a value and then Query, it may not immediately appear in the query results. It generally propagates within a second, which is usually an acceptable tradeoff.

DynamoDB Queries are paginated, and following the pagination is not parallelizable. A single page returns up to 1 MB of items, which can contain thousands of entries. So in some cases, a parallel scan may be faster than a query.

If this tradeoff is unacceptable, please open an issue with your use case. It is possible to use separate tables with transactions for strong consistency, but it is not implemented because it is more complex (both internally and for users). Another downside of this approach is that you can't add new query prefixes without manually backfilling data. When a new GSI is created, the backfill happens automatically, so that you can easily add optimizations over time.
