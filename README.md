This is an implementation of [go-datastore](https://github.com/ipfs/go-datastore) that is backed by DynamoDB.

ddbds includes support for optimized prefix queries. When you setup your table's key schema correctly and register it with ddbds, then incoming queries that match the schema will be converted into DynamoDB queries instead of table scans, enabling high performance, ordered, high-cardinality prefix queries.

Note that ddbds currently only stores values up to 400 kb (the DynamoDB maximum item size). This makes ddbds inappropriate for block storage. It could be extended to fall back to S3, but that is not yet implemented. Within the InterPlanetary ecosystem, it's designed for storing DHT records, IPNS records, peerstore records, etc.

## Setup ##

### Simple Setup with Unoptimized Queries ###
ddbds can be used as a simple key-value store, without optimized queries.

In this case, all datastore queries will result in full table scans using the ParallelScan API, and filtering/ordering/etc. will be performed client-side.

This is a good option if your table is small or your data and access patterns would not significantly benefit from optimized queries.

### Advanced Setup with Optimized Prefix Queries ###
Certain types of datastore queries can be optimized using DynamoDB features:

* Prefix queries
  * Can be mapped to Global Secondary Indexes (GSIs) for reducing the number of items needed to query/scan
  * Very useful for high-cardinality prefixes like DHT Provider records
* Ordering by key
  * DynamoDB keep results under a "partition key" sorted by the "sort key". We can use this property to return streaming sorted results, otherwise we'd need to buffer all results in-memory first in order to sort client-side.

You can create your table with such indexes and register them with ddbds, and it will "do the right thing", including:

* Writing items with the correct key schema so that they are propagated into the correct GSIs
* Optimize queries that match the prefixes so that they use the correct GSI

This will result in reduced read capacity utilization and lower latency, and as a result, better application scalability.

To do this, define a "key transform" which describes how a datastore key maps to a GSI key:

```go
ddbDS := ddbds.New(
	ddbClient,
	"TableName",
	ddbds.WithKeyTransform(ddbds.KeyTransform{
		Prefix: "/providers",
		QueryIndex: "ProvidersQueryIndex",
		PartitionKeyName: "ContentHash",
		SortKeyName: "PeerID",
	}),
)
```

Fore example, when you write an entry into the datastore such as `/providers/<multihash>/<peerid>`:

* ddbds will write this into the main table with the following attributes:
  * `Key`: `/providers/<multihash>/<peerid>`  (the main key)
  * `ContentHash`: `<multihash>`  (the partition key of the `ProvidersQueryIndex` GSI)
  * `PeerID`: `<peerid>`  (the sort key of the `ProvidersQueryIndex` GSI)
  
Since `ContentHash` and `PeerID` are the partition and sort keys of the `ProvidersQueryIndex` GSI, this entry will be populated into the GSI by DynamoDB, and will surface in queries such as `datastore.Query{Prefix: "/providers/<multihash>", Orders: []query.Order{&query.OrderByKeyDescending{}}}`:

* ddbds iterates through its list of `KeyTransforms` and finds that the prefix matches the transform for `ProvidersQueryIndex`
* Since the index has a sort key, and the prefix includes the partition key, then ddbds will issue a DynamoDB Query
  * GSI: `ProvidersQueryIndex`
  * Partition Key: `ContentHash = <mulithash>`
  * ScanIndexForward = false (for descending order)
* DynamoDB returns all keys under `/providers/<multihash>` sorted descending by key

## Datastore Feature Support ##

### TTL ✓ ###
ddbds implements the TTL datastore interface. To use this, you need to enable TTL support in your table and set the TTL field to `Expiration`. This field is auto-populated by ddbds and is the correct Unix epoch millisecond timestamp.


### Transactions 🗴 ###
ddbds does not implement the Txn interface for datastore transactions. This may be feasible, but would need more investigation. Since GSIs are eventually consistent, this may have limited utility.

### Batching 🗴 ###
ddbds uses naive batching and doesn't yet use DynamoDB batch operations. This is feasible to implement.
