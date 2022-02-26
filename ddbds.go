package ddbds

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	golog "github.com/ipfs/go-log/v2"
	"go.uber.org/zap"
)

const (
	attrNameDSKey      = "DSKey"
	attrNameSize       = "Size"
	attrNameExpiration = "Expiration"
)

var (
	log = golog.Logger("ddbds")

	ErrNoMatchingKeyTransforms = errors.New("no matching key transforms")
)

type Options struct {
	UseStronglyConsistentReads bool
	ScanParallelism            int
	KeyTransforms              []KeyTransform
}

func WithStronglyConsistentReads() func(o *Options) {
	return func(o *Options) {
		o.UseStronglyConsistentReads = true
	}
}

func WithScanParallelism(n int) func(o *Options) {
	return func(o *Options) {
		o.ScanParallelism = n
	}
}

func WithKeyTransform(k KeyTransform) func(o *Options) {
	return func(o *Options) {
		o.KeyTransforms = append(o.KeyTransforms, k)
	}
}

func New(ddbClient *dynamodb.DynamoDB, optFns ...func(o *Options)) (*ddbDatastore, error) {
	opts := Options{}
	for _, o := range optFns {
		o(&opts)
	}

	ddbDS := &ddbDatastore{
		ddbClient:                  ddbClient,
		ScanParallelism:            opts.ScanParallelism,
		useStronglyConsistentReads: opts.UseStronglyConsistentReads,
		keyTransforms:              opts.KeyTransforms,
	}

	if ddbDS.ScanParallelism == 0 {
		ddbDS.ScanParallelism = 1
	}

	return ddbDS, nil
}

type ddbDatastore struct {
	ddbClient *dynamodb.DynamoDB

	useStronglyConsistentReads bool

	// Controls the parallelism of scans that are preformed for unoptimized datastore queries.
	// Unoptimized datastore queries are queries without registered query prefixes, and always
	// result in full table scans.
	ScanParallelism int

	// The key transforms that specify how to generate keys for indices.
	// These should be ordered from more-specific to less-specific, because when querying, this
	// uses the first match.
	// For example, if you have a prefix on /foo and /foo/bar, then /foo/bar should be before /foo
	// in this slice, so that queries for /foo/bar/baz use the /foo/bar index and not the /foo index.
	keyTransforms []KeyTransform
}

var _ ds.Datastore = (*ddbDatastore)(nil)

// ddbItem is a raw DynamoDB item.
// Note that some attributes may not be present if a projection expression was used when reading the item.
type ddbItem struct {
	DSKey      string
	Value      []byte `dynamodbav:",omitempty"`
	Size       int64
	Expiration int64
}

func (d *ddbItem) GetExpiration() time.Time {
	return time.Unix(d.Expiration, 0)
}

func unmarshalItem(itemMap map[string]*dynamodb.AttributeValue) (*ddbItem, error) {
	item := &ddbItem{}
	err := dynamodbattribute.UnmarshalMap(itemMap, item)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling item: %w", err)
	}
	return item, nil
}

// makeGetKey makes a DynamoDB key from a datastore key, for GetItem requests.
func (d *ddbDatastore) makeGetKey(key ds.Key) (Key, error) {
	for _, transform := range d.keyTransforms {
		k, ok := transform.GetKey(key)
		if !ok {
			continue
		}
		return k, nil
	}
	return Key{}, ErrNoMatchingKeyTransforms
}

// makePutKey makes a DynamoDB key from a datastore key, for PutItem requests.
// It populates all the attributes for all the registered indices.
func (d *ddbDatastore) makePutKeys(key ds.Key) ([]Key, error) {
	// var keys []Key

	// // add any additional index keys
	// for _, transform := range d.keyTransforms {
	// 	transformKey, ok := transform.PutKey(key)
	// 	if !ok {
	// 		// TODO metric?
	// 		continue
	// 	}
	// 	keys = append(keys, transformKey)
	// }
	// if len(keys) == 0 {
	// 	return nil, ErrNoMatchingKeyTransforms
	// }
	// return keys, nil

	for _, transform := range d.keyTransforms {
		transformKey, ok := transform.PutKey(key)
		if !ok {
			// TODO metric?
			continue
		}
		return []Key{transformKey}, nil
	}
	return nil, ErrNoMatchingKeyTransforms
}

// getItem fetches an item from DynamoDB.
// If attributes is non-nil, only those attributes are fetched. This doesn't reduce consumed read capacity,
// it only reduces the amount of data transferred.
func (d *ddbDatastore) getItem(ctx context.Context, key ds.Key, attributes []string) (*ddbItem, error) {
	var projExpr *string
	if attributes != nil {
		projExpr = aws.String(strings.Join(attributes, ","))
	}

	ddbKey, err := d.makeGetKey(key)
	if err != nil {
		return nil, err
	}

	res, err := d.ddbClient.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:            &ddbKey.Table,
		Key:                  ddbKey.Attrs,
		ConsistentRead:       &d.useStronglyConsistentReads,
		ProjectionExpression: projExpr,
	})
	if err != nil {
		return nil, err
	}
	if res.Item == nil {
		return nil, ds.ErrNotFound
	}
	return unmarshalItem(res.Item)
}

func (d *ddbDatastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	item, err := d.getItem(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func (d *ddbDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	k, err := d.makeGetKey(key)
	if err != nil {
		return false, err
	}

	res, err := d.ddbClient.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:            &k.Table,
		Key:                  k.Attrs,
		ProjectionExpression: aws.String(k.PartitionKeyName), // TODO: if we set this to some non-existent key, will it return an empty item?  that would be betters
	})
	if err != nil {
		return false, err
	}
	return res.Item != nil, nil
}

func (d *ddbDatastore) GetSize(ctx context.Context, key ds.Key) (size int, err error) {
	item, err := d.getItem(ctx, key, []string{attrNameSize})
	if err != nil {
		return 0, err
	}
	return int(item.Size), nil
}

func (d *ddbDatastore) put(ctx context.Context, key ds.Key, value []byte, ttl time.Duration) error {
	keys, err := d.makePutKeys(key)
	if err != nil {
		return err
	}

	req := &dynamodb.TransactWriteItemsInput{
		ClientRequestToken: aws.String(uuid.New().String()),
	}

	for _, putKey := range keys {
		item := &ddbItem{
			Size:  int64(len(value)),
			Value: value,
			DSKey: key.String(),
		}

		if ttl > 0 {
			item.Expiration = time.Now().Add(ttl).Unix()
		}

		itemMap, err := dynamodbattribute.ConvertToMap(*item)
		if err != nil {
			return fmt.Errorf("marshaling item: %w", err)
		}

		log.Debugw("item map to put", "ItemMap", itemMap)

		for k, v := range putKey.Attrs {
			itemMap[k] = v
		}

		log.Debugw("final item map to put", "ItemMap", itemMap)

		req.TransactItems = append(req.TransactItems, &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName: aws.String(putKey.Table),
				Item:      itemMap,
			},
		})
	}

	log.Debug("putting items", zap.Reflect("item", req))

	_, err = d.ddbClient.TransactWriteItemsWithContext(ctx, req)
	log.Debug("done putting items")
	if err != nil {
		return fmt.Errorf("transacting %d DynamoDB items: %w", len(req.TransactItems), err)
	}

	return nil
}

func (d *ddbDatastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	return d.put(ctx, key, value, time.Duration(0))
}

func (d *ddbDatastore) Delete(ctx context.Context, key ds.Key) error {
	k, err := d.makeGetKey(key)
	if err != nil {
		return err
	}

	req := &dynamodb.DeleteItemInput{
		TableName: &k.Table,
		Key:       k.Attrs,
	}

	_, err = d.ddbClient.DeleteItemWithContext(ctx, req)
	if err != nil {
		// if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
		// 	// TODO is this the right behavior, or do we just swallow this?
		// 	return ds.ErrNotFound
		// }
		return err
	}
	return nil
}

func (d *ddbDatastore) Sync(ctx context.Context, prefix ds.Key) error { return nil }

func (d *ddbDatastore) Close() error { return nil }

func (d *ddbDatastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	useNaiveOrders := len(q.Orders) > 0

	var results query.Results

	// search for a matching key transform and take the first one we find
	for _, transform := range d.keyTransforms {
		k, ok := transform.QueryKey(q.Prefix)
		if !ok {
			continue
		}

		if k.ShouldQuery {
			log.Debugw("querying", "QueryTable", transform.Table, "key", k)
			if transform.disableQueries {
				return nil, fmt.Errorf("queries on '%s' are disabled", transform.Table)
			}

			partitionKeyValue := *k.Attrs[k.PartitionKeyName].S
			ddbQuery := &dynamodb.QueryInput{
				TableName:                 &transform.Table,
				KeyConditionExpression:    aws.String(fmt.Sprintf("#k = :v")),
				ExpressionAttributeNames:  map[string]*string{"#k": &k.PartitionKeyName},
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":v": {S: &partitionKeyValue}},
				ConsistentRead:            &transform.UseStronglyConsistentReads,
			}

			// we can only do this with queries and if there is exactly one order,
			// otherwise we have to do the sorting client-side
			if len(q.Orders) == 1 {
				if _, ok := q.Orders[0].(*query.OrderByKeyDescending); ok {
					ddbQuery.ScanIndexForward = aws.Bool(false)
					useNaiveOrders = false
				}
			}

			queryIter := newQueryIterator(d.ddbClient, ddbQuery, q.KeysOnly)
			queryIter.start(ctx)
			results = query.ResultsFromIterator(q, query.Iterator{
				Next:  queryIter.Next,
				Close: queryIter.Close,
			})
		} else {
			log.Debugw("scanning", "ScanTable", transform.Table, "key", k)
			if transform.disableScans {
				return nil, fmt.Errorf("scans on '%s' are disabled", transform.Table)
			}

			scanParallelism := transform.ScanParallelism
			if scanParallelism == 0 {
				scanParallelism = 5
			}

			scanIter := &scanIterator{
				ddbClient: d.ddbClient,
				tableName: transform.Table,
				segments:  d.ScanParallelism,
				keysOnly:  q.KeysOnly,
			}
			scanIter.start(ctx)
			results = query.ResultsFromIterator(q, query.Iterator{
				Next:  scanIter.Next,
				Close: scanIter.Close,
			})
		}

		if results != nil {
			break
		}
	}

	if results == nil {
		return nil, ErrNoMatchingKeyTransforms
	}

	// this is copied from go-datastore since it isn't reusable
	if q.Prefix != "" {
		// Clean the prefix as a key and append / so a prefix of /bar
		// only finds /bar/baz, not /barbaz.
		prefix := q.Prefix
		if len(prefix) == 0 {
			prefix = "/"
		} else {
			if prefix[0] != '/' {
				prefix = "/" + prefix
			}
			prefix = path.Clean(prefix)
		}
		// If the prefix is empty, ignore it.
		if prefix != "/" {
			results = query.NaiveFilter(results, query.FilterKeyPrefix{Prefix: prefix + "/"})
		}
	}

	// TODO: some kinds of filters can be done server-side
	for _, f := range q.Filters {
		results = query.NaiveFilter(results, f)
	}

	if useNaiveOrders {
		results = query.NaiveOrder(results, q.Orders...)
	}

	// this is not possible to do server-side with DynamoDB
	if q.Offset != 0 {
		results = query.NaiveOffset(results, q.Offset)
	}

	// TODO: this will usually over-read the last page...not terrible, but we can do better
	if q.Limit != 0 {
		results = query.NaiveLimit(results, q.Limit)
	}

	return results, nil
}

// TODO: implement batching using BatchWriteItem and BatchGetItem
func (d *ddbDatastore) Batch(ctx context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func (d *ddbDatastore) PutWithTTL(ctx context.Context, key ds.Key, value []byte, ttl time.Duration) error {
	return d.put(ctx, key, value, ttl)
}

func (d *ddbDatastore) SetTTL(ctx context.Context, key ds.Key, ttl time.Duration) error {
	expiration := time.Now().Add(ttl).Unix()
	expirationStr := strconv.Itoa(int(expiration))
	keys, err := d.makePutKeys(key)
	if err != nil {
		return err
	}

	req := &dynamodb.TransactWriteItemsInput{}

	for _, key := range keys {
		req.TransactItems = append(req.TransactItems, &dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				TableName:        aws.String(key.Table),
				Key:              key.Attrs,
				UpdateExpression: aws.String(fmt.Sprintf("SET %s = :e", attrNameExpiration)),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":e": {N: &expirationStr},
				},
				ConditionExpression:      aws.String("attribute_exists(#k)"),
				ExpressionAttributeNames: map[string]*string{"#k": aws.String(attrNameDSKey)},
			},
		})
	}

	log.Debug("updating TTL", zap.Reflect("item", req))

	_, err = d.ddbClient.TransactWriteItemsWithContext(ctx, req)
	if err != nil {
		// TODO: fix this to dig out the failure reasons
		if awsErr, ok := err.(awserr.Error); ok {
			// the conditional check failed which means there is no such item to set the TTL on
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return ds.ErrNotFound
			}
		}
		return fmt.Errorf("setting DynamoDB TTL: %w", err)
	}
	return nil
}
func (d *ddbDatastore) GetExpiration(ctx context.Context, key ds.Key) (time.Time, error) {
	item, err := d.getItem(ctx, key, []string{attrNameExpiration})
	if err != nil {
		return time.Time{}, err
	}
	return item.GetExpiration(), nil
}

// func (d *ddbDatastore) NewTransaction(ctx context.Context, readOnly bool) (ds.Txn, error) {
// 	return nil, nil
// }

type txn struct {
}

func (t *txn) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	panic("not implemented") // TODO: Implement
}

func (t *txn) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	panic("not implemented") // TODO: Implement
}

func (t *txn) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	panic("not implemented") // TODO: Implement
}

func (t *txn) Query(ctx context.Context, q query.Query) (query.Results, error) {
	panic("not implemented") // TODO: Implement
}

func (t *txn) Put(ctx context.Context, key datastore.Key, value []byte) error {
	panic("not implemented") // TODO: Implement
}

func (t *txn) Delete(ctx context.Context, key datastore.Key) error {
	panic("not implemented") // TODO: Implement
}

func (t *txn) Commit(ctx context.Context) error {
	panic("not implemented") // TODO: Implement
}

func (t *txn) Discard(ctx context.Context) {
	panic("not implemented") // TODO: Implement
}
