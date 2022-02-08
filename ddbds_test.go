package ddbds

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/elgohr/go-localstack"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/multiformats/go-multibase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tableName = "testtable"
	bucket    = "testbucket"
)

func startLocalstack() (*localstack.Instance, func()) {
	inst, err := localstack.NewInstance()
	if err != nil {
		panic(err)
	}
	err = inst.StartWithContext(
		context.Background(),
		localstack.S3,
		localstack.DynamoDB,
	)
	if err != nil {
		panic(err)
	}
	return inst, func() {
		err := inst.Stop()
		if err != nil {
			fmt.Printf("error shutting down localstack instance: %s", err.Error())
		}
	}
}

func forceSDKError(err error) func(*request.Request) {
	return func(r *request.Request) {
		r.Error = err
		r.Retryable = aws.Bool(false)
	}
}

type clientOpts struct {
	endpoint   string
	forceError error
}

func newDDBClient(opts clientOpts) *dynamodb.DynamoDB {
	cfg := &aws.Config{
		Credentials: credentials.NewStaticCredentials("a", "a", "a"),
		DisableSSL:  aws.Bool(true),
		Region:      aws.String(endpoints.UsEast1RegionID),
		Endpoint:    &opts.endpoint,
	}
	sess := session.Must(session.NewSession(cfg))
	if opts.forceError != nil {
		sess.Handlers.Send.PushFront(forceSDKError(opts.forceError))
	}
	return dynamodb.New(sess)
}

func newS3Client(opts clientOpts) *s3.S3 {
	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("a", "a", "a"),
		DisableSSL:       aws.Bool(true),
		Region:           aws.String(endpoints.UsEast1RegionID),
		Endpoint:         &opts.endpoint,
		S3ForcePathStyle: aws.Bool(true),
	}
	sess := session.Must(session.NewSession(cfg))
	if opts.forceError != nil {
		sess.Handlers.Send.PushFront(forceSDKError(opts.forceError))
	}
	return s3.New(sess)
}

type index struct {
	name         string
	partitionKey string
	sortKey      string
}

func setupTables(ddbClient *dynamodb.DynamoDB, indices ...index) {
	attrDefs := []*dynamodb.AttributeDefinition{{
		AttributeName: aws.String("Key"),
		AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
	}}

	// for each index, add the GSI definition
	// each index also needs its keys added to the attribute definitions of the main table
	var gsis []*dynamodb.GlobalSecondaryIndex
	for _, index := range indices {
		idx := index
		gsis = append(gsis, &dynamodb.GlobalSecondaryIndex{
			IndexName: &idx.name,
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: &idx.partitionKey,
					KeyType:       aws.String(dynamodb.KeyTypeHash),
				},
				{
					AttributeName: &idx.sortKey,
					KeyType:       aws.String(dynamodb.KeyTypeRange),
				},
			},
			Projection: &dynamodb.Projection{ProjectionType: aws.String(dynamodb.ProjectionTypeAll)},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1000),
				WriteCapacityUnits: aws.Int64(1000),
			},
		})
		attrDefs = append(attrDefs, &dynamodb.AttributeDefinition{
			AttributeName: &idx.partitionKey,
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		})
		attrDefs = append(attrDefs, &dynamodb.AttributeDefinition{
			AttributeName: &idx.sortKey,
			AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
		})
	}

	_, err := ddbClient.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: attrDefs,
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		TableName:   &tableName,
		BillingMode: aws.String(dynamodb.BillingModeProvisioned),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1000),
			WriteCapacityUnits: aws.Int64(1000),
		},
		GlobalSecondaryIndexes: gsis,
	})
	if err != nil {
		// idempotency
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceInUseException {
			return
		}
		panic(err)
	}
}

func cleanupTables(ddbClient *dynamodb.DynamoDB) {
	_, err := ddbClient.DeleteTable(&dynamodb.DeleteTableInput{TableName: &tableName})
	if err != nil {
		panic(err)
	}
}

type s3Opts struct {
	disableVersioning bool
}

func setupBucket(s3Client *s3.S3, opts s3Opts) {
	_, err := s3Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		panic(err)
	}
	if !opts.disableVersioning {
		_, err = s3Client.PutBucketVersioning(&s3.PutBucketVersioningInput{
			Bucket: &bucket,
			VersioningConfiguration: &s3.VersioningConfiguration{
				Status: aws.String(s3.BucketVersioningStatusEnabled),
			},
		})
		if err != nil {
			panic(err)
		}
	}
}

func cleanupBucket(s3Client *s3.S3) {
	var nextKeyMarker *string
	for {
		pageRes, err := s3Client.ListObjectVersions(&s3.ListObjectVersionsInput{
			Bucket:    &bucket,
			KeyMarker: nextKeyMarker,
		})
		if err != nil {
			awsErr, isAWSErr := err.(awserr.Error)
			isNSBErr := isAWSErr && awsErr.Code() == s3.ErrCodeNoSuchBucket
			if !isNSBErr {
				panic(err)
			}
		}
		for _, version := range pageRes.Versions {
			_, err := s3Client.DeleteObject(&s3.DeleteObjectInput{
				Bucket:    &bucket,
				Key:       version.Key,
				VersionId: version.VersionId,
			})
			if err != nil {
				panic(err)
			}
		}
		nextKeyMarker = pageRes.NextKeyMarker

		if nextKeyMarker == nil {
			break
		}
	}
}

func s3Key(s string) *string {
	b, err := multibase.Encode(multibase.Base64url, ds.NewKey(s).Bytes())
	if err != nil {
		panic(err)
	}
	bs := string(b)
	return &bs
}

type testDeps struct {
	ddbClient *dynamodb.DynamoDB
	ddbDS     *ddbDatastore
}

func TestDDBDatastore_PutAndGet(t *testing.T) {
	inst, stopLocalstack := startLocalstack()
	t.Cleanup(stopLocalstack)
	ddbEndpoint := inst.Endpoint(localstack.DynamoDB)

	ddbSizedValue := []byte("bar")

	cases := []struct {
		name      string
		putKey    string
		getKey    string
		value     []byte
		beforePut func(deps *testDeps)
		beforeGet func(deps *testDeps)

		expectPutErrContains string
		expectGetErrContains string
		expectGetValue       []byte
	}{
		{
			name:   "happy case",
			putKey: "foo",
			getKey: "foo",
			value:  ddbSizedValue,

			expectGetValue: ddbSizedValue,
		},
		{
			name:                 "returns ErrNotFound if the value is not in DDB",
			putKey:               "foo",
			getKey:               "foo1",
			expectGetErrContains: "datastore: key not found",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			ddbClient := newDDBClient(clientOpts{endpoint: ddbEndpoint})
			setupTables(ddbClient)
			defer cleanupTables(ddbClient)

			ddbDS := &ddbDatastore{ddbClient: ddbClient, table: tableName}
			deps := &testDeps{ddbClient: ddbClient, ddbDS: ddbDS}

			if c.beforePut != nil {
				c.beforePut(deps)
			}

			err := ddbDS.Put(ctx, ds.NewKey(c.putKey), c.value)
			if c.expectPutErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.expectPutErrContains)
				return
			}
			require.NoError(t, err)

			if c.beforeGet != nil {
				c.beforeGet(deps)
			}

			val, err := ddbDS.Get(ctx, ds.NewKey(c.getKey))
			if c.expectGetErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), c.expectGetErrContains)
				return
			}
			require.NoError(t, err)

			require.Equal(t, len(c.expectGetValue), len(val)) // try to prevent printing huge values
			require.Equal(t, c.expectGetValue, val)
		})
	}
}

func TestDDBDatastore_Query(t *testing.T) {
	inst, stopLocalstack := startLocalstack()
	t.Cleanup(stopLocalstack)
	ddbEndpoint := inst.Endpoint(localstack.DynamoDB)

	cases := []struct {
		name          string
		indices       []index
		keyTransforms []KeyTransform
		dsEntries     map[string]string
		beforeQuery   func(t *testing.T, deps *testDeps)
		queries       []query.Query

		expResults [][]query.Entry
		//		afterQuery  func(t *testing.T, results query.Results, err error)
	}{
		{
			name:      "single entry with no key transforms",
			dsEntries: map[string]string{"foo": "bar"},
			queries:   []query.Query{{}},
			expResults: [][]query.Entry{{
				{Key: "/foo", Value: []byte("bar")},
			}},
		},
		// {
		// 	name: "many entries",
		// 	beforeQuery: func(t *testing.T, deps *testDeps) {
		// 		for i := 0; i < 100; i++ {
		// 			key := fmt.Sprintf("%d", i)
		// 			err := deps.ddbDS.Put(context.Background(), ds.NewKey(key), []byte("value"))
		// 			require.NoError(t, err)
		// 		}
		// 	},
		// 	afterQuery: func(t *testing.T, results query.Results, err error) {
		// 		require.NoError(t, err)
		// 		entries, err := results.Rest()
		// 		require.NoError(t, err)
		// 		assert.Equal(t, 100, len(entries))
		// 		sort.Slice(entries, func(i, j int) bool {
		// 			iKey, err := strconv.Atoi(strings.TrimPrefix(entries[i].Key, "/"))
		// 			if err != nil {
		// 				panic(err)
		// 			}
		// 			jKey, err := strconv.Atoi(strings.TrimPrefix(entries[j].Key, "/"))
		// 			if err != nil {
		// 				panic(err)
		// 			}
		// 			return iKey < jKey
		// 		})
		// 		for i := 0; i < 100; i++ {
		// 			assert.Equal(t, fmt.Sprintf("/%d", i), entries[i].Key)
		// 			assert.Equal(t, []byte("value"), entries[i].Value)
		// 			assert.Equal(t, 5, entries[i].Size)
		// 		}
		// 	},
		// },
		{
			name: "prefix query",
			dsEntries: map[string]string{
				"/foo/bar":     "baz",
				"/foo/bar/baz": "quz",

				// these should not be in the results
				"/foobar": "baz",
				"/quux":   "quuz",
				"/":       "corge",
			},
			indices: []index{{
				name:         "index1",
				partitionKey: "index1PartitionKey",
				sortKey:      "index1SortKey",
			}},
			keyTransforms: []KeyTransform{{
				Index:            "index1",
				KeyPattern:       regexp.MustCompile(`^(/foo)/(.*?)$`),
				PrefixPattern:    regexp.MustCompile(`^/foo$`),
				PartitionKeyName: "index1PartitionKey",
				SortKeyName:      "index1SortKey",
			}},
			queries: []query.Query{{Prefix: "/foo"}},
			expResults: [][]query.Entry{{
				{Key: "/foo/bar", Value: []byte("baz")},
				{Key: "/foo/bar/baz", Value: []byte("quz")},
			}},
		},
		{
			name: "prefix query with multiple indices",
			keyTransforms: []KeyTransform{
				{
					Index:            "index1",
					KeyPattern:       regexp.MustCompile(`^(/foo/bar)/(.*?)$`),
					PrefixPattern:    regexp.MustCompile(`^/foo/bar$`),
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
				},
				{
					Index:            "index2",
					KeyPattern:       regexp.MustCompile(`^(/foo)/(.*?)$`),
					PrefixPattern:    regexp.MustCompile(`^/foo$`),
					PartitionKeyName: "index2PartitionKey",
					SortKeyName:      "index2SortKey",
				},
			},
			indices: []index{
				{
					name:         "index1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "index2",
					partitionKey: "index2PartitionKey",
					sortKey:      "index2SortKey",
				},
			},
			dsEntries: map[string]string{
				"/foo/bar/baz": "quz",
				"/foo/bar":     "baz",

				// these should not be in the results
				"/foobar": "baz",
				"/quux":   "quuz",
				"/":       "corge",
			},
			queries: []query.Query{{Prefix: "/foo"}},
			expResults: [][]query.Entry{{
				{Key: "/foo/bar", Value: []byte("baz")},
				{Key: "/foo/bar/baz", Value: []byte("quz")},
			}},
		},
		{
			name: "prefix query with dynamodb-optimized descending order",
			keyTransforms: []KeyTransform{{
				Index:            "index1",
				KeyPattern:       regexp.MustCompile(`^(/foo)/(.*?)$`),
				PrefixPattern:    regexp.MustCompile(`^/foo`),
				PartitionKeyName: "index1PartitionKey",
				SortKeyName:      "index1SortKey",
			}},
			indices: []index{{
				name:         "index1",
				partitionKey: "index1PartitionKey",
				sortKey:      "index1SortKey",
			}},
			dsEntries: map[string]string{
				"/foo/a": "bar",
				"/foo/c": "bar",
				"/foo/b": "bar",

				"/quux": "quuz",
			},
			queries: []query.Query{{
				Prefix: "/foo",
				Orders: []query.Order{&query.OrderByKeyDescending{}},
			}},
			expResults: [][]query.Entry{{
				{Key: "/foo/c", Value: []byte("bar")},
				{Key: "/foo/b", Value: []byte("bar")},
				{Key: "/foo/a", Value: []byte("bar")},
			}},
		},
		{
			name: "prefix query with naive filters and orders, and a non-matching first transform",
			keyTransforms: []KeyTransform{
				{
					Index:            "index1",
					KeyPattern:       regexp.MustCompile(`^(/qux)/(.*?)$`),
					PrefixPattern:    regexp.MustCompile(`^/qux`),
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
				},
				{
					Index:            "index2",
					KeyPattern:       regexp.MustCompile(`^(/foo)/(.*?)$`),
					PrefixPattern:    regexp.MustCompile(`^/foo`),
					PartitionKeyName: "index2PartitionKey",
					SortKeyName:      "index2SortKey",
				},
			},
			indices: []index{
				{
					name:         "index1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "index2",
					partitionKey: "index2PartitionKey",
					sortKey:      "index2SortKey",
				},
			},
			dsEntries: map[string]string{
				"/foo/a": "bar1",
				"/foo/c": "bar3",
				"/foo/b": "bar2",
				"/foo/e": "bar5",
				"/foo/d": "bar4",
				"/foo/f": "bar6",
				"/foo/g": "bar7",
				"/foo/h": "bar8",

				"/qux/quux": "quuz",
			},
			queries: []query.Query{{
				Prefix: "/foo",
				Orders: []query.Order{&query.OrderByKeyDescending{}, &query.OrderByValue{}},
				Limit:  2,
				Offset: 1,
				Filters: []query.Filter{&query.FilterKeyCompare{
					Op:  query.GreaterThan,
					Key: "/foo/c",
				}},
			}},
			expResults: [][]query.Entry{{
				{Key: "/foo/g", Value: []byte("bar7")},
				{Key: "/foo/f", Value: []byte("bar6")},
			}},
		},
		{
			name: "all matching transforms are used for putting items, and the first matching transform is used for queries",
			keyTransforms: []KeyTransform{
				{
					Index:            "index2",
					KeyPattern:       regexp.MustCompile(`^(/foo/bar)/(.*?)$`),
					PrefixPattern:    regexp.MustCompile(`^/foo/bar`),
					PartitionKeyName: "index2PartitionKey",
					SortKeyName:      "index2SortKey",
				},
				{
					Index:            "index1",
					KeyPattern:       regexp.MustCompile(`^(/foo)/(.*?)$`),
					PrefixPattern:    regexp.MustCompile(`^/foo`),
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
				},
			},
			indices: []index{
				{
					name:         "index1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "index2",
					partitionKey: "index2PartitionKey",
					sortKey:      "index2SortKey",
				},
			},
			dsEntries: map[string]string{
				"/foo":         "bar",
				"/foo/bar":     "baz",
				"/foo/baz":     "bang",
				"/foo/bar/baz": "bang",

				"/quux": "quuz",
			},
			queries: []query.Query{
				{Prefix: "/foo"},
				{Prefix: "/foo/bar"},
			},
			expResults: [][]query.Entry{
				{
					{Key: "/foo/bar", Value: []byte("baz")},
					{Key: "/foo/bar/baz", Value: []byte("bang")},
					{Key: "/foo/baz", Value: []byte("bang")},
				},
				{
					{Key: "/foo/bar/baz", Value: []byte("bang")},
				},
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			ddbClient := newDDBClient(clientOpts{endpoint: ddbEndpoint})
			setupTables(ddbClient, c.indices...)
			defer cleanupTables(ddbClient)

			ddbDS := &ddbDatastore{
				ddbClient:       ddbClient,
				table:           tableName,
				ScanParallelism: 10,
				keyTransforms:   c.keyTransforms,
			}
			deps := &testDeps{ddbClient: ddbClient, ddbDS: ddbDS}

			for k, v := range c.dsEntries {
				err := deps.ddbDS.Put(ctx, ds.NewKey(k), []byte(v))
				require.NoError(t, err)
			}

			for i, q := range c.queries {
				func() {
					res, err := ddbDS.Query(ctx, q)
					if res != nil {
						defer res.Close()
					}
					assert.NoError(t, err)
					results, err := res.Rest()
					assert.NoError(t, err)

					exp := c.expResults[i]
					assert.Equal(t, len(exp), len(results))
					for j, expResult := range exp {
						// TODO: compare the whole entry
						assert.Equal(t, expResult.Key, results[j].Key)
						assert.Equal(t, expResult.Value, results[j].Value)
					}
				}()
			}
		})
	}
}
