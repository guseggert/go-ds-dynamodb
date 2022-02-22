package ddbds

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/elgohr/go-localstack"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	golog "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tableName = "testtable"
	bucket    = "testbucket"
)

func init() {
	//	golog.SetDebugLogging()
	golog.SetAllLoggers(golog.LevelInfo)
}

func startLocalstack() (*localstack.Instance, func()) {
	inst, err := localstack.NewInstance()
	if err != nil {
		panic(err)
	}
	err = inst.StartWithContext(
		context.Background(),
		localstack.DynamoDB,
	)
	if err != nil {
		panic(err)
	}
	return inst, func() {
		err := inst.Stop()
		if err != nil {
			log.Errorw("error shutting down localstack instance", "Error", err.Error())
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

type index struct {
	name         string
	partitionKey string
	sortKey      string
}

func setupTables(ddbClient *dynamodb.DynamoDB, indices ...index) {
	attrs := map[string]bool{attrNameKey: true}

	// for each index, add the GSI definition
	// each index also needs its keys added to the attribute definitions of the main table
	var gsis []*dynamodb.GlobalSecondaryIndex
	for _, index := range indices {
		idx := index

		gsi := &dynamodb.GlobalSecondaryIndex{

			IndexName: &idx.name,
			KeySchema: []*dynamodb.KeySchemaElement{{
				AttributeName: &idx.partitionKey,
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			}},
			Projection: &dynamodb.Projection{ProjectionType: aws.String(dynamodb.ProjectionTypeAll)},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1000),
				WriteCapacityUnits: aws.Int64(1000),
			},
		}

		attrs[idx.partitionKey] = true
		if idx.sortKey != "" {
			attrs[idx.sortKey] = true
			gsi.KeySchema = append(gsi.KeySchema, &dynamodb.KeySchemaElement{
				AttributeName: &idx.sortKey,
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			})
		}

		gsis = append(gsis, gsi)
	}

	var attrDefs []*dynamodb.AttributeDefinition
	for a := range attrs {
		attrName := a
		attrDefs = append(attrDefs, &dynamodb.AttributeDefinition{
			AttributeName: &attrName,
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

type testDeps struct {
	ddbClient *dynamodb.DynamoDB
	ddbDS     *ddbDatastore
}

func TestDDBDatastore_PutAndGet(t *testing.T) {
	inst, _ := startLocalstack()
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

type result struct {
	entry query.Entry
	err   string
}

func TestDDBDatastore_Query(t *testing.T) {
	inst, stopLocalstack := startLocalstack()
	t.Cleanup(stopLocalstack)
	ddbEndpoint := inst.Endpoint(localstack.DynamoDB)

	orderByKey := []query.Order{&query.OrderByKey{}}

	makeEntries := func(n int) map[string]string {
		m := map[string]string{}
		for i := 0; i < n; i++ {
			m[fmt.Sprintf("entry%06d", i)] = "val"
		}
		return m
	}

	makeExpResult := func(n int) []result {
		var results []result
		for i := 0; i < n; i++ {
			results = append(results, result{entry: query.Entry{
				Key:   fmt.Sprintf("/entry%06d", i),
				Value: []byte("val"),
			}})
		}
		return results
	}

	cases := []struct {
		name          string
		indices       []index
		keyTransforms []KeyTransform
		dsEntries     map[string]string
		beforeQuery   func(t *testing.T, deps *testDeps)
		queries       []query.Query

		disableTableScans bool

		expResults     [][]result
		expQueryErrors []string
	}{
		{
			name:      "single entry with no key transforms",
			dsEntries: map[string]string{"foo": "bar"},
			queries:   []query.Query{{}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo", Value: []byte("bar")}},
			}},
		},
		{
			name:       "100 entries",
			dsEntries:  makeEntries(100),
			queries:    []query.Query{{Orders: orderByKey}},
			expResults: [][]result{makeExpResult(100)},
		},
		{
			name: "prefix query",
			dsEntries: map[string]string{
				"/foo/bar/baz":     "qux",
				"/foo/bar/baz/qux": "quux",

				// these should not be in the results
				"/foo/bar":    "baz",
				"/foo/barbaz": "bang",
				"/foobar":     "baz",
				"/quux":       "quuz",
				"/":           "corge",
			},
			indices: []index{{
				name:         "queryindex1",
				partitionKey: "index1PartitionKey",
				sortKey:      "index1SortKey",
			}},
			disableTableScans: true,
			keyTransforms: []KeyTransform{{
				QueryIndex:       "queryindex1",
				Prefix:           "/foo",
				PartitionKeyName: "index1PartitionKey",
				SortKeyName:      "index1SortKey",
			}},
			queries: []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("qux")}},
				{entry: query.Entry{Key: "/foo/bar/baz/qux", Value: []byte("quux")}},
			}},
		},
		{
			name: "prefix query with multiple indices",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "index1",
					Prefix:           "/foo1",
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
				},
				{
					QueryIndex:       "index2",
					Prefix:           "/foo2",
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
			disableTableScans: true,
			dsEntries: map[string]string{
				"/foo1/bar/baz": "quz1",
				"/foo2/bar/baz": "quz2",

				// these should not be in the results
				"/foo1bar":  "baz",
				"/foo1/bar": "baz",
				"/foo2bar":  "baz",
				"/foo2/bar": "baz",
				"/quux":     "quuz",
				"/":         "corge",
			},
			queries: []query.Query{
				{Prefix: "/foo1/bar", Orders: orderByKey},
				{Prefix: "/foo2/bar", Orders: orderByKey},
			},
			expResults: [][]result{
				{{entry: query.Entry{Key: "/foo1/bar/baz", Value: []byte("quz1")}}},
				{{entry: query.Entry{Key: "/foo2/bar/baz", Value: []byte("quz2")}}},
			},
		},
		{
			// TODO: can this tell that sort happened at ddb layer?
			name: "prefix query with dynamodb-optimized descending order",
			keyTransforms: []KeyTransform{{
				QueryIndex:       "index1",
				Prefix:           "/foo",
				PartitionKeyName: "index1PartitionKey",
				SortKeyName:      "index1SortKey",
			}},
			indices: []index{{
				name:         "index1",
				partitionKey: "index1PartitionKey",
				sortKey:      "index1SortKey",
			}},
			disableTableScans: true,
			dsEntries: map[string]string{
				"/foo/z/a": "bar",
				"/foo/z/c": "bar",
				"/foo/z/b": "bar",

				"/foo/x/a": "quuz",
			},
			queries: []query.Query{{
				Prefix: "/foo/z",
				Orders: []query.Order{&query.OrderByKeyDescending{}},
			}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/z/c", Value: []byte("bar")}},
				{entry: query.Entry{Key: "/foo/z/b", Value: []byte("bar")}},
				{entry: query.Entry{Key: "/foo/z/a", Value: []byte("bar")}},
			}},
		},
		{
			name: "prefix query with naive filters and orders, and a non-matching first transform",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "index1",
					Prefix:           "/qux",
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
				},
				{
					QueryIndex:       "index2",
					Prefix:           "/foo",
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
			disableTableScans: true,
			dsEntries: map[string]string{
				"/foo/k/a": "bar1",
				"/foo/k/c": "bar3",
				"/foo/k/b": "bar2",
				"/foo/k/e": "bar5",
				"/foo/k/d": "bar4",
				"/foo/k/f": "bar6",
				"/foo/k/g": "bar7",
				"/foo/k/h": "bar8",

				"/qux/k/quux": "quuz",
			},
			queries: []query.Query{{
				Prefix: "/foo/k",
				Orders: []query.Order{&query.OrderByKeyDescending{}, &query.OrderByValue{}},
				Limit:  2,
				Offset: 1,
				Filters: []query.Filter{&query.FilterKeyCompare{
					Op:  query.GreaterThan,
					Key: "/foo/k/c",
				}},
			}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/k/g", Value: []byte("bar7")}},
				{entry: query.Entry{Key: "/foo/k/f", Value: []byte("bar6")}},
			}},
		},
		{
			name: "all matching transforms are used for putting items, and the first matching transform is used for queries",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "index2",
					Prefix:           "/foo/bar",
					PartitionKeyName: "index2PartitionKey",
					SortKeyName:      "index2SortKey",
				},
				{
					QueryIndex:       "index1",
					Prefix:           "/foo",
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
			disableTableScans: true,
			dsEntries: map[string]string{
				"/foo":              "bar",
				"/foo/bar":          "baz",
				"/foo/bar/baz":      "bang",
				"/foo/baz":          "bang",
				"/foo/bar/baz/bang": "boom",

				"/quux": "quuz",
			},
			queries: []query.Query{
				{Prefix: "/foo/bar", Orders: orderByKey},
				{Prefix: "/foo/bar/baz", Orders: orderByKey},
			},
			expResults: [][]result{
				{
					{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
					{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				},
				{
					{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				},
			},
		},
		{
			name: "all matching transforms are used for putting items, and the first matching transform is used for queries",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "queryindex1",
					ScanIndex:        "scanindex1",
					ScanParallelism:  1,
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
				},
				{
					QueryIndex:       "index2",
					Prefix:           "/foo/bar",
					PartitionKeyName: "index2PartitionKey",
					SortKeyName:      "index2SortKey",
					disableQueries:   true,
				},
			},
			indices: []index{
				{
					name:         "queryindex1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "scanindex1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "index2",
					partitionKey: "index2PartitionKey",
					sortKey:      "index2SortKey",
				},
			},
			disableTableScans: true,
			dsEntries: map[string]string{
				"/foo":              "bar",
				"/foo/bar":          "baz",
				"/foo/bar/baz":      "bang",
				"/foo/baz":          "bang",
				"/foo/bar/baz/bang": "boom",

				"/quux": "quuz",
			},
			queries: []query.Query{
				{Prefix: "/foo/bar", Orders: orderByKey},
				{Prefix: "/foo/bar/baz", Orders: orderByKey},
			},
			expResults: [][]result{
				// first query should use queryindex1
				{
					{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
					{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				},
				// second query should use scanindex1 and not index2
				{
					{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
					{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				},
			},
		},
		{
			name: "root prefix /",
			keyTransforms: []KeyTransform{
				{
					ScanIndex:        "index1",
					ScanParallelism:  1,
					Prefix:           "/",
					PartitionKeyName: "index1PartitionKey",
				},
			},
			indices: []index{
				{
					name:         "index1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries: map[string]string{
				"/foo":              "bar",
				"/foo/bar":          "baz",
				"/foo/bar/baz":      "bang",
				"/foo/baz":          "bang",
				"/foo/bar/baz/bang": "boom",
				"/quux":             "quuz",
			},
			queries: []query.Query{{Prefix: "/", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo", Value: []byte("bar")}},
				{entry: query.Entry{Key: "/foo/bar", Value: []byte("baz")}},
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
				{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				{entry: query.Entry{Key: "/foo/baz", Value: []byte("bang")}},
				{entry: query.Entry{Key: "/quux", Value: []byte("quuz")}},
			}},
		},
		{
			name: "scanning prefix",
			keyTransforms: []KeyTransform{
				{
					ScanIndex:        "index1",
					ScanParallelism:  1,
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
				},
			},
			indices: []index{
				{
					name:         "index1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries: map[string]string{
				"/foo":         "bar",
				"/foo/bar":     "baz",
				"/foo/bar/baz": "bang",

				"/foobar": "baz",
				"/quux":   "quuz",
			},
			queries: []query.Query{{Prefix: "/foo", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar", Value: []byte("baz")}},
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
			}},
		},
		{
			name: "uses the scan index when there's both a scan and query index and the entry is only one level deep",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "queryindex1",
					ScanIndex:        "scanindex1",
					ScanParallelism:  1,
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
					disableQueries:   true,
				},
			},
			indices: []index{
				{
					name:         "index1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "scanindex1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/bar": "baz"},
			queries:           []query.Query{{Prefix: "/foo", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar", Value: []byte("baz")}},
			}},
		},
		{
			name: "uses the scan index when there's both a scan and query index and the entry is >2 levels deep",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "queryindex1",
					ScanIndex:        "scanindex1",
					ScanParallelism:  1,
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
					disableQueries:   true,
				},
			},
			indices: []index{
				{
					name:         "index1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "scanindex1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/bar/baz/bang": "boom"},
			queries:           []query.Query{{Prefix: "/foo", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
			}},
		},
		{
			name: "uses the query index when there's both a scan and query index and the entry is one level deep",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "queryindex1",
					ScanIndex:        "scanindex1",
					ScanParallelism:  1,
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
					SortKeyName:      "index1SortKey",
					disableScans:     true,
				},
			},
			indices: []index{
				{
					name:         "queryindex1",
					partitionKey: "index1PartitionKey",
					sortKey:      "index1SortKey",
				},
				{
					name:         "scanindex1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/bar/baz": "bang"},
			queries:           []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
			}},
		},
		{
			name: "performs an index scan when there's no query index and prefix matches two levels deep",
			keyTransforms: []KeyTransform{
				{
					ScanIndex:        "scanindex1",
					ScanParallelism:  1,
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
				},
			},
			indices: []index{
				{
					name:         "scanindex1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/bar/baz": "bang"},
			queries:           []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
			}},
		},
		{
			name: "scans using the second scan index when the first one doesn't match",
			keyTransforms: []KeyTransform{
				{
					ScanIndex:        "scanindex1",
					ScanParallelism:  1,
					Prefix:           "/foo/bar",
					PartitionKeyName: "index1PartitionKey",
					disableScans:     true,
				},
				{
					ScanIndex:        "scanindex2",
					ScanParallelism:  1,
					Prefix:           "/foo/baz",
					PartitionKeyName: "index2PartitionKey",
				},
			},
			indices: []index{
				{
					name:         "scanindex1",
					partitionKey: "index1PartitionKey",
				},
				{
					name:         "scanindex2",
					partitionKey: "index2PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/baz/bang": "boom"},
			queries:           []query.Query{{Prefix: "/foo/baz", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/baz/bang", Value: []byte("boom")}},
			}},
		},
		{
			// this is internal functionality, but we want to make sure it works
			// because other tests rely on it
			name: "returns an error when scans are disabled on an index that matches a query",
			keyTransforms: []KeyTransform{
				{
					ScanIndex:        "scanindex1",
					ScanParallelism:  1,
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
					disableScans:     true,
				},
			},
			indices: []index{
				{
					name:         "scanindex1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/bar/baz": "bang"},
			queries:           []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expQueryErrors:    []string{"scans on 'scanindex1' are disabled"},
		},
		{
			// this is internal functionality, but we want to make sure it works
			// because other tests rely on it
			name: "returns an error when queries are disabled on an index that matches a query",
			keyTransforms: []KeyTransform{
				{
					QueryIndex:       "queryindex1",
					Prefix:           "/foo",
					PartitionKeyName: "index1PartitionKey",
					disableQueries:   true,
				},
			},
			indices: []index{
				{
					name:         "queryindex1",
					partitionKey: "index1PartitionKey",
				},
			},
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/bar/baz": "bang"},
			queries:           []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expQueryErrors:    []string{"queries on 'queryindex1' are disabled"},
		},
		{
			// this is internal functionality, but we want to make sure it works
			// because other tests rely on it
			name:              "returns an error when table scans are disabled and a query results in a table scan",
			disableTableScans: true,
			dsEntries:         map[string]string{"/foo/bar/baz": "bang"},
			queries:           []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expQueryErrors:    []string{"table scans are disabled"},
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
				ddbClient:         ddbClient,
				table:             tableName,
				ScanParallelism:   10,
				keyTransforms:     c.keyTransforms,
				disableTableScans: c.disableTableScans,
			}
			deps := &testDeps{ddbClient: ddbClient, ddbDS: ddbDS}

			for k, v := range c.dsEntries {
				err := deps.ddbDS.Put(ctx, ds.NewKey(k), []byte(v))
				require.NoError(t, err)
			}
			for i, q := range c.queries {
				// we run this in a func so we can defer closing the result stream
				func() {
					res, err := ddbDS.Query(ctx, q)
					if res != nil {
						defer res.Close()
					}
					if c.expQueryErrors != nil && c.expQueryErrors[i] != "" {
						require.Error(t, err)
						require.Contains(t, err.Error(), c.expQueryErrors[i])
						return
					}
					require.NoError(t, err)

					// collect the results
					// we don't do this with Rest() since it short-circuits on errors
					var results []query.Result
					for {
						result, ok := res.NextSync()
						if !ok {
							log.Debugw("not ok result", "Result", result)
							break
						}
						results = append(results, result)
						log.Debugw("test got query result", "Result", result)
					}

					// assert the results
					assert.Equal(t, len(c.expResults[i]), len(results))
					for resultIdx, exp := range c.expResults[i] {
						// TODO: compare the whole entry
						result := results[resultIdx]
						if exp.err != "" {
							require.Error(t, result.Error)
							require.Contains(t, result.Error.Error(), exp.err)
							continue
						}
						require.NoError(t, result.Error)

						assert.Equal(t, exp.entry.Key, result.Key)
						assert.Equal(t, exp.entry.Value, result.Value)
					}
				}()
			}
		})
	}
}
