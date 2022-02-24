package ddbds

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
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

	logLevel = golog.LevelInfo
)

func init() {
	golog.SetAllLoggers(logLevel)
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

func startDDBLocal(ctx context.Context, ddbClient *dynamodb.DynamoDB) (func(), error) {
	cmd := exec.Command("docker", "run", "-d", "-p", "8000:8000", "amazon/dynamodb-local", "-jar", "DynamoDBLocal.jar", "-inMemory")
	buf := &bytes.Buffer{}
	cmd.Stdout = buf
	cmd.Stderr = buf
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("error running DynamoDB Local (%s), output:\n%s", err.Error(), buf)
	}

	ctrID := strings.TrimSpace(buf.String())

	cleanupFunc := func() {
		cmd := exec.Command("docker", "kill", ctrID)
		if err := cmd.Run(); err != nil {
			fmt.Printf("error killing %s: %s\n", ctrID, err)
		}
	}

	// wait for DynamoDB to respond
	for {
		select {
		case <-ctx.Done():
			cleanupFunc()
			return nil, ctx.Err()
		default:
		}

		_, err := ddbClient.ListTablesWithContext(ctx, &dynamodb.ListTablesInput{})
		if err == nil {
			break
		}
	}

	return cleanupFunc, err
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

type table struct {
	name         string
	partitionKey string
	sortKey      string
}

func setupTables(ddbClient *dynamodb.DynamoDB, tables ...table) {
	for _, table := range tables {
		tbl := table

		attrDefs := []*dynamodb.AttributeDefinition{
			{AttributeName: &tbl.partitionKey, AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
		}
		keySchema := []*dynamodb.KeySchemaElement{
			{AttributeName: &tbl.partitionKey, KeyType: aws.String(dynamodb.KeyTypeHash)},
		}
		if tbl.sortKey != "" {
			attrDefs = append(attrDefs, &dynamodb.AttributeDefinition{AttributeName: &tbl.sortKey, AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)})
			keySchema = append(keySchema, &dynamodb.KeySchemaElement{AttributeName: &tbl.sortKey, KeyType: aws.String(dynamodb.KeyTypeRange)})
		}

		req := &dynamodb.CreateTableInput{
			AttributeDefinitions: attrDefs,
			KeySchema:            keySchema,
			TableName:            &tbl.name,
			BillingMode:          aws.String(dynamodb.BillingModeProvisioned),
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1000),
				WriteCapacityUnits: aws.Int64(1000),
			},
		}

		log.Debugw("creating table", "Table", tbl.name, "Req", req)
		_, err := ddbClient.CreateTable(req)
		if err != nil {
			// idempotency
			if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceInUseException {
				return
			}
			panic(err)
		}
	}
}

func cleanupTables(ddbClient *dynamodb.DynamoDB, tables ...table) {
	for _, t := range tables {
		log.Debugw("deleting table", "Table", t.name)
		_, err := ddbClient.DeleteTable(&dynamodb.DeleteTableInput{TableName: &t.name})
		if err != nil {
			panic(err)
		}
	}
}

type testDeps struct {
	ddbClient *dynamodb.DynamoDB
	ddbDS     *ddbDatastore
}

func TestDDBDatastore_PutAndGet(t *testing.T) {
	// inst, _ := startLocalstack()
	// inst, stopLocalstack := startLocalstack()
	// t.Cleanup(stopLocalstack)
	// ddbEndpoint := inst.Endpoint(localstack.DynamoDB)

	ddbClient := newDDBClient(clientOpts{endpoint: "http://localhost:8000"})
	stopDDBLocal, err := startDDBLocal(context.Background(), ddbClient)
	t.Cleanup(stopDDBLocal)
	require.NoError(t, err)

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
			tbl := table{name: tableName, partitionKey: "key"}
			setupTables(ddbClient, tbl)
			defer cleanupTables(ddbClient, tbl)

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
	// inst, stopLocalstack := startLocalstack()
	// t.Cleanup(stopLocalstack)
	// ddbEndpoint := inst.Endpoint(localstack.DynamoDB)
	// ddbClient := newDDBClient(clientOpts{endpoint: ddbEndpoint})

	ddbClient := newDDBClient(clientOpts{endpoint: "http://localhost:8000"})
	stopDDBLocal, err := startDDBLocal(context.Background(), ddbClient)
	t.Cleanup(stopDDBLocal)
	require.NoError(t, err)

	orderByKey := []query.Order{&query.OrderByKey{}}

	makeEntries := func(keyPrefix string, n int) map[string]string {
		m := map[string]string{}
		for i := 0; i < n; i++ {
			m[fmt.Sprintf("%s%06d", keyPrefix, i)] = "val"
		}
		return m
	}

	makeExpResult := func(keyPrefix string, n int) []result {
		var results []result
		for i := 0; i < n; i++ {
			results = append(results, result{entry: query.Entry{
				Key:   fmt.Sprintf("%s%06d", keyPrefix, i),
				Value: []byte("val"),
			}})
		}
		return results
	}

	cases := []struct {
		name             string
		overrideLogLevel *golog.LogLevel
		tables           []table
		keyTransforms    []KeyTransform
		dsEntries        map[string]string
		beforeQuery      func(t *testing.T, deps *testDeps)
		queries          []query.Query

		expResults     [][]result
		expQueryErrors []string
		expPutError    string
	}{
		{
			name:             "100 scan entries",
			overrideLogLevel: &golog.LevelInfo,
			keyTransforms: []KeyTransform{{
				Table:            "table1",
				Prefix:           "/",
				PartitionKeyName: "table1PartitionKey",
			}},
			tables: []table{{
				name:         "table1",
				partitionKey: "table1PartitionKey",
			}},
			dsEntries:  makeEntries("/entry", 100),
			queries:    []query.Query{{Orders: orderByKey}},
			expResults: [][]result{makeExpResult("/entry", 100)},
		},
		{
			name:             "100 scan entries",
			overrideLogLevel: &golog.LevelInfo,
			keyTransforms: []KeyTransform{{
				Table:            "table1",
				Prefix:           "/",
				PartitionKeyName: "table1PartitionKey",
			}},
			tables: []table{{
				name:         "table1",
				partitionKey: "table1PartitionKey",
			}},
			dsEntries:  makeEntries("/entry", 100),
			queries:    []query.Query{{Orders: orderByKey}},
			expResults: [][]result{makeExpResult("/entry", 100)},
		},
		{
			name: "prefix query",
			keyTransforms: []KeyTransform{{
				Table:            "table1",
				Prefix:           "/foo",
				PartitionKeyName: "table1PartitionKey",
				SortKeyName:      "table1SortKey",
			}},
			tables: []table{{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			}},
			dsEntries: map[string]string{
				"/foo/bar/baz":     "qux",
				"/foo/bar/baz/qux": "quux",
			},
			queries: []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("qux")}},
				{entry: query.Entry{Key: "/foo/bar/baz/qux", Value: []byte("quux")}},
			}},
		},
		{
			name: "prefix query with multiple tables",
			keyTransforms: []KeyTransform{
				{
					Table:            "table1",
					Prefix:           "/foo1",
					PartitionKeyName: "table1PartitionKey",
					SortKeyName:      "table1SortKey",
				},
				{
					Table:            "table2",
					Prefix:           "/foo2",
					PartitionKeyName: "table2PartitionKey",
					SortKeyName:      "table2SortKey",
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
					sortKey:      "table1SortKey",
				},
				{
					name:         "table2",
					partitionKey: "table2PartitionKey",
					sortKey:      "table2SortKey",
				},
			},
			dsEntries: map[string]string{
				"/foo1/bar/baz": "quz1",
				"/foo2/bar/baz": "quz2",
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
				Table:            "table1",
				Prefix:           "/foo",
				PartitionKeyName: "table1PartitionKey",
				SortKeyName:      "table1SortKey",
			}},
			tables: []table{{
				name:         "table1",
				partitionKey: "table1PartitionKey",
				sortKey:      "table1SortKey",
			}},
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
					Table:            "table1",
					Prefix:           "/qux",
					PartitionKeyName: "table1PartitionKey",
					SortKeyName:      "table1SortKey",
				},
				{
					Table:            "table2",
					Prefix:           "/foo",
					PartitionKeyName: "table2PartitionKey",
					SortKeyName:      "table2SortKey",
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
					sortKey:      "table1SortKey",
				},
				{
					name:         "table2",
					partitionKey: "table2PartitionKey",
					sortKey:      "table2SortKey",
				},
			},
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
					Table:            "table1",
					Prefix:           "/foo/bar",
					PartitionKeyName: "table1PartitionKey",
					SortKeyName:      "table1SortKey",
				},
				{
					Table:            "table2",
					Prefix:           "/foo/bar",
					PartitionKeyName: "table2PartitionKey",
				},
				{
					Table:            "table3",
					Prefix:           "/foo",
					PartitionKeyName: "table3PartitionKey",
					SortKeyName:      "table3SortKey",
				},
				{
					Table:            "table4",
					Prefix:           "/foo",
					PartitionKeyName: "table4PartitionKey",
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
					sortKey:      "table1SortKey",
				},
				{
					name:         "table2",
					partitionKey: "table2PartitionKey",
				},
				{
					name:         "table3",
					partitionKey: "table3PartitionKey",
					sortKey:      "table3SortKey",
				},
				{
					name:         "table4",
					partitionKey: "table4PartitionKey",
				},
			},
			dsEntries: map[string]string{
				"/foo/bar/baz":      "bang",
				"/foo/baz":          "bang",
				"/foo/bar/baz/bang": "boom",

				// "/quux": "quuz",
			},
			queries: []query.Query{
				{Prefix: "/foo/bar", Orders: orderByKey},
				{Prefix: "/foo/bar/baz", Orders: orderByKey},
			},
			expResults: [][]result{
				{
					{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				},
				{
					{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				},
			},
		},
		{
			name: "all matching transforms are used for putting items, and the first matching transform is used for queries (different key transform order)",
			// this is the same as the previous test but the key transform order is different,
			// leading to different query results
			keyTransforms: []KeyTransform{
				{
					Table:            "table3",
					Prefix:           "/foo",
					PartitionKeyName: "table3PartitionKey",
					SortKeyName:      "table3SortKey",
				},
				{
					Table:            "table4",
					Prefix:           "/foo",
					PartitionKeyName: "table4PartitionKey",
				},
				{
					Table:            "table1",
					Prefix:           "/foo/bar",
					PartitionKeyName: "table1PartitionKey",
					SortKeyName:      "table1SortKey",
				},
				{
					Table:            "table2",
					Prefix:           "/foo/bar",
					PartitionKeyName: "table2PartitionKey",
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
					sortKey:      "table1SortKey",
				},
				{
					name:         "table2",
					partitionKey: "table2PartitionKey",
				},
				{
					name:         "table3",
					partitionKey: "table3PartitionKey",
					sortKey:      "table3SortKey",
				},
				{
					name:         "table4",
					partitionKey: "table4PartitionKey",
				},
			},
			dsEntries: map[string]string{
				"/foo/bar/baz":      "bang",
				"/foo/baz":          "bang",
				"/foo/bar/baz/bang": "boom",
			},
			queries: []query.Query{
				{Prefix: "/foo/bar", Orders: orderByKey},
				{Prefix: "/foo/bar/baz", Orders: orderByKey},
			},
			expResults: [][]result{
				// first query should use query on table3
				{
					{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
					{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
				},
				// second query should use scan on table4
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
					Table:            "table1",
					Prefix:           "/",
					PartitionKeyName: "table1PartitionKey",
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
				},
			},
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
					Table:            "table1",
					Prefix:           "/foo",
					PartitionKeyName: "table1PartitionKey",
					disableQueries:   true,
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
				},
			},
			dsEntries: map[string]string{
				"/foo/bar":          "baz",
				"/foo/bar/baz":      "bang",
				"/foo/bar/baz/bang": "boom",
			},
			queries: []query.Query{{Prefix: "/foo", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/bar", Value: []byte("baz")}},
				{entry: query.Entry{Key: "/foo/bar/baz", Value: []byte("bang")}},
				{entry: query.Entry{Key: "/foo/bar/baz/bang", Value: []byte("boom")}},
			}},
		},
		{
			name: "scans using the second scan table when the first one doesn't match",
			keyTransforms: []KeyTransform{
				{
					Table:            "scantable1",
					ScanParallelism:  1,
					Prefix:           "/foo/bar",
					PartitionKeyName: "table1PartitionKey",
					disableScans:     true,
				},
				{
					Table:            "scantable2",
					ScanParallelism:  1,
					Prefix:           "/foo/baz",
					PartitionKeyName: "table2PartitionKey",
				},
			},
			tables: []table{
				{
					name:         "scantable1",
					partitionKey: "table1PartitionKey",
				},
				{
					name:         "scantable2",
					partitionKey: "table2PartitionKey",
				},
			},
			dsEntries: map[string]string{"/foo/baz/bang": "boom"},
			queries:   []query.Query{{Prefix: "/foo/baz", Orders: orderByKey}},
			expResults: [][]result{{
				{entry: query.Entry{Key: "/foo/baz/bang", Value: []byte("boom")}},
			}},
		},
		{
			// this is internal functionality, but we want to make sure it works
			// because other tests rely on it
			name: "returns an error when scans are disabled on a scan table that matches a query",
			keyTransforms: []KeyTransform{
				{
					Table:            "table1",
					Prefix:           "/foo",
					PartitionKeyName: "table1PartitionKey",
					disableScans:     true,
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
				},
			},
			dsEntries:      map[string]string{"/foo/bar/baz": "bang"},
			queries:        []query.Query{{Prefix: "/foo", Orders: orderByKey}},
			expQueryErrors: []string{"scans on 'table1' are disabled"},
		},
		{
			// this is internal functionality, but we want to make sure it works
			// because other tests rely on it
			name: "returns an error when queries are disabled on an table that matches a query",
			keyTransforms: []KeyTransform{
				{
					Table:            "table1",
					Prefix:           "/foo",
					PartitionKeyName: "table1PartitionKey",
					SortKeyName:      "table1SortKey",
					disableQueries:   true,
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
					sortKey:      "table1SortKey",
				},
			},
			dsEntries:      map[string]string{"/foo/bar/baz": "bang"},
			queries:        []query.Query{{Prefix: "/foo/bar", Orders: orderByKey}},
			expQueryErrors: []string{"queries on 'table1' are disabled"},
		},
		{
			name: "returns an error if transform is missing the table's sort key",
			// Note that the inverse isn't true, if the table is missing the sort key then we have no idea
			// because DynamoDB will still happily accept the "sort" key as just another attribute, and
			// then the item will only appear in scans.
			//
			// There's not much we can do about this, perhaps describe the tables when the datastore
			// starts up and verify that they their schema matches the key transforms?
			keyTransforms: []KeyTransform{
				{
					Table:            "table1",
					Prefix:           "/foo",
					PartitionKeyName: "table1PartitionKey",
				},
			},
			tables: []table{
				{
					name:         "table1",
					partitionKey: "table1PartitionKey",
					sortKey:      "table1SortKey",
				},
			},
			dsEntries:   map[string]string{"/foo/bar/baz": "bang"},
			expPutError: "One of the required keys was not given a value",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()

			if c.overrideLogLevel != nil {
				golog.SetAllLoggers(*c.overrideLogLevel)
				defer golog.SetAllLoggers(logLevel)
			}

			setupTables(ddbClient, c.tables...)
			defer cleanupTables(ddbClient, c.tables...)

			ddbDS := &ddbDatastore{
				ddbClient:       ddbClient,
				table:           tableName,
				ScanParallelism: 10,
				keyTransforms:   c.keyTransforms,
			}
			deps := &testDeps{ddbClient: ddbClient, ddbDS: ddbDS}

			for k, v := range c.dsEntries {
				err := deps.ddbDS.Put(ctx, ds.NewKey(k), []byte(v))
				if c.expPutError != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), c.expPutError)
					return
				}
				require.NoError(t, err)
			}
			for i, q := range c.queries {
				// we run this in a func so we can defer closing the result stream
				func() {
					log.Debugw("test querying", "Query", q)
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
