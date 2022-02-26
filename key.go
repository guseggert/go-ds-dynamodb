package ddbds

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	ds "github.com/ipfs/go-datastore"
)

// Key describes the key schema for a DynamoDB item for a datastore entry.
// This is used for gets and puts on the table.
type Key struct {
	Table string

	// Attrs are the key attributes. It contains either the partition key or both the partition key and the sort key.
	// It also contains any additional index keys.
	Attrs            map[string]*dynamodb.AttributeValue
	PartitionKeyName string
	SortKeyName      string
	ShouldQuery      bool

	disableScan  bool
	disableQuery bool
}

type KeyTransform struct {
	Prefix string

	UseStronglyConsistentReads bool

	Table string

	ScanParallelism int

	PartitionKeyName string
	// Sort key is optional, if unspecified then all matching queries will result in scans.
	SortKeyName string

	// used for testing
	disableScans   bool
	disableQueries bool
}

func (p *KeyTransform) namespaces(k ds.Key) []string {
	namespaces := k.Namespaces()

	// for the root key "/", we want an empty/nil slice, not [""]
	if len(namespaces) == 1 && namespaces[0] == "" {
		namespaces = nil
	}
	return namespaces
}

func (p *KeyTransform) QueryKey(queryPrefix string) (Key, bool) {
	queryPrefixKey := ds.NewKey(queryPrefix)
	transformPrefixKey := ds.NewKey(p.Prefix)

	queryPrefixNamespaces := p.namespaces(queryPrefixKey)
	transformPrefixNamespaces := p.namespaces(transformPrefixKey)

	trimmed, isPrefix := p.trimPrefix(queryPrefixNamespaces, transformPrefixNamespaces)
	if !isPrefix {
		return Key{}, false
	}

	// note that if QueryPrefix="" (i.e. no prefix was specified), then it's effectively
	// the same as QueryPrefix="/"

	// // if TransformPrefix=/foo/bar and QueryPrefix=/foo/bar, there is not a case where the
	// // transform prefix is usable for that scenario
	// // but TransformPrefix=/ and QueryPrefix=/ is a special case
	// if len(trimmed) == 0 {
	// 	return Key{}, false
	// }

	log.Debugw("trimmed query key", "QueryPrefixNS", queryPrefixNamespaces, "TransformPrefixNS", transformPrefixNamespaces, "Trimmed", trimmed)

	key := Key{
		Attrs:            map[string]*dynamodb.AttributeValue{},
		PartitionKeyName: p.PartitionKeyName,
		SortKeyName:      p.SortKeyName,
		disableScan:      p.disableScans,
		disableQuery:     p.disableQueries,
	}

	// we query only if len(trimmed)==1, otherwise we scan
	if len(trimmed) == 1 {
		partitionKey := trimmed[0]
		key.Attrs[p.PartitionKeyName] = &dynamodb.AttributeValue{S: &partitionKey}
		key.ShouldQuery = true
	}

	return key, true
}

func (p *KeyTransform) GetKey(key ds.Key) (Key, bool) {
	keyNamespaces := p.namespaces(key)
	prefixNamespaces := p.namespaces(ds.NewKey(p.Prefix))

	trimmed, isPrefix := p.trimPrefix(keyNamespaces, prefixNamespaces)
	if !isPrefix || len(trimmed) == 0 {
		return Key{}, false
	}

	log.Debugw("trimmed put key", "TrimmedKey", trimmed)

	attrs := map[string]*dynamodb.AttributeValue{}

	if p.SortKeyName == "" {
		partitionKey := strings.Join(trimmed, "/")
		attrs[p.PartitionKeyName] = &dynamodb.AttributeValue{S: &partitionKey}
	} else {
		// if there's a sort key, then the first element of the trimmed key is the partition key
		// and the rest of the trimmed key is the sort key

		// there need to be >= 2 elements in the trimmed key so we can derive a sort key
		// otherwise we can't write to this table
		if len(trimmed) < 2 {
			return Key{}, false
		}

		partitionKey := trimmed[0]
		attrs[p.PartitionKeyName] = &dynamodb.AttributeValue{S: &partitionKey}

		sortKeyNamespaces := trimmed[1:]
		sortKey := strings.Join(sortKeyNamespaces, "/")
		attrs[p.SortKeyName] = &dynamodb.AttributeValue{S: &sortKey}
	}

	return Key{
		Table:            p.Table,
		Attrs:            attrs,
		PartitionKeyName: p.PartitionKeyName,
		SortKeyName:      p.SortKeyName,
		disableScan:      p.disableScans,
		disableQuery:     p.disableQueries,
	}, true
}

func (p *KeyTransform) PutKey(key ds.Key) (Key, bool) {
	// the put key is the same as the get key, except we add the additional Key attribute (the full datastore key)
	// this isn't strictly necessary since we can reconstruct the key, but makes digging through DynamoDB items easier
	k, ok := p.GetKey(key)
	if !ok {
		return k, false
	}
	k.Attrs[attrNameDSKey] = &dynamodb.AttributeValue{S: aws.String(key.String())}
	return k, true
}

func (p *KeyTransform) trimPrefix(ss []string, prefix []string) ([]string, bool) {
	if len(ss) < len(prefix) {
		return nil, false
	}

	var (
		i int
		s string
	)

	for i, s = range prefix {
		if ss[i] != s {
			return nil, false
		}
	}

	trimmed := append([]string(nil), ss[len(prefix):]...)

	return trimmed, true
}
