package ddbds

import (
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	ds "github.com/ipfs/go-datastore"
)

// Key describes the key schema for a DynamoDB item for a datastore entry.
// This is used for gets and puts on the table.
type Key struct {
	// Attrs are the key attributes. It contains either the partition key or both the partition key and the sort key.
	// It also contains any additional index keys.
	Attrs            map[string]*dynamodb.AttributeValue
	PartitionKeyName string
	SortKeyName      string

	disableScan  bool
	disableQuery bool
}

type KeyTransform struct {
	Prefix string

	// If you want to always scan the index instead of query,
	// then don't include a QueryIndex
	ScanIndex       string
	ScanParallelism int

	QueryIndex string

	PartitionKeyName string
	SortKeyName      string

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

	log.Debugw("trimmed query key", "QueryPrefixNS", queryPrefixNamespaces, "TransformPrefixNS", transformPrefixNamespaces, "Trimmed", trimmed)

	attrs := map[string]*dynamodb.AttributeValue{}

	// we query only if len(trimmed)==1, otherwise we scan
	if len(trimmed) == 1 {
		partitionKey := trimmed[0]
		attrs[p.PartitionKeyName] = &dynamodb.AttributeValue{S: &partitionKey}
	}

	return Key{
		Attrs:            attrs,
		PartitionKeyName: p.PartitionKeyName,
		SortKeyName:      p.SortKeyName,
		disableScan:      p.disableScans,
		disableQuery:     p.disableQueries,
	}, true
}

func (p *KeyTransform) PutKey(key ds.Key) (Key, bool) {
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
		// and the rest of the trimmed key, if it exists, is the sort key
		partitionKey := trimmed[0]
		attrs[p.PartitionKeyName] = &dynamodb.AttributeValue{S: &partitionKey}

		// after trimming prefix, if len >= 2, then set the sort key as well
		// this will populate both the query and scan indices with this entry
		if len(trimmed) >= 2 {
			// query index, add the sort key
			sortKeyNamespaces := trimmed[1:]
			sortKey := strings.Join(sortKeyNamespaces, "/")
			attrs[p.SortKeyName] = &dynamodb.AttributeValue{S: &sortKey}
		}
	}

	return Key{
		Attrs:            attrs,
		PartitionKeyName: p.PartitionKeyName,
		SortKeyName:      p.SortKeyName,
		disableScan:      p.disableScans,
		disableQuery:     p.disableQueries,
	}, true
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
