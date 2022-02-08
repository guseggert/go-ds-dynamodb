package ddbds

import (
	"path"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
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
}

type KeyTransform struct {
	Index string

	// Pattern used for extracting the partition and sort keys from a key.
	// If the key does not match this pattern, then this transform is skipped.
	// This pattern should have exactly two groups matching the partition key
	// and sort key respectively.
	//
	// Example:
	//
	//   ^/providers/(.*?)/(.*)$
	//
	// With a key /providers/foo/bar:
	//
	// - Partition key: /foo
	// - Sort Key: bar
	KeyPattern *regexp.Regexp

	// Pattern used for determining if a given query prefix matches this transform.
	// If the query prefix does not match the pattern, then this transform is not used for the query.
	// If the query prefix does match the pattern, then the entire prefix is used as the partition key for the query.
	//
	// Example:
	//
	//   ^/providers/[^/]*$
	//
	// With a key /providers/foo/bar/baz:

	PrefixPattern *regexp.Regexp

	PartitionKeyName string
	SortKeyName      string
}

func (p *KeyTransform) QueryKey(prefix string) (Key, bool) {
	if p.PrefixPattern == nil || !p.PrefixPattern.MatchString(prefix) {
		return Key{}, false
	}
	// Copied from query.NaiveQueryApply:
	//
	// Clean the prefix as a key and append / so a prefix of /bar
	// only finds /bar/baz, not /barbaz.
	if len(prefix) == 0 {
		prefix = "/"
	} else {
		if prefix[0] != '/' {
			prefix = "/" + prefix
		}
		prefix = path.Clean(prefix)
	}

	return Key{
		Attrs: map[string]*dynamodb.AttributeValue{
			p.PartitionKeyName: {S: &prefix},
		},
		PartitionKeyName: p.PartitionKeyName,
		SortKeyName:      p.SortKeyName,
	}, true
}

func (p *KeyTransform) Key(key ds.Key) (Key, bool) {
	var attrs map[string]*dynamodb.AttributeValue
	if p.KeyPattern == nil {
		attrs = map[string]*dynamodb.AttributeValue{
			p.PartitionKeyName: {S: aws.String(key.String())},
		}
	} else {
		match := p.KeyPattern.FindStringSubmatch(key.String())
		if len(match) < 2 {
			return Key{}, false
		}
		attrs = map[string]*dynamodb.AttributeValue{
			p.PartitionKeyName: {S: &match[1]},
		}
		if len(match) == 3 {
			attrs[p.SortKeyName] = &dynamodb.AttributeValue{S: &match[2]}
		}
	}
	// the pattern should match either one or two groups
	// the first is the partition key
	// the second is the optional sort key
	return Key{
		Attrs:            attrs,
		PartitionKeyName: p.PartitionKeyName,
		SortKeyName:      p.SortKeyName,
	}, true
}
