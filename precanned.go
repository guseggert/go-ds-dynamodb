package ddbds

func WithIPFSKeyTransforms() func(o *Options) {
	return func(o *Options) {
		WithProviderStoreKeyTransform()(o)
		WithIPNSKeyTransform()(o)
		WithPeersKeysTransform()(o)
		WithPeersMetadataTransform()(o)
	}
}

func WithProviderStoreKeyTransform() func(o *Options) {
	return func(o *Options) {
		o.KeyTransforms = append(o.KeyTransforms, KeyTransform{
			Prefix:           "/providers",
			QueryIndex:       "ProvidersQueryIndex",
			PartitionKeyName: "Providers-ContentHash",
			SortKeyName:      "Providers-PeerID",
		})
	}
}

func WithIPNSKeyTransform() func(o *Options) {
	return func(o *Options) {
		o.KeyTransforms = append(o.KeyTransforms, KeyTransform{
			Prefix: "/ipns",
		})
	}
}

func WithPeersKeysTransform() func(o *Options) {
	return func(o *Options) {
		o.KeyTransforms = append(o.KeyTransforms, KeyTransform{
			Prefix:           "/peers/keys",
			QueryIndex:       "PeersKeysQueryIndex",
			PartitionKeyName: "PeersKeys-PeerID",
			SortKeyName:      "PeersKeys-PeerKeys",
		})
	}
}

func WithPeersMetadataTransform() func(o *Options) {
	return func(o *Options) {
		o.KeyTransforms = append(o.KeyTransforms, KeyTransform{
			Prefix:           "/peers/metadata",
			QueryIndex:       "PeersMetadataQueryIndex",
			PartitionKeyName: "PeersMetadata-PeerID",
			SortKeyName:      "PeersMetadata-PeerMetadata",
		})
	}
}
