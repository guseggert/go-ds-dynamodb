package main

import (
	"net"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	ddbds "github.com/guseggert/go-ds-dynamodb"
	pb "github.com/guseggert/go-ds-grpc/proto"
	"github.com/guseggert/go-ds-grpc/server"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/mount"
	golog "github.com/ipfs/go-log/v2"
	"google.golang.org/grpc"
)

type ddbDS struct {
	ddbds.DDBDatastore
}

func main() {
	cfg := aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody)
	sess := session.Must(session.NewSession(cfg))
	ddbClient := dynamodb.New(sess)
	ddbDS := mount.New([]mount.Mount{
		{
			Prefix: ds.NewKey("/peers/metadata"),
			Datastore: ddbds.New(
				ddbClient,
				"datastore-peers-metadata",
				ddbds.WithPartitionkey("PeerID"),
				ddbds.WithSortKey("MetadataKey"),
			),
		},
		{
			Prefix: ds.NewKey("/peers/addrs"),
			Datastore: ddbds.New(
				ddbClient,
				"datastore-peers-addrs",
				ddbds.WithPartitionkey("PeerID"),
			),
		},
		{
			Prefix: ds.NewKey("/peers/keys"),
			Datastore: ddbds.New(
				ddbClient,
				"datastore-peers-keys",
				ddbds.WithPartitionkey("PeerID"),
				ddbds.WithSortKey("KeyType"),
			),
		},
		{
			Prefix: ds.NewKey("/providers"),
			Datastore: ddbds.New(
				ddbClient,
				"datastore-providers",
				ddbds.WithPartitionkey("ContentHash"),
				ddbds.WithSortKey("PeerID"),
			),
		},
		{
			Prefix: ds.NewKey("/"),
			Datastore: ddbds.New(
				ddbClient,
				"datastore-all",
				ddbds.WithPartitionkey("DSRootKey"),
			),
		},
	})

	golog.SetAllLoggers(golog.LevelDebug)

	grpcServer := grpc.NewServer()
	dsServer := server.New(ddbDS)

	pb.RegisterDatastoreServer(grpcServer, dsServer)
	l, err := net.Listen("tcp", "127.0.0.1:9982")
	if err != nil {
		panic(err)
	}
	err = grpcServer.Serve(l)
}
