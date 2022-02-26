package main

import (
	"net"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	ddbds "github.com/guseggert/go-ds-dynamodb"
	pb "github.com/guseggert/go-ds-grpc/proto"
	"github.com/guseggert/go-ds-grpc/server"
	golog "github.com/ipfs/go-log/v2"
	"google.golang.org/grpc"
)

func main() {
	cfg := aws.NewConfig().WithLogLevel(aws.LogDebugWithHTTPBody)
	sess := session.Must(session.NewSession(cfg))
	ddbClient := dynamodb.New(sess)
	ddbDS, err := ddbds.New(
		ddbClient,
		ddbds.WithKeyTransform(ddbds.KeyTransform{
			Prefix:           "/peers/metadata",
			Table:            "datastore-peers-metadata",
			PartitionKeyName: "PeerID",
			SortKeyName:      "MetadataKey",
		}),
		ddbds.WithKeyTransform(ddbds.KeyTransform{
			Prefix:           "/peers/addrs",
			Table:            "datastore-peers-addrs",
			PartitionKeyName: "PeerID",
		}),
		ddbds.WithKeyTransform(ddbds.KeyTransform{
			Prefix:           "/peers/keys",
			Table:            "datastore-peers-keys",
			PartitionKeyName: "PeerID",
			SortKeyName:      "KeyType",
		}),
		ddbds.WithKeyTransform(ddbds.KeyTransform{
			Prefix:           "/providers",
			Table:            "datastore-providers",
			PartitionKeyName: "ContentHash",
			SortKeyName:      "PeerID",
		}),
		ddbds.WithKeyTransform(ddbds.KeyTransform{
			Prefix:           "/",
			Table:            "datastore-all",
			PartitionKeyName: "DSRootKey",
		}),

		//  /provider-v1/queue/1645798483954494964/QmQy6xmJhrcC5QLboAcGFcAE1tC8CrwDVkrHdEYJkLscrQ

		// /pins/index/cidRindex/uEiBZlIQ5Bl8pYZ70EoDLuTK-UsVtmcWWa2XgERI58Ji77w/uODRhMWE3MGE1OGVlNDkzZTk3OTM5NjM2ZDU1ZWJkYzc
		// /ipns/AASAQAISEATV4MU4W5ALRC2R7UAQ4YJXEIFDZFX5LMNXCWABLWLKQPD4MSDD6

		// /pins/pin/84a1a70a58ee493e97939636d55ebdc7
	)
	if err != nil {
		panic(err)
	}

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
