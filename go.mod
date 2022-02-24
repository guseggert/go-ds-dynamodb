module github.com/guseggert/go-ds-dynamodb

go 1.16

require (
	github.com/aws/aws-sdk-go v1.43.1
	github.com/elgohr/go-localstack v0.0.0-20220206105054-01f040ddc915
	github.com/google/uuid v1.3.0
	github.com/ipfs/go-datastore v0.5.1
	github.com/ipfs/go-log/v2 v2.5.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.19.1
)

replace github.com/elgohr/go-localstack v0.0.0-20220206105054-01f040ddc915 => ../go-localstack
