package ddbds

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ipfs/go-datastore/query"
)

// queryIterator queries a DynamoDB table/index for a query.
// Queries cannot be performed in parallel, they are paginated sequentially.
type queryIterator struct {
	ddbClient *dynamodb.DynamoDB
	cancel    context.CancelFunc
	ddbQuery  *dynamodb.QueryInput

	keysOnly bool

	resultChan chan query.Result
}

func newQueryIterator(ddbClient *dynamodb.DynamoDB, ddbQuery *dynamodb.QueryInput, keysOnly bool) *queryIterator {
	qi := &queryIterator{
		ddbClient:  ddbClient,
		ddbQuery:   ddbQuery,
		keysOnly:   keysOnly,
		resultChan: make(chan query.Result),
	}
	return qi
}

func (q *queryIterator) start(ctx context.Context) {
	ctx, q.cancel = context.WithCancel(ctx)
	go func() {
		defer close(q.resultChan)
		defer q.cancel()

		//		fmt.Printf("starting query\n")
		// b, err := json.MarshalIndent(q.ddbQuery, "", "  ")
		// if err != nil {
		// 	panic(err)
		// }
		//		fmt.Println(string(b))

		err := q.ddbClient.QueryPagesWithContext(ctx, q.ddbQuery, func(page *dynamodb.QueryOutput, morePages bool) bool {
			//			fmt.Printf("got %d results in a page\n", len(page.Items))
			for _, itemMap := range page.Items {
				//				fmt.Printf("got an item\n")
				result := itemMapToQueryResult(itemMap, q.keysOnly)

				select {
				case <-ctx.Done():
					return false
				case q.resultChan <- result:
				}
			}
			return true
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case q.resultChan <- query.Result{Error: err}:
			}

		}
	}()
}

func (q *queryIterator) Next() (query.Result, bool) {
	res, ok := <-q.resultChan
	//	fmt.Printf("got %v %v\n", res, ok)
	return res, ok
}

func (q *queryIterator) Close() error {
	q.cancel()
	return nil
}
