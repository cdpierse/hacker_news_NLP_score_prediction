package download

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// ArticlePost is a struct
// for storing the results of
// our query
type ArticlePost struct {
	Title     string
	URL       string
	Score     int
	Timestamp string
	ID        int
	Type      string
}

// Fetch Does
func Fetch() []ArticlePost {

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, "bigquery-public-data:hacker_news")
	if err != nil {
		// TODO: Handle error.
	}
	fmt.Print(client)

	return []ArticlePost{}

}
