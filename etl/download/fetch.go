package download

import (
	"cloud.google.com/go/bigquery"
	"context"
	"google.golang.org/api/iterator"
	"log"
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

func _setUpClientContext() (*bigquery.Client, context.Context) {

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "hacker-news-analysis-275115")
	if err != nil {
		log.Fatalf("Error setting up new client %v", err)
	}
	return client, ctx
}

func _query(context context.Context, client *bigquery.Client) {

}

// Fetch Does
func Fetch() []ArticlePost {

	client, ctx := _setUpClientContext()

	q := client.Query(`Select title, url, score, timestamp, id, type
	FROM ` + "`bigquery-public-data.hacker_news.full`" + `
	WHERE title != ""
	`)

	it, err := q.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		log.Print(values[1])

		break
	}

	return []ArticlePost{}

}
