package main

import (
	"context"
	"log"
	"time"
	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// Post is a struct for storing
// the results of our query to the hacker news
// table in bigquery
type Post struct {
	Title     string    `bigquery:"title"`
	URL       string    `bigquery:"url"`
	Score     int       `bigquery:"score"`
	Timestamp time.Time `bigquery:"timestamp"`
	ID        int       `bigquery:"id"`
	Type      string    `bigquery:"type"`
}

// GetPosts returns a slice of
// posts made up of Post structs returned
// from our bigquery query
func GetPosts() []Post {
	log.Println("Starting query to BigQuery")
	client, ctx := _setUpClientContext()
	log.Println("Set Up BigQuery Client")
	it := _query(ctx, client)
	log.Println("Got Query")
	res := _createPosts(it)
	log.Println("Successfully fetched and created posts")
	return res
}

func _setUpClientContext() (*bigquery.Client, context.Context) {

	ctx := context.Background()
	// Enter your own project ID here
	projectID := "hacker-news-analysis-275115"

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Error setting up new client %v", err)
	}
	return client, ctx
}

func _query(ctx context.Context, client *bigquery.Client) *bigquery.RowIterator {
	q := client.Query(
		`Select COALESCE(title,"None"), COALESCE(url,"None"), score, timestamp, id, type
		FROM ` + "`bigquery-public-data.hacker_news.full`" + `
		WHERE title != ""
					`)

	it, err := q.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return it

}

func _createPosts(it *bigquery.RowIterator) []Post {
	var posts []Post
	rowsAdded := 0

	for {
		var p Post
		err := it.Next(&p)
		posts = append(posts, p)
		rowsAdded++
		if rowsAdded%10000 == 0 {
			log.Printf("Rows added so far is %v", rowsAdded)
		}
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Encountered error with iterator: %v", err)
		}
	}
	return posts

}
