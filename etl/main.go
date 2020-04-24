package main

import (
	"fmt"
	"github.com/cdpierse/hacker_news_NLP_score_prediction/etl/download"
	"log"
)

func main(){
	fmt.Println("Hello world")
	posts := download.GetPosts()
	log.Println(len(posts))


}