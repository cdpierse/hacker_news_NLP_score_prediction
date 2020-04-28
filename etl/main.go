package main

import (
	"log"
)

func main() {
	log.Println("Starting Posts Fetching, DB Creation and Post Insertion")
	posts := GetPosts()
	Insert(posts)
}
