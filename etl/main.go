package main

import (
	"fmt"
	"log"
)

func main(){
	fmt.Println("Hello world")
	posts := GetPosts()
	log.Println(len(posts))


}