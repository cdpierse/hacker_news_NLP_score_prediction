package main

import (
	"database/sql"
	_ "database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

// Config params for db
const (
	HOST     = "localhost"
	PORT     = 54320
	USERNAME = "postgres"
	PASSWORD = "password"
	DBNAME   = "hn_db"
)

// Insert starts the process of 
// inserting a slice of post structs 
// into the db. 
func Insert(posts []Post) {
	_insertPosts(posts)

}

func _insertPosts(posts []Post) {
	db := _connect()
	defer db.Close()
	for i, p := range posts {
		if i%100000 == 0 {
			log.Printf("%v rows added so far", i)
		}
		if !_RowAlreadyExists(p.ID, db) {
			_insertPostIntoDB(p, db)
		} else {

		}

	}

}
func _insertPostIntoDB(p Post, db *sql.DB) {
	insertStatment := `INSERT INTO posts (title, url, score, timestamp, id, type)
	VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := db.Exec(insertStatment, p.Title, p.URL,
		p.Score, p.Timestamp, p.ID, p.Type)
	if err != nil {
		log.Panic("Error inserting post into DB with error: ", err)
	}

}

func _RowAlreadyExists(id int, db *sql.DB) bool {
	var result string
	insertStatement := `select exists(select 1 from posts where id= $1)`
	err := db.QueryRow(insertStatement, id).Scan(&result)
	if err != nil {
		log.Panic("Error checking if id exists with error: ", err)
	}
	if result == "true" {
		return true
	}

	return false

}

func _connect() *sql.DB {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		HOST, PORT, USERNAME, PASSWORD, DBNAME)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}

	_createTable(db)

	return db

}

func _createTable(db *sql.DB) {
	tableName := "posts"
	tables := _getTables(db)
	postsTableExists := false
	for _, value := range tables {
		if value == tableName {
			postsTableExists = true
		}
	}
	if postsTableExists {
		log.Println("Table " + tableName + " already created")

	} else {
		stmt, err := db.Prepare(`CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			id SERIAL PRIMARY KEY,
			title TEXT,
			url TEXT,
			type TEXT,
			score INT,
			timestamp TIMESTAMP);`)

		if err != nil {
			log.Fatal(err)
		}

		defer stmt.Close()

		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}

		log.Println("Table " + tableName + " succesfully created")

	}

}

func _getTables(db *sql.DB) []string {
	var tableName string
	var tables []string

	query := `SELECT table_name
	FROM information_schema.tables
	WHERE table_schema = 'public'
	ORDER BY table_name;`

	rows, err := db.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&tableName)
		tables = append(tables, tableName)
		if err != nil {
			log.Fatal(err)
		}
	}

	return tables

}
