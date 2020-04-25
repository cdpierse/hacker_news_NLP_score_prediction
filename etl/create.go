package main

import (
	_"database/sql"
	_ "github.com/lib/pq"
)

const (
	HOST     = "localhost"
	PORT     = "5432"
	USERNAME = "postgres"
	PASSWORD = "password"
	DBNAME   = "hn_posts_server"
)
