package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://postgres:postgres@127.0.0.1:58432/pgkinetic?sslmode=disable"
	}

	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	var balance int64
	if err := conn.QueryRow(ctx, "select balance_cents from accounts where email = $1", "alice@example.com").Scan(&balance); err != nil {
		panic(err)
	}
	if balance != 1000 {
		panic(fmt.Sprintf("expected balance 1000, got %d", balance))
	}

	fmt.Println("go pgx smoke passed")
}
