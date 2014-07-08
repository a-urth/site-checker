package main

import (
    _ "database/sql"
    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq"
    "log"
    "net/http"
    "fmt"
    "sync"
)

type Company struct {
    Id  string
    Url string
}

func check_site(url string) {
    fmt.Printf("IN routine %#v\n", url)
    resp, err := http.Head(url)
    if err != nil {
        log.Fatalln(err)
    }
    defer resp.Body.Close()
}

func main() {
    db, err := sqlx.Connect(
        "postgres",
        "user=postgres password=postgres port=5432 host=analytics.uaprom database=vacuum",
    )
    if err != nil {
        log.Fatalln(err)
    }
    comps := []Company{}
    err = db.Select(&comps, "SELECT id, company_site url FROM company ORDER BY id LIMIT 10")
    if err != nil {
        log.Fatalln(err)
    }

    for _, comp := range comps {
        sync.WaitGroup.Add(1)
        go check_site(comp.Url)
    }
}
