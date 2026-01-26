# HarmonyQuery

A Postgres/Yugabyte adapter in harmony with busy developers. The top few dev mistakes with DB

## Features

- Rolling to secondary database servers on connection failure
- Convenience features for Go + SQL
- Prevention of SQL injection vulnerabilities
- Prevents non-transaction calls in a transaction (really hard to debug)
- Requires context, so cancel at your lowest layer is guaranteed plumbed.
- Includes your migrations and test isolation so your dev's db instance can run tests and have a main instance
- Discourages dangling transactions & dangling result cursors. 
- Conveniences:  QueryRow().Scan(&a) and: var a []resultType; db.Select(&a, "query")
- Monitors behavior via Prometheus stats and logging of errors
- Entirely within the Go1 promise. No unsafe. 

## Notes
- Only DB can be shared across threads.

## Installation

```bash
go get github.com/curiostorage/harmonyquery
```

## Usage

```go
package main

import (
    "context"
    "github.com/curiostorage/harmonyquery"
)

//go:embed sql
var upgradeFS embed.FS

//go:embed downgrade
var downgradeFS embed.FS

func main() {
    db, err := harmonyquery.NewFromConfig(harmonyquery.Config{
        Hosts:       []string{"localhost"},
        Username:    "yugabyte",
        Password:    "yugabyte",
        Database:    "yugabyte",
        Port:        "5433",
        LoadBalance: false,
        Schema:      "BobsFishery",
        UpgradeFS:   &upgradeFS,
        DowngradeF:  &downgradeFS,
        SSLMode:     "require",
    })
    if err != nil {
        panic(err)
    }

    // Execute queries
    ctx := context.Background()
    
    // Insert/Update/Delete
    count, err := db.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", "Alice")
    
    // Select into struct slice
    var users []struct {
        ID   int
        Name string
    }
    err = db.Select(ctx, &users, "SELECT id, name FROM users")
    
    // Query single row
    var name string
    err = db.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", 1).Scan(&name)
    
    // Transactions
    committed, err := db.BeginTransaction(ctx, func(tx *harmonyquery.Tx) (bool, error) {
        _, err := tx.Exec("UPDATE users SET name = $1 WHERE id = $2", "Bob", 1)
        if err != nil {
            return false, err
        }
        return true, nil // commit
    })
}
```

## Schema Migrations

SQL migrations are embedded in the package and automatically applied on connection.

Place migration files in the `sql/` folder with naming convention: `YYYYMMDD-description.sql`

Rules:
- CREATE TABLE should NOT have a schema (managed automatically)
- Never change shipped SQL files - create new files for corrections
- All migrations run once in order

## License

MIT License - see [LICENSE](LICENSE) for details.

