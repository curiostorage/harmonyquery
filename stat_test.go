package harmonyquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yugabyte/pgx/v5/pgxpool"
)

// No DB needed: pgxpool opens connections lazily when MinConns is zero, so
// the pool reports its configuration and an empty state without dialing.
func TestStat(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:1/db?pool_max_conns=7")
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	defer pool.Close()

	db := &DB{pgx: pool}
	require.Equal(t, PoolStat{MaxConns: 7}, db.Stat())
}
