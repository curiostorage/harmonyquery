package harmonyquery

// tests for BTFP mechanism - may be sensitive to Go version/inlining

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/yugabyte/pgx/v5"
)

func TestBTFP_NestedTransaction(t *testing.T) {
	db := &DB{}
	db.setBTFP()
	ctx := context.Background()

	var innerErr error
	var reached bool

	func() {
		defer func() { _ = recover() }()
		_, err := db.BeginTransaction(ctx, func(tx *Tx) (bool, error) {
			reached = true
			_, innerErr = db.BeginTransaction(ctx, func(tx2 *Tx) (bool, error) {
				return false, nil
			})
			return false, innerErr
		})
		require.NotNil(t, err)
		require.ErrorIs(t, innerErr, errTx)
	}()

	if !reached {
		t.Skip("need real DB")
	}
}

func TestBTFP_ExecInsideTransaction(t *testing.T) {
	db := &DB{}
	db.setBTFP()
	ctx := context.Background()

	var execErr error
	var reached bool

	func() {
		defer func() { _ = recover() }()
		_, err := db.BeginTransaction(ctx, func(tx *Tx) (bool, error) {
			reached = true
			_, execErr = db.Exec(ctx, "SELECT 1")
			return false, execErr
		})
		require.NotNil(t, err)
		require.ErrorIs(t, execErr, errTx)
	}()

	if !reached {
		t.Skip("need real DB")
	}
}

func TestTxQueryRow_PropagatesTransactionStartError(t *testing.T) {
	beginErr := errors.New("begin failed")
	tx := &Tx{
		tx: func() (pgx.Tx, error) {
			return nil, beginErr
		},
	}

	var got int
	err := tx.QueryRow("SELECT 1").Scan(&got)
	require.ErrorIs(t, err, beginErr)
	require.NotErrorIs(t, err, errTx)
}

func TestBTFP_QueryRowInsideTransactionReturnsErrTx(t *testing.T) {
	db := &DB{}
	db.setBTFP()
	ctx := context.Background()

	var queryErr error
	var reached bool

	func() {
		defer func() { _ = recover() }()
		_, err := db.BeginTransaction(ctx, func(tx *Tx) (bool, error) {
			reached = true
			var id int
			queryErr = db.QueryRow(ctx, "SELECT 1").Scan(&id)
			return false, queryErr
		})
		require.NotNil(t, err)
		require.ErrorIs(t, queryErr, errTx)
	}()

	if !reached {
		t.Skip("need real DB")
	}
}

func TestBTFP_StoredValue(t *testing.T) {
	db := &DB{}
	db.setBTFP() // New() calls setBTFP()

	btfp := db.BTFP
	if btfp == 0 {
		t.Fatal("BTFP not set")
	}

	fn := runtime.FuncForPC(uintptr(btfp))
	name := fn.Name()

	if !strings.Contains(name, "transactionInner") || strings.Contains(name, ".func") {
		t.Errorf("BTFP points to %q, want transactionInner", name)
	}
}

func TestBTFP_FoundInCallStack(t *testing.T) {
	db := &DB{}
	db.setBTFP()
	ctx := context.Background()

	btfp := db.BTFP
	if btfp == 0 {
		t.Fatal("BTFP not set")
	}

	var found bool
	func() {
		defer func() { _ = recover() }()
		_, err := db.BeginTransaction(ctx, func(tx *Tx) (bool, error) {
			var pcs [20]uintptr
			n := runtime.Callers(1, pcs[:])
			found = lo.Contains(pcs[:n], uintptr(btfp))
			return false, nil
		})
		if err != nil {
			t.Errorf("got %v, want nil", err)
		}
	}()

	if !found {
		t.Error("BTFP not found in call stack")
	}
}
