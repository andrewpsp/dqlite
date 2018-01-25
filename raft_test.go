package dqlite_test

import (
	_ "github.com/CanonicalLtd/go-sqlite3"
)

/*
func TestAddressProvider(t *testing.T) {
	db := newDB(t)

	provider := dqlite.NewAddressProvider(db)
	_, err := provider.ServerAddr("1")
	assert.EqualError(t, err, "failed to fetch node address: no node node matching ID 1")

	err = db.Tx(func(tx *dqlite.Tx) error {
		_, err := tx.NodeAdd("1.2.3.4")
		return err
	})
	require.NoError(t, err)

	address, err := provider.ServerAddr("1")
	require.NoError(t, err)
	assert.Equal(t, raft.ServerAddress("1.2.3.4"), address)
}

func newDB(t *testing.T) *dqlite.Store {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to create in-memory test DB: %v", err)
	}

	db, err := dqlite.NewStore(sqlDB)
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}

	return db
}
*/
