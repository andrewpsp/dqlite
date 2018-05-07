package connection_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot(t *testing.T) {
	fs := sqlite3.RegisterVolatileFileSystem("volatile")
	defer sqlite3.UnregisterVolatileFileSystem(fs)

	// Create a database with some content.
	uri := connection.EncodeURI("test.db", fs.Name(), "")
	methods := sqlite3.NoopReplicationMethods()
	conn, err := connection.OpenLeader(uri, methods)
	require.NoError(t, err)
	_, err = conn.Exec("CREATE TABLE foo (n INT); INSERT INTO foo VALUES(1)", nil)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	// Perform and restore the snapshot.
	database, wal, err := connection.Snapshot(fs, "test.db")
	require.NoError(t, err)
	require.NoError(t, connection.Restore(fs, "test.db", database, wal))

	// Check that the data actually matches our source database.
	dir, cleanup := newDir()
	defer cleanup()

	require.NoError(t, fs.Dump(dir))
	path := filepath.Join(dir, "test.db")

	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM foo", nil)
	require.NoError(t, err)
	defer rows.Close()

	require.Equal(t, true, rows.Next())
	var n int
	assert.NoError(t, rows.Scan(&n))
	assert.Equal(t, 1, n)
}
