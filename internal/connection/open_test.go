package connection_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Open a connection in leader replication mode.
func TestOpenLeader(t *testing.T) {
	fs := sqlite3.RegisterVolatileFileSystem("volatile")
	defer sqlite3.UnregisterVolatileFileSystem(fs)

	uri := connection.EncodeURI("test.db", fs.Name(), "")
	methods := sqlite3.NoopReplicationMethods()
	conn, err := connection.OpenLeader(uri, methods)
	require.NoError(t, err)
	require.NotNil(t, conn)

	defer conn.Close()

	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	require.NoError(t, err)

	// The journal mode is set to WAL.
	dir, cleanup := newDir()
	defer cleanup()

	require.NoError(t, fs.Dump(dir))

	info, err := os.Stat(filepath.Join(dir, "test.db-wal"))
	require.NoError(t, err)
	assert.NotEqual(t, int64(0), info.Size())
}

// Open a connection in follower replication mode.
func TestOpenFollower(t *testing.T) {
	fs := sqlite3.RegisterVolatileFileSystem("volatile")
	defer sqlite3.UnregisterVolatileFileSystem(fs)

	uri := connection.EncodeURI("test.db", fs.Name(), "")
	conn, err := connection.OpenFollower(uri)
	require.NoError(t, err)
	require.NotNil(t, conn)

	_, err = conn.Exec("CREATE TABLE test (n INT)", nil)
	require.EqualError(t, err, "database is in follower replication mode: main")
}

// Possible failure modes.
func TestOpenFollower_Error(t *testing.T) {
	cases := []struct {
		title string
		dsn   string
		err   string
	}{
		{
			`non existing dsn`,
			"/non/existing/dsn.db",
			"open error",
		},
	}
	for _, c := range cases {
		conn, err := connection.OpenFollower(c.dsn)
		assert.Nil(t, conn)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), c.err)
	}
}

// Create a new temporary directory.
func newDir() (string, func()) {
	dir, err := ioutil.TempDir("", "dqlite-connection-test-")
	if err != nil {
		panic(fmt.Sprintf("could not create temporary dir: %v", err))
	}

	cleanup := func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(fmt.Sprintf("could not remove temporary dir: %v", err))
		}
	}

	return dir, cleanup
}
