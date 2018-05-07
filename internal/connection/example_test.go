package connection_test

import (
	"log"

	"github.com/CanonicalLtd/dqlite/internal/connection"
	"github.com/CanonicalLtd/go-sqlite3"
)

func Example() {
	fs := sqlite3.RegisterVolatileFileSystem("volatile")
	defer sqlite3.UnregisterVolatileFileSystem(fs)

	// Create a connection in leader replication mode.
	methods := sqlite3.NoopReplicationMethods()
	leader, err := connection.OpenLeader("test.db", methods)
	if err != nil {
		log.Fatalf("failed to open leader connection: %v", err)
	}
	defer leader.Close()

	// Create a connection in follower replication mode.
	follower, err := connection.OpenFollower("test.db")
	if err != nil {
		log.Fatalf("failed to open follower connection: %v", err)
	}
	defer follower.Close()
}
