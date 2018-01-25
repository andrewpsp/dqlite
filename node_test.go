package dqlite_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite"
	"github.com/CanonicalLtd/go-grpc-sql"
	"github.com/CanonicalLtd/raft-test"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// A new node with default parameters can be created by passing it a directory.
func TestNewNode_Defaults(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	node, err := dqlite.NewNode(dir)
	assert.NoError(t, err)
	assert.NotNil(t, node)

	for _, name := range []string{"servers.db", "logs.db", "snapshots"} {
		_, err := os.Stat(filepath.Join(dir, name))
		assert.NoError(t, err)
	}
}

// Get and Set the node's identity to a YAML file.
func TestYAMLIdentityStore(t *testing.T) {
	dir, cleanup := newDir(t)
	defer cleanup()

	filename := filepath.Join(dir, "identity.yaml")
	store := dqlite.NewYAMLIdentityStore(filename)

	// If the file does not exist, nil is returned.
	identity, err := store.Get()
	require.NoError(t, err)
	assert.Nil(t, identity)

	// Store a new identity.
	identity = &dqlite.Identity{
		ID: 1,
	}
	require.NoError(t, store.Set(identity))

	// Fetch the stored identity.
	identity, err = store.Get()
	require.NoError(t, err)
	require.NotNil(t, identity)
	assert.Equal(t, uint64(1), identity.ID)
}

// Create a new Node using in-memory stores.
func newNode(t testing.TB) *dqlite.Node {
	t.Helper()

	servers := grpcsql.NewInmemServerStore()

	conf := raft.DefaultConfig()

	// Set low timeouts.
	conf.HeartbeatTimeout = rafttest.Duration(15 * time.Millisecond)
	conf.ElectionTimeout = rafttest.Duration(15 * time.Millisecond)
	conf.CommitTimeout = rafttest.Duration(1 * time.Millisecond)
	conf.LeaderLeaseTimeout = rafttest.Duration(10 * time.Millisecond)

	identity := dqlite.NewInmemIdentityStore()
	store := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()
	logger := zaptest.NewLogger(t)

	node, err := dqlite.NewNode(
		"",
		dqlite.WithServerStore(servers),
		dqlite.WithIdentityStore(identity),
		dqlite.WithConfig(conf),
		dqlite.WithLogStore(store),
		dqlite.WithSnapshotStore(snaps),
		dqlite.WithLogger(logger),
	)
	require.NoError(t, err)

	return node
}
