package dqlite_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite"
	_ "github.com/CanonicalLtd/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Add a new node to the registry.
func TestNodeRegistry_Create(t *testing.T) {
	registry, cleanup := newNodeRegistry(t)
	defer cleanup()

	id, err := registry.Create(context.Background(), "1.2.3.4:666")
	require.NoError(t, err)

	assert.Equal(t, 1, id)
}

// Fetch a node by ID.
func TestNodeRegistry_Fetch(t *testing.T) {
	registry, cleanup := newNodeRegistry(t)
	defer cleanup()

	id, err := registry.Create(context.Background(), "1.2.3.4:666")
	require.NoError(t, err)

	node, err := registry.Fetch(context.Background(), id)
	require.NoError(t, err)

	assert.Equal(t, 1, node.ID)
	assert.Equal(t, "1.2.3.4:666", node.Address)
	assert.True(t, node.Pending)
	assert.WithinDuration(t, time.Now(), node.Heartbeat, time.Second)
	assert.Equal(t, "", node.Name)
	assert.Equal(t, "", node.Description)
	assert.Equal(t, -1, node.Schema)
	assert.Equal(t, -1, node.Version)

	_, err = registry.Fetch(context.Background(), 123)
	require.EqualError(t, err, "not found")
}

// Turn off the pending flag of a node
func TestNodeRegistry_Accep(t *testing.T) {
	registry, cleanup := newNodeRegistry(t)
	defer cleanup()

	id, err := registry.Create(context.Background(), "1.2.3.4:666")
	require.NoError(t, err)

	err = registry.Accept(context.Background(), id)
	require.NoError(t, err)

	node, err := registry.Fetch(context.Background(), id)
	require.NoError(t, err)

	assert.False(t, node.Pending)
}

// Refresh the heartbeat of a node.
func TestNodeRegistry_Heartbeat(t *testing.T) {
	registry, cleanup := newNodeRegistry(t)
	defer cleanup()

	id, err := registry.Create(context.Background(), "1.2.3.4:666")
	require.NoError(t, err)

	info1, err := registry.Fetch(context.Background(), id)
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	err = registry.Heartbeat(context.Background(), id)
	require.NoError(t, err)

	info2, err := registry.Fetch(context.Background(), id)
	require.NoError(t, err)

	assert.WithinDuration(t, time.Now(), info2.Heartbeat, time.Second)

	t.Log(info1.Heartbeat)
	t.Log(info2.Heartbeat)
	assert.True(t, info1.Heartbeat.Before(info2.Heartbeat))

	err = registry.Heartbeat(context.Background(), 123)
	require.EqualError(t, err, "unexpected number of rows affected by the update: 0")
}

// Create a new node registry.
func newNodeRegistry(t *testing.T) (*dqlite.NodeRegistry, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	registry := dqlite.NewNodeRegistry(tx)

	err = registry.Init(context.Background())
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, tx.Rollback())
		require.NoError(t, db.Close())
	}

	return registry, cleanup
}
