package dqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

// ClusterStore is where cluster-wide replicated data is persisted.
//
// This includes raft data (such as logs and snapshots) and replicated SQLite
// database files.
type ClusterStore struct {
	databases string
	logs      raft.LogStore
	stable    raft.StableStore
	snaps     raft.SnapshotStore
}

// NodeRegistry manages information about the nodes in a dqlite cluster, and
// persists it in in a SQLite database replicated by dqlite itself.
type NodeRegistry struct {
	tx      *sql.Tx       // Transaction to use to query the store.
	queries *nodesQueries // Text of node-relaed SQL queries.
}

// DefaultNodeRegistry creates a new node store.
//
// TODO: drop this, it's just to make deadcode happy
var DefaultNodeRegistry = newNodeRegistry

// Creates a new NodeRegistry.
func newNodeRegistry(tx *sql.Tx, queries *nodesQueries) *NodeRegistry {
	return &NodeRegistry{
		tx:      tx,
		queries: queries,
	}
}

// Init initializes the store, creating the nodes table if doesn't exist.
func (r *NodeRegistry) Init(ctx context.Context) error {
	_, err := r.tx.ExecContext(ctx, r.queries.CreateTable)
	if err != nil {
		return errors.Wrap(err, "failed to create nodes table")
	}
	return nil
}

// Create adds a node to the nodes table. It returns the ID of the newly
// inserted node.
func (r *NodeRegistry) Create(ctx context.Context, address string) (int, error) {
	result, err := r.tx.ExecContext(ctx, r.queries.Insert, address)
	if err != nil {
		return -1, errors.Wrap(err, "failed to insert row into nodes table")
	}
	id, err := result.LastInsertId()
	if err != nil {
		return -1, errors.Wrap(err, "failed to get node ID")
	}
	return int(id), nil
}

// Accept sets the pending flag of the node to false.
func (r *NodeRegistry) Accept(ctx context.Context, id int) error {
	result, err := r.tx.ExecContext(ctx, r.queries.UpdatePending, 0, id)
	if err != nil {
		return errors.Wrapf(err, "failed to update nodes table row")
	}
	n, err := result.RowsAffected()
	if n != 1 {
		return fmt.Errorf("unexpected number of rows affected by the update: %d", n)
	}
	return nil
}

// Fetch returns all information about the the node with the given ID.
//
// If no node with the given ID exists, an error is returned.
func (r *NodeRegistry) Fetch(ctx context.Context, id int) (NodeInfo, error) {
	info := NodeInfo{
		ID: id,
	}

	rows, err := r.tx.QueryContext(ctx, r.queries.Select, id)
	if err != nil {
		return info, errors.Wrap(err, "failed to query nodes table")
	}
	defer rows.Close()

	if !rows.Next() {
		return info, fmt.Errorf("not found")
	}

	err = rows.Scan(
		&info.Address,
		&info.Pending,
		&info.Heartbeat,
		&info.Name,
		&info.Description,
		&info.Schema,
		&info.Version,
	)
	if err != nil {
		return info, errors.Wrap(err, "failed to scan nodes table row")
	}

	if rows.Next() {
		return info, fmt.Errorf("more than one matching node found")
	}

	if err := rows.Err(); err != nil {
		return info, errors.Wrapf(err, "result set error")
	}

	return info, nil
}

// Heartbeat refreshes the heartbeat column of the node the given ID.
//
// If no node with the given ID exists, an error is returned.
func (r *NodeRegistry) Heartbeat(ctx context.Context, id int) error {
	result, err := r.tx.ExecContext(ctx, r.queries.UpdateHeartbeat, id)
	if err != nil {
		return errors.Wrapf(err, "failed to update nodes table row")
	}
	n, err := result.RowsAffected()
	if n != 1 {
		return fmt.Errorf("unexpected number of rows affected by the update: %d", n)
	}
	return nil
}

// NodeInfo holds information about a single node in the nodes SQL table
// managed by Store.
type NodeInfo struct {
	ID          int       // Stable node identifier.
	Address     string    // Network address of the node.
	Pending     bool      // Whether the node has asked to be accepted but hasn't joined yet.
	Heartbeat   time.Time // Timestamp of the last heartbeat from the node.
	Name        string    // Name of the node (maybe empty)
	Description string    // Node description (maybe empty)
	Schema      int       // Schema version the node expects (optional)
	Version     int       // Application version the node is running (optional)
}

// NodesTable contains information about the nodes table in the Store.
type NodesTable struct {
	Name              string                 // Name of the table (default is "nodes")
	Columns           map[NodesColumn]string // Names of the required columns.
	CreateIfNotExists bool                   // Whether to create the table if it doesn't exist yet.
}

// DefaultNodesTable foo.
//
// TODO: drop this, it's just to make deadcode happy
var DefaultNodesTable = defaultNodesTable

func defaultNodesTable() NodesTable {
	return NodesTable{
		Name:              "nodes",
		Columns:           defaultNodesColums,
		CreateIfNotExists: true,
	}
}

// NodesColumn is a code identifying a certain column in the nodes table of the
// store.
type NodesColumn int

// Codes for all required columns in the nodes table.
const (
	NodesColumnID NodesColumn = iota
	NodesColumnAddress
	NodesColumnPending
	NodesColumnHeartbeat
	NodesColumnName
	NodesColumnDescription
	NodesColumnSchema
	NodesColumnVersion
)

var defaultNodesColums = map[NodesColumn]string{
	NodesColumnID:          "id",
	NodesColumnAddress:     "address",
	NodesColumnPending:     "pending",
	NodesColumnHeartbeat:   "heartbeat",
	NodesColumnName:        "name",
	NodesColumnDescription: "description",
	NodesColumnSchema:      "schema",
	NodesColumnVersion:     "version",
}

// Hold the text of node-related SQL queries generated from a NodesTable
// definition.
type nodesQueries struct {
	CreateTable     string
	Insert          string
	Select          string
	UpdatePending   string
	UpdateHeartbeat string
}

// NewNodesQueries foo.
//
// TODO: drop this, it's just to make deadcode happy
var NewNodesQueries = newNodesQueries

// Render nodes-related SQL queries using the given NodesTable definition.
func newNodesQueries(table NodesTable) *nodesQueries {
	return &nodesQueries{
		CreateTable: fmt.Sprintf(
			nodesQueryCreateTable,
			table.Name,
			table.Columns[NodesColumnID],
			table.Columns[NodesColumnAddress],
			table.Columns[NodesColumnPending],
			table.Columns[NodesColumnHeartbeat],
			table.Columns[NodesColumnName],
			table.Columns[NodesColumnDescription],
			table.Columns[NodesColumnSchema],
			table.Columns[NodesColumnVersion],
			table.Columns[NodesColumnAddress],
			table.Columns[NodesColumnName],
		),
		Insert: fmt.Sprintf(
			nodesQueryInsert,
			table.Name,
			table.Columns[NodesColumnAddress],
		),
		Select: fmt.Sprintf(
			nodesQuerySelect,
			table.Columns[NodesColumnAddress],
			table.Columns[NodesColumnPending],
			table.Columns[NodesColumnHeartbeat],
			table.Columns[NodesColumnName],
			table.Columns[NodesColumnDescription],
			table.Columns[NodesColumnSchema],
			table.Columns[NodesColumnVersion],
			table.Name,
			table.Columns[NodesColumnID],
		),
		UpdatePending: fmt.Sprintf(
			nodesQueryUpdatePending,
			table.Name,
			table.Columns[NodesColumnPending],
			table.Columns[NodesColumnID],
		),
		UpdateHeartbeat: fmt.Sprintf(
			nodesQueryUpdateHeartbeat,
			table.Name,
			table.Columns[NodesColumnHeartbeat],
			table.Columns[NodesColumnID],
		),
	}
}

const (
	nodesQueryCreateTable = `
CREATE TABLE IF NOT EXISTS %s (
  %s INTEGER PRIMARY KEY,
  %s TEXT NOT NULL,
  %s INTEGER NOT NULL DEFAULT 1,
  %s DATETIME DEFAULT CURRENT_TIMESTAMP,
  %s TEXT DEFAULT '',
  %s TEXT DEFAULT '',
  %s INTEGER DEFAULT -1,
  %s INTEGER DEFAULT -1,
  UNIQUE (%s),
  UNIQUE (%s)
)`
	nodesQueryInsert = `
INSERT INTO %s(%s) VALUES(?)
`
	nodesQuerySelect = `
SELECT %s, %s, %s, %s, %s, %s, %s FROM %s WHERE %s=?
`
	nodesQueryUpdatePending = `
UPDATE %s SET %s=? WHERE %s=?
`
	nodesQueryUpdateHeartbeat = `
UPDATE %s SET %s=strftime('%%s','now') WHERE %s=?
`
)
