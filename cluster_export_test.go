package dqlite

import (
	"database/sql"
)

// Create a node registry with default values.
func NewNodeRegistry(tx *sql.Tx) *NodeRegistry {
	nodesTable := defaultNodesTable()
	nodesQueries := newNodesQueries(nodesTable)

	return newNodeRegistry(tx, nodesQueries)
}
