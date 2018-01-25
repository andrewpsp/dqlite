package dqlite

import (
	"github.com/CanonicalLtd/dqlite/internal/registry"
)

// Registry tracks internal data shared by the dqlite Driver and FSM.
type Registry registry.Registry

// NewRegistry creates a new Registry, which is expected to be passed to both
// NewFSM and NewDriver.
func NewRegistry(id int) *Registry {
	// Create a random ID for the volatile file system, mainly to avoid
	// collisions in unit tests.
	//id := rand.Int()

	return (*Registry)(registry.New(id))
}
