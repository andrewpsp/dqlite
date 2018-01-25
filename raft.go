package dqlite

import (
	rafthttp "github.com/CanonicalLtd/raft-http"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// A dqlite-specific wrapper around raft.Raft, which also holds a reference to the rafthttp
// handler and layer used to create the raft transport.
type raftInstance struct {
	layer   *rafthttp.Layer   // Used for raft replication and membership.
	handler *rafthttp.Handler // Used for raft replication and membership.
	raft    *raft.Raft        // Actual raft instance.
	server  *grpc.Server      // Used to handle gRPC SQL requests.
}

// Helper to bootstrap the raft cluster if needed.
func raftMaybeBootstrap(
	conf *raft.Config,
	logs *raftboltdb.BoltStore,
	snaps raft.SnapshotStore,
	trans raft.Transport) error {

	// First check if we were already bootstrapped.
	hasExistingState, err := raft.HasExistingState(logs, logs, snaps)
	if err != nil {
		return errors.Wrap(err, "failed to check if raft has existing state")
	}
	if hasExistingState {
		return nil
	}

	// We need to bootstrap a new configuration.
	server := raft.Server{
		ID:      conf.LocalID,
		Address: trans.LocalAddr(),
	}
	configuration := raft.Configuration{
		Servers: []raft.Server{server},
	}

	return raft.BootstrapCluster(conf, logs, logs, snaps, trans, configuration)
}

/*
// NewAddressProvider creates a new AddressProvider.
func NewAddressProvider(store *Store) *AddressProvider {
	return &AddressProvider{store: store}
}

// AddressProvider that looks up server addresses in a SQL table.
type AddressProvider struct {
	store *Store
}

// ServerAddr returns the address of the raft node with the given ID.
func (p *AddressProvider) ServerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	storeID, err := strconv.Atoi(string(id))
	if err != nil {
		return "", errors.Wrap(err, "non-numeric server ID")
	}

	var address string
	err = p.store.Tx(func(tx *Tx) error {
		var err error
		address, err = tx.NodeAddress(int64(storeID))
		if err != nil {
			return errors.Wrap(err, "failed to fetch node address")
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	return raft.ServerAddress(address), nil
}
*/
