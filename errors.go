package dqlite

import "github.com/hashicorp/raft"

// Wrapper around raft.ErrNotLeader implemening the grpcsql/cluster.Error
// interface.
type notLeaderError struct {
}

// NotLeader returns true if the error is due to the server not being the
// leader.
func (e notLeaderError) Error() string {
	return raft.ErrNotLeader.Error()
}

// NotLeader returns true if the error is due to the server not being the
// leader.
func (e notLeaderError) NotLeader() bool {
	return true
}
