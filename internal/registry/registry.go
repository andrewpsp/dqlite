// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package registry

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/CanonicalLtd/dqlite/internal/trace"
	"github.com/CanonicalLtd/dqlite/internal/transaction"
	"github.com/CanonicalLtd/go-sqlite3"
)

// Registry is a dqlite node-level data structure that tracks:
//
// - All SQLite connections opened on the node, either in leader replication
//   mode or follower replication mode.
//
// - All inflight WAL write transactions, either for leader or follower
//   connections.
//
// - All tracers used to emit trace messages.
//
// - Last log index applied by the FSM.
//
// A single Registry instance is shared by a single replication.FSM instance, a
// single replication.Methods instance and a single dqlite.Driver instance.
//
// Methods that access or mutate the registry are not thread-safe and must be
// performed after acquiring the lock. See Lock() and Unlock().
type Registry struct {
	mu        sync.Mutex                     // Serialize access to internal state.
	fs        *sqlite3.VolatileFileSystem    // Database files are be stored in this in-memory FS.
	leaders   map[*sqlite3.SQLiteConn]string // Map leader connections to database filenames.
	followers map[string]*sqlite3.SQLiteConn // Map database filenames to follower connections.
	txns      map[uint64]*transaction.Txn    // Transactions by ID
	tracers   *trace.Set                     // Tracers used by this dqlite instance.
	index     uint64                         // Last log index applied by the dqlite FSM.
	frames    uint64                         // Number of frames written to the WAL so far.
	hookSync  *hookSync                      // Used for synchronizing Methods and FSM.

	// Map a connection to its serial number. Serial numbers are guaranteed
	// to be unique inside the same process.
	serial map[*sqlite3.SQLiteConn]uint64

	// Circular buffer holding the IDs of the last N transactions that
	// where successfully committed. It is used to recover a transaction
	// that errored because of lost leadership but that might actually get
	// completed because a quorum was reached for the lost commit frames
	// command log.
	committed       []uint64
	committedCursor int

	// Map a leader connection to the ID of the last transaction executed
	// on it. Used by the driver's Tx implementation to know its ID in case
	// a client asks for it for recovering a lost commit.
	lastTxnIDs map[*sqlite3.SQLiteConn]uint64

	// Flag indicating whether transactions state transitions
	// should actually callback the relevant SQLite APIs. Some
	// tests need set this flag to true because there's no public
	// API to acquire the WAL read lock in leader connections.
	txnDryRun bool
}

// New creates a new registry.
func New(id int) *Registry {
	tracers := trace.NewSet(250)

	// Register the is the tracer that will be used by the FSM associated
	// with this registry.
	tracers.Add("fsm")

	return &Registry{
		fs:         sqlite3.RegisterVolatileFileSystem(fmt.Sprintf("volatile-%d", id)),
		leaders:    map[*sqlite3.SQLiteConn]string{},
		followers:  map[string]*sqlite3.SQLiteConn{},
		txns:       map[uint64]*transaction.Txn{},
		tracers:    tracers,
		serial:     map[*sqlite3.SQLiteConn]uint64{},
		committed:  make([]uint64, committedBufferSize),
		lastTxnIDs: make(map[*sqlite3.SQLiteConn]uint64),
	}
}

// Close the registry, releasing allocated resources.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	sqlite3.UnregisterVolatileFileSystem(r.fs)
}

// FS returns the underlying volatile file system.
func (r *Registry) FS() *sqlite3.VolatileFileSystem {
	return r.fs
}

// Lock the registry.
func (r *Registry) Lock() {
	r.mu.Lock()
}

// Unlock the registry.
func (r *Registry) Unlock() {
	r.mu.Unlock()
}

// Testing sets up this registry for unit-testing.
//
// The tracers will forward all entries to the testing logger, using the given
// node prefix.
func (r *Registry) Testing(t *testing.T, node int) {
	r.tracers.Testing(t, node)
}

// Dump the content of the registry, useful for debugging.
func (r *Registry) Dump() string {
	buffer := bytes.NewBuffer(nil)
	fmt.Fprintf(buffer, "leaders:\n")
	for conn, name := range r.leaders {
		fmt.Fprintf(buffer, "-> %d: %s\n", r.ConnSerial(conn), name)
	}
	fmt.Fprintf(buffer, "followers:\n")
	for name, conn := range r.followers {
		fmt.Fprintf(buffer, "-> %d: %s\n", r.ConnSerial(conn), name)
	}
	fmt.Fprintf(buffer, "transactions:\n")
	for _, txn := range r.txns {
		fmt.Fprintf(buffer, "-> %s\n", txn)
	}
	return buffer.String()
}

// Keep track of at most this much comitted transactions. This number should be
// large enough for any real-world situation, where it's unlikely that a client
// tries to recover a transaction that is so old.
const committedBufferSize = 10000
