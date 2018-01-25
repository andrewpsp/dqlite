package dqlite

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/registry"
	"github.com/CanonicalLtd/dqlite/internal/replication"
	"github.com/CanonicalLtd/go-grpc-sql"
	"github.com/CanonicalLtd/raft-http"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v2"
)

// A Node in a dqlite cluster.
type Node struct {
	servers    grpcsql.ServerStore // Used to store addresses of known database nodes.
	identity   IdentityStore       // Used to store the node's raft identity.
	conf       *raft.Config        // Raft configuration.
	logs       raft.LogStore       // Raft log store.
	stable     raft.StableStore    // Raft stable store.
	snaps      raft.SnapshotStore  // Raft snapshot store.
	logger     *zap.Logger         // Zap logger to use.
	driverName string              // Name to use when registering the gRPC SQL driver into database/sql.
	clientTLS  *tls.Config         // Used for outbound HTTP/2.0 requests to other nodes.
	userAgent  string              // User-Agent of all outbout HTTP requests will be set to this value.
	mu         sync.RWMutex        // Serialize access to the fields below.
	id         int                 // Cache for IdentityStore.Get().
	raft       *raftInstance       // Wrapper around raft.Raft with additional references.
}

// NewNode creates a dqlite cluster node with the given options.
//
// All nodes in a dqlite cluster can perform queries against the SQLite
// databases managed by the cluster.
//
// Some nodes (usually 3 or 5) can be also "database nodes" part of the dqlite
// internal raft cluster and holding replicated copies of the SQLite databases
// managed by the dqlite cluster.
//
// The decision of whether a node should be (or should become at some point) a
// database node is automatically managed by dqlite, which will monitor all
// nodes to mantain the desired number of database nodes.
//
// The dir parameter is the directory where dqlite will store:
//
// - A plain SQLite database file which is local to this node and where the
//   addresses of current database nodes are stored. They are used by to create
//   grpcsql client that can perform SQL queries against the cluster, using the
//   standard database/sql interface. These addresses are updated dynamically
//   and transparently upon cluster changes. The default is "<dir>/servers.db".
//
// - A plain YAML file which is local to this node and where node-specific
//   information is stored, such as its raft ID (in case the node is or becomes
//   a database node). The default is "<dir>/identity.yaml".
//
// - A boltdb file for the raft logs, in case the node is or becomes a database
//   node. The default is "<dir>/logs.db".
//
// - A sub-directory for the raft snapshots, in case the node is or becomes a
//   database node. The default is "<dir>/snapshots/".
//
// All these sub-paths can be changed with node options, by injecting custom
// node and raft stores.
//
// If the given directory does not exists, dqlite will attempt to create it
// (with permissions 0700).
func NewNode(dir string, options ...NodeOption) (*Node, error) {
	node := &Node{}

	// Apply custom options.
	for _, option := range options {
		option(node)
	}

	// Set defaults for missing options.
	if err := nodeDefaults(node, dir); err != nil {
		return nil, err
	}

	identity, err := node.identity.Get()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get node identity")
	}

	// Cache the node raft ID, if any.
	if identity != nil {
		node.id = int(identity.ID)
	}

	// Register the gRPC SQL driver.
	credentials := credentials.NewTLS(node.clientTLS)
	driver := grpcsql.NewDriver(
		node.servers,
		grpcsql.DriverLogger(node.logger),
		grpcsql.DriverDialOptions(grpc.WithTransportCredentials(credentials)),
	)
	sql.Register(node.driverName, driver)

	return node, nil
}

// ServeHTTP makes a Node implement the http.Handler interface, which can used
// to expose this node on the network.
//
// The handler will manage all inbound requests to this node, such as:
//
//  - Cluster membership changes such ass add/remove nodes. If this node is not
//    a database node or it's not the leader database node, it will redirect
//    the request to the leader.
//
//  - Raft log replication, if this node is a follower database node.
//
//  - Execution of SQL queries using the grpcsql protocol. If this node is not
//    a database node or it's not the leader database node, it will redirect
//    the request to the leader.
//
// The handler must be mounted on an HTTP/2.0 server (required for gRPC RPCs)
// and serve all requests where User-Agent is set to "dqlite". The user agent
// can be customized using the WithUserAgent node option.
func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch n.currentID() {
	case -1:
		// This is a database node, but Start() hasnt been called yet.
		http.Error(w, "node hasn't be started yet", http.StatusServiceUnavailable)
		return
	case 0:
		// This is not a database node, and should not be serving any
		// request.
		http.Error(w, "node is not a database node", http.StatusNotImplemented)
		return
	}
}

// Bootstrap the node, making it the first node of the cluster. It will be
// assigned ID 1.
//
// This API is idempotent if the node has already ID 1 and has already been
// bootstrapped.
func (n *Node) Bootstrap() error {
	if n.id != 0 {
		if n.id != 1 {
			return fmt.Errorf("only node 1 can be bootstrapped")
		}

		// If the identity is set, the node was bootstrapped already.
		return nil
	}

	return nil
}

// Start the node.
//
// This method must be invoked after this Node instance has been hooked into a
// http.Server, and the http.Server has been started using a net.Listener.
//
// The network address of the listener must be then passed to this method, and
// it will be used as local raft address if this node acts as database node.
func (n *Node) Start(addr net.Addr) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.id == 0 {
		// This is not a database node, so nothing to do.
		return nil
	}

	// Create raft transport.
	handler := rafthttp.NewHandler()
	dial := rafthttp.NewDialTLS(n.clientTLS)
	layer := rafthttp.NewLayer("/dqlite/raft", addr, handler, dial)
	trans := raft.NewNetworkTransport(layer, 2, time.Second, ioutil.Discard)

	// Copy our raft configuration and set the raft server ID.
	conf := *n.conf
	conf.LocalID = raft.ServerID(strconv.Itoa(int(n.id)))

	// Create a dqlite FSM.
	registry := registry.New(n.id)
	fsm := replication.NewFSM(registry)

	// Start raft.
	raft, err := raft.NewRaft(&conf, fsm, n.logs, n.stable, n.snaps, trans)
	if err != nil {
		return errors.Wrap(err, "failed to start raft")
	}

	// Create gRPC SQL gateway backed by the a driver.
	driver, err := NewDriver((*Registry)(registry), raft, DriverConfig{})
	if err != nil {
		return errors.Wrap(err, "failed to create driver")
	}
	server := grpc.NewServer()
	grpcsql.RegisterGateway(server, driver, grpcsql.ServiceLogger(n.logger))

	n.raft = &raftInstance{
		layer:   layer,
		handler: handler,
		raft:    raft,
		server:  server,
	}

	return nil
}

// Return the current node identity ID.
//
// If the node is not a database node, 0 is returned.
//
// If the node is a database node, but hasn't been started yet, -1 is returned.
func (n *Node) currentID() int64 {
	n.mu.RLock()
	n.mu.RUnlock()

	if n.id == 0 {
		// This is not a database node.
		return 0
	}

	if n.raft == nil {
		// This is a database node, but it hasn't been started yet.
		return -1
	}

	return int64(n.id)
}

// A NodeOption can be used to customize a Node.
type NodeOption func(*Node)

// WithClientTLS sets the TLS configuration that this node will use when
// performing outbound HTTP/2.0 requests to other nodes.
//
// You have to use either this option or the WithInsecure option if the TLS
// certificate you use to expose your cluster nodes is self-signed.
func WithClientTLS(clientTLS *tls.Config) NodeOption {
	return func(node *Node) {
		node.clientTLS = clientTLS
	}
}

// WithInsecure is a convenience around WithClientTLS to skip verification of a
// target node certificate when performing outbound HTTP/2.0 requests
func WithInsecure() NodeOption {
	clientTLS := &tls.Config{
		InsecureSkipVerify: true,
	}
	return WithClientTLS(clientTLS)
}

// WithLogger sets the zap.Logger instance used by this node to emit log
// messages.
func WithLogger(logger *zap.Logger) NodeOption {
	return func(node *Node) {
		node.logger = logger
	}
}

// WithUserAgent sets the User-Agent header of all dqlite-related outbound
// HTTP/2.0 requests performed by this node. If not set, the default is
// "dqlite" (see also the docstring of the ServerHTTP method). All nodes in a
// cluster must share the same user agent.
func WithUserAgent(userAgent string) NodeOption {
	return func(node *Node) {
		node.userAgent = userAgent
	}
}

// WithDriverName sets the name that will be used to register the internal gRPC
// SQL driver created by this node agains the database/sql registry. The
// default is "dqlite".
//
// To open a dqlite database you can then use sql.Open(driverName, "my.db").
func WithDriverName(driverName string) NodeOption {
	return func(node *Node) {
		node.driverName = driverName
	}
}

// WithServerStore makes the node use the given grpcsql.ServerStore instead of
// the default one (a grpcsql.DatabaseServerStore backed by <dir>/servers.db).
func WithServerStore(servers grpcsql.ServerStore) NodeOption {
	return func(node *Node) {
		node.servers = servers
	}
}

// WithIdentityStore makes the node use the given IdentityStore instead of
// the default one (a YAMLIdentityStore backed by <dir>/identity.yaml).
func WithIdentityStore(identity IdentityStore) NodeOption {
	return func(node *Node) {
		node.identity = identity
	}
}

// WithConfig makes the node use the given raft configuration instead of the
// default one.
func WithConfig(conf *raft.Config) NodeOption {
	return func(node *Node) {
		node.conf = &raft.Config{}
		*node.conf = *conf
	}
}

// WithLogStore makes the node use the given raft log store instead of the
// default one (which is a boltdb file under <dir>/raft/logs.db).
func WithLogStore(logs raft.LogStore) NodeOption {
	return func(node *Node) {
		node.logs = logs
	}
}

// WithStableStore makes the node use the given raft stable store instead of the
// default one (which is a boltdb file under <dir>/raft/logs.db).
func WithStableStore(stable raft.StableStore) NodeOption {
	return func(node *Node) {
		node.stable = stable
	}
}

// WithSnapshotStore makes the node use the given raft snapshot store instead of the
// default one (which is a file-based sore under <dir>/raft/snapshots/).
func WithSnapshotStore(snaps raft.SnapshotStore) NodeOption {
	return func(node *Node) {
		node.snaps = snaps
	}
}

// Identity contains information about this node, such has its raft ID and
// address (if any).
type Identity struct {
	ID uint64
}

// IdentityStore is used to persist and query the raft identity of this
// node, when it acts as database node.
type IdentityStore interface {
	Get() (*Identity, error)
	Set(*Identity) error
}

// YAMLIdentityStore stores the node identity in a YAML file.
type YAMLIdentityStore struct {
	mu       sync.RWMutex
	filename string
}

// NewYAMLIdentityStore creates a new YAML identity store using the given
// file.
func NewYAMLIdentityStore(filename string) *YAMLIdentityStore {
	return &YAMLIdentityStore{
		filename: filename,
	}
}

// Get reads the node identity from the underlying YAML file. If no YAML file
// exists, nil is returned.
func (s *YAMLIdentityStore) Get() (*Identity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, err := os.Stat(s.filename); err != nil {
		if os.IsNotExist(err) {
			// This node is not a database node and hasn't stored
			// any identity.
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to access YAML node identity file %s", s.filename)
	}

	data, err := ioutil.ReadFile(s.filename)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read YAML node identity file %s", s.filename)
	}

	identity := Identity{}
	err = yaml.Unmarshal(data, &identity)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse YAML node identity file %s", s.filename)
	}

	return &identity, nil
}

// Set replaces the node identity information in the underlying YAML file.
func (s *YAMLIdentityStore) Set(identity *Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := yaml.Marshal(identity)
	if err != nil {
		return errors.Wrap(err, "failed to encode node identity to YAML")
	}

	if err := ioutil.WriteFile(s.filename, data, 0600); err != nil {
		return errors.Wrapf(err, "failed to write YAML node identity file %s", s.filename)
	}

	return nil
}

// InmemIdentityStore stores the node identity in memory.
type InmemIdentityStore struct {
	mu       sync.RWMutex
	identity *Identity
}

// NewInmemIdentityStore create a store for saving the node identity in
// memory.
func NewInmemIdentityStore() *InmemIdentityStore {
	return &InmemIdentityStore{}
}

// Get returns previously stored identity.
func (s *InmemIdentityStore) Get() (*Identity, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.identity, nil
}

// Set replaces the node identity.
func (s *InmemIdentityStore) Set(identity *Identity) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.identity = identity
	return nil
}

// Set defaults for options that were left blank.
func nodeDefaults(node *Node, dir string) error {
	// Set a default server store if missing.
	if node.servers == nil {
		filename := filepath.Join(dir, "servers.db")
		store, err := grpcsql.DefaultServerStore(filename)
		if err != nil {
			return errors.Wrapf(err, "failed to create default server store at %s", filename)
		}
		node.servers = store
	}

	// Set a default node store if missing.
	if node.identity == nil {
		filename := filepath.Join(dir, "identity.yaml")
		node.identity = NewYAMLIdentityStore(filename)
	}

	// Set a default raft config if missing.
	if node.conf == nil {
		node.conf = raft.DefaultConfig()
	}

	// Set default raft log and stable stores if missing.
	if node.logs == nil || node.stable == nil {
		filename := filepath.Join(dir, "logs.db")
		store, err := raftboltdb.NewBoltStore(filename)
		if err != nil {
			return errors.Wrapf(err, "failed to open bolt store at %s", filename)
		}
		if node.logs == nil {
			node.logs = store
		}
		if node.stable == nil {
			node.stable = store
		}
	}

	// Set a default snapshot store if missing.
	if node.snaps == nil {
		store, err := raft.NewFileSnapshotStore(dir, 2, ioutil.Discard)
		if err != nil {
			return errors.Wrapf(err, "failed to open snapshot store at %s", dir)
		}
		node.snaps = store
	}

	// Set the default logger if missing.
	if node.logger == nil {
		node.logger = nodeDefaultLogger()
	}

	// Set the default client TLS configuration if missing.
	if node.clientTLS == nil {
		node.clientTLS = &tls.Config{}
	}

	// Set the default user agent if missing.
	if node.userAgent == "" {
		node.userAgent = "dqlite"
	}

	return nil
}

// Create a zap.Logger with reasonable defaults.
func nodeDefaultLogger() *zap.Logger {
	encoderConfig := zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		os.Stdout,
		zapcore.InfoLevel,
	)
	return zap.New(core)
}
