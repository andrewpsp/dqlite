package bindings_test

import (
	"encoding/binary"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/CanonicalLtd/dqlite/internal/bindings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_Lifecycle(t *testing.T) {
	file, cleanup := newTestFile(t)
	defer cleanup()

	cluster := newTestCluster()

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	server.Close()
	server.Free()
}

func TestServer_Run(t *testing.T) {
	file, cleanup := newTestFile(t)
	defer cleanup()

	cluster := newTestCluster()

	server, err := bindings.NewServer(file, cluster)
	require.NoError(t, err)

	defer server.Free()
	defer server.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	ch := make(chan error)
	go func() {
		err := server.Run()
		ch <- err
	}()

	go func() {
		conn, err := listener.Accept()
		require.NoError(t, err)
		require.NoError(t, server.Handle(conn))
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)

	err = binary.Write(conn, binary.LittleEndian, bindings.ServerProtocolVersion)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	time.Sleep(100 * time.Millisecond)

	require.NoError(t, server.Stop())

	err = <-ch
	assert.NoError(t, err)
}

func newTestFile(t *testing.T) (*os.File, func()) {
	t.Helper()

	file, err := ioutil.TempFile("", "dqlite-bindings-")
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, file.Close())
		require.NoError(t, os.Remove(file.Name()))
	}

	return file, cleanup
}

type testCluster struct {
}

func newTestCluster() *testCluster {
	return &testCluster{}
}

func (c *testCluster) Replication() string {
	return "test"
}

func (c *testCluster) Leader() string {
	return "127.0.0.1:666"
}

func (c *testCluster) Servers() ([]string, error) {
	addresses := []string{
		"1.2.3.4:666",
		"5.6.7.8:666",
	}

	return addresses, nil
}

func (c *testCluster) Recover(token uint64) error {
	return nil
}
