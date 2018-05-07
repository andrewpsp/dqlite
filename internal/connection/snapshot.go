package connection

import (
	"crypto/rand"
	"fmt"

	"github.com/CanonicalLtd/go-sqlite3"
	"github.com/pkg/errors"
)

// Snapshot returns a snapshot of the SQLite database with the given path in
// volatile file system.
//
// The snapshot is comprised of two byte slices, one with the content of the
// database and one is the content of the WAL file.
func Snapshot(fs *sqlite3.VolatileFileSystem, path string) ([]byte, []byte, error) {
	// Create a source connection that will read the database snapshot.
	sourceURI := EncodeURI(path, fs.Name(), "")
	sourceConn, err := open(sourceURI)
	if err != nil {
		return nil, nil, errors.Wrap(err, "source connection")
	}
	defer sourceConn.Close()

	// Create a backup connection that will write the database snapshot.
	backupPath := newBackupPath(path)
	backupURI := EncodeURI(backupPath, fs.Name(), "")
	backupConn, err := open(backupURI)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open backup connection")
	}
	defer fs.Remove(backupPath)

	// Cleanup the wal file of the backup as well.
	defer fs.Remove(backupPath + "-wal")
	defer backupConn.Close()

	// Perform the backup.
	backup, err := backupConn.Backup("main", sourceConn, "main")
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to init backup database")
	}
	done, err := backup.Step(-1)
	backup.Close()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to backup database")
	}
	if !done {
		return nil, nil, fmt.Errorf("database backup not complete")
	}

	// Read the backup database and WAL.
	database, err := fs.ReadFile(backupPath)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "cannot read backup content at %s", backupPath)
	}

	wal, err := fs.ReadFile(backupPath + "-wal")
	if err != nil {
		return nil, nil, err
	}

	return database, wal, nil
}

// Restore the given database and WAL backups, writing them at the given
// database path in a volatile file system.
func Restore(fs *sqlite3.VolatileFileSystem, path string, database []byte, wal []byte) error {
	if err := fs.Remove(path); err != nil && !isDeleteNoEntErr(err) {
		return errors.Wrap(err, "failed to remove current database")
	}
	if err := fs.CreateFile(path, database); err != nil {
		return errors.Wrapf(err, "failed to write database content at %s", path)
	}

	if err := fs.Remove(path + "-wal"); err != nil && !isDeleteNoEntErr(err) {
		return errors.Wrap(err, "failed to remove current WAL")
	}
	if err := fs.CreateFile(path+"-wal", wal); err != nil {
		return errors.Wrapf(err, "failed to write wal content at %s", path)
	}

	return nil
}

// Return the path to a temporary file that will be used to write the backup of
// the database being snapshotted.
//
// The temporary file lives in the same directory as the database being
// snapshotted and its named after its filename.
func newBackupPath(path string) string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%s-%x-%x-%x", path, b[0:4], b[4:6], b[6:8])
}

func isDeleteNoEntErr(err error) bool {
	sqliteErr, ok := err.(sqlite3.Error)
	if !ok {
		return false
	}
	return sqliteErr.ExtendedCode == sqlite3.ErrIoErrDeleteNoent
}
