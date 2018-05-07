package connection

import (
	"fmt"
	"net/url"
	"strings"
)

// ParseURI parses the given sqlite3 URI checking if it's compatible with
// dqlite.
//
// Only pure file names without any directory segment are accepted
// (e.g. "test.db"). Query parameters are always valid except for
// "mode=memory" and "vfs".
//
// It returns the filename and query parameters.
func ParseURI(uri string) (string, string, error) {
	filename := uri
	query := ""

	pos := strings.IndexRune(uri, '?')
	if pos >= 1 {
		params, err := url.ParseQuery(uri[pos+1:])
		if err != nil {
			return "", "", err
		}
		if params.Get("mode") == "memory" {
			return "", "", fmt.Errorf("can't replicate a memory database")
		}
		if params.Get("vfs") != "" {
			return "", "", fmt.Errorf("vfs parameter not supported")
		}
		query = params.Encode()
		filename = filename[:pos]
	}

	if strings.HasPrefix(filename, "file:") {
		filename = filename[len("file:"):]
	}

	if filename == ":memory:" {
		return "", "", fmt.Errorf("can't replicate a memory database")
	}

	if strings.IndexRune(filename, '/') >= 0 {
		return "", "", fmt.Errorf("directory segments are invalid")
	}

	return filename, query, nil
}

// EncodeURI concatenates the given filename, VFS and query returning the
// full URI.
func EncodeURI(filename, vfs, query string) string {
	uri := fmt.Sprintf("file:%s?vfs=%s", filename, vfs)
	if query != "" {
		uri += "&" + query
	}
	return uri
}
