package e2e

import (
	"os"
	"path/filepath"
)

// mustReadFile reads a file relative to the package and panics on error. It is
// used by helpers that do not carry a *testing.T.
func mustReadFile(rel string) string {
	data, err := os.ReadFile(rel)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// readSample reads a sample feed by name, anchoring the path at the package
// directory.
func readSample(t interface{ Helper() }, name string) string {
	// samples live at <repo>/samples; test/e2e is at <repo>/test/e2e.
	dir := filepath.Join("..", "..", "samples")
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		panic(err)
	}
	return string(data)
}
