package e2e

import "flag"

// boolFlag registers and returns a bool flag. Defined in its own file so the
// flag package import stays in one place.
func boolFlag(name string, def bool, usage string) *bool {
	return flag.Bool(name, def, usage)
}
