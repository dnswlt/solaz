package trace

import (
	"fmt"
	"os"
)

var verbose bool

// SetVerbose enables or disables debug output to stderr.
func SetVerbose(v bool) { verbose = v }

// Debugf prints a line to stderr when verbose mode is on. No timestamp,
// no level prefix — this is a CLI, not a service.
func Debugf(format string, a ...any) {
	if !verbose {
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", a...)
}

// Warningf prints a line to stderr unconditionally, prefixed by "warning: ". No timestamp,
// no level prefix — this is a CLI, not a service.
func Warningf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "warning: "+format+"\n", a...)
}
