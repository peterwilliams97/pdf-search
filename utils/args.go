package utils

import (
	"flag"
	"fmt"
	"os"
)

// MakeUsage updates flag.Usage to include usage message `msg`.
func MakeUsage(msg string) {
	usage := flag.Usage
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, msg)
		usage()
	}
}
