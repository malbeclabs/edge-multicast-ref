package main

import (
	"fmt"
	"os"
)

const version = "0.1.0-dev"

func main() {
	fmt.Fprintf(os.Stderr, "depthofbook-parser %s starting...\n", version)
}
