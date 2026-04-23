package main

import (
	"fmt"
	"os"
)

const version = "0.1.0-dev"

func main() {
	fmt.Fprintf(os.Stderr, "depthofbook-bot %s starting...\n", version)
}
