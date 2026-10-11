package main

import (
	"os"

	"dorradb/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:]))
}
