// Package cli implements the dorradb command-line tool (file ops, no network).
package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"dorradb"
)

const (
	ExitOK       = 0
	ExitError    = 1
	ExitNotFound = 2 // get: key missing

	defaultDBPath = "app.dorradb"
)

// Run parses flags/subcommand and returns a process exit code.
// No args → opens interactive shell on ./app.dorradb (easiest path).
func Run(args []string) int {
	var dbPath string
	var jsonOut bool
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--db" && i+1 < len(args):
			i++
			dbPath = args[i]
		case strings.HasPrefix(a, "--db="):
			dbPath = strings.TrimPrefix(a, "--db=")
		case a == "--json":
			jsonOut = true
		case a == "-v" || a == "--verbose":
			// reserved
		case a == "-h" || a == "--help" || a == "help":
			printUsage()
			return ExitOK
		default:
			rest = append(rest, a)
		}
	}
	if dbPath == "" {
		dbPath = os.Getenv("DORRADB_PATH")
	}
	if dbPath == "" {
		dbPath = defaultDBPath
	}

	// Bare invocation: go run ./cmd/dorradb  →  shell
	cmd := "shell"
	cmdArgs := []string(nil)
	if len(rest) > 0 {
		cmd = rest[0]
		cmdArgs = rest[1:]
	}

	if cmd == "version" {
		fmt.Println("dorradb 0.1.0")
		return ExitOK
	}

	switch cmd {
	case "set", "get", "delete", "status", "shell":
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q — try: go run ./cmd/dorradb\n", cmd)
		printUsage()
		return ExitError
	}

	db, err := dorradb.Open(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitError
	}
	defer db.Close()

	switch cmd {
	case "set":
		return cmdSet(db, cmdArgs)
	case "get":
		return cmdGet(db, cmdArgs, jsonOut)
	case "delete":
		return cmdDelete(db, cmdArgs)
	case "status":
		return cmdStatus(db, jsonOut)
	case "shell":
		return cmdShell(db, dbPath)
	}
	return ExitError
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `dorradb — embedded KV CLI

  go run ./cmd/dorradb              interactive shell (uses ./app.dorradb)
  go run ./cmd/dorradb --db my.db   shell on another file

Inside the shell (lowercase is fine):
  set name Ada
  get name
  del name
  stats
  help
  quit

One-shot (optional):
  go run ./cmd/dorradb get name
  go run ./cmd/dorradb set name Ada
`)
}

func cmdSet(db *dorradb.DB, args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: dorradb set <key> <value|->")
		return ExitError
	}
	key := args[0]
	var val []byte
	if args[1] == "-" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitError
		}
		val = b
	} else {
		val = []byte(strings.Join(args[1:], " "))
	}
	if err := db.Set([]byte(key), val); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitError
	}
	return ExitOK
}

func cmdGet(db *dorradb.DB, args []string, jsonOut bool) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: dorradb get <key>")
		return ExitError
	}
	val, err := db.Get([]byte(args[0]))
	if errors.Is(err, dorradb.ErrNotFound) {
		return ExitNotFound
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitError
	}
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		_ = enc.Encode(map[string]string{"value": string(val)})
		return ExitOK
	}
	os.Stdout.Write(val)
	if len(val) == 0 || val[len(val)-1] != '\n' {
		fmt.Println()
	}
	return ExitOK
}

func cmdDelete(db *dorradb.DB, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: dorradb delete <key>")
		return ExitError
	}
	if err := db.Delete([]byte(args[0])); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitError
	}
	return ExitOK
}

func cmdStatus(db *dorradb.DB, jsonOut bool) int {
	s := db.Stats()
	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		_ = enc.Encode(s)
		return ExitOK
	}
	fmt.Printf("path=%s\n", s.Path)
	fmt.Printf("seq=%d\n", s.Seq)
	fmt.Printf("live_keys=%d\n", s.LiveKeys)
	fmt.Printf("main_bytes=%d\n", s.MainBytes)
	fmt.Printf("wal_bytes=%d\n", s.WALBytes)
	fmt.Printf("approx_dead_bytes=%d\n", s.ApproxDeadBytes)
	return ExitOK
}

func cmdShell(db *dorradb.DB, dbPath string) int {
	in := bufio.NewScanner(os.Stdin)
	fmt.Fprintf(os.Stderr, "Opened %s\n", dbPath)
	fmt.Fprintln(os.Stderr, `Type help for commands. Examples:
  set name Ada
  get name
  del name
  quit`)
	for {
		fmt.Fprint(os.Stderr, "dorradb> ")
		if !in.Scan() {
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])
		switch cmd {
		case "q", "quit", "exit":
			return ExitOK
		case "h", "help", "?":
			fmt.Fprintln(os.Stderr, `  set <key> <value>   save
  get <key>           read
  del <key>           delete
  stats               info
  quit`)
		case "get":
			if len(parts) < 2 {
				fmt.Fprintln(os.Stderr, "usage: get <key>")
				continue
			}
			val, err := db.Get([]byte(parts[1]))
			if errors.Is(err, dorradb.ErrNotFound) {
				fmt.Println("(missing)")
				continue
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			fmt.Println(string(val))
		case "set":
			if len(parts) < 3 {
				fmt.Fprintln(os.Stderr, "usage: set <key> <value>")
				continue
			}
			key := parts[1]
			val := strings.Join(parts[2:], " ")
			if err := db.Set([]byte(key), []byte(val)); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			fmt.Println("saved")
		case "del", "delete", "rm":
			if len(parts) < 2 {
				fmt.Fprintln(os.Stderr, "usage: del <key>")
				continue
			}
			if err := db.Delete([]byte(parts[1])); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			fmt.Println("deleted")
		case "stats", "status":
			s := db.Stats()
			fmt.Printf("keys=%d  seq=%d  main=%d bytes  wal=%d bytes\n", s.LiveKeys, s.Seq, s.MainBytes, s.WALBytes)
		default:
			fmt.Fprintf(os.Stderr, "unknown %q — type help\n", cmd)
		}
	}
	return ExitOK
}
