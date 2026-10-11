// Package cli implements the dorradb command-line tool (file ops, no network).
package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"dorradb"
)

const (
	ExitOK       = 0
	ExitError    = 1
	ExitNotFound = 2 // get: key missing
)

// Run parses flags/subcommand and returns a process exit code.
func Run(args []string) int {
	var dbPath string
	var jsonOut bool
	var verbose bool
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
			verbose = true
		default:
			rest = append(rest, a)
		}
	}
	if dbPath == "" {
		dbPath = os.Getenv("DORRADB_PATH")
	}
	if len(rest) == 0 {
		fmt.Fprintln(os.Stderr, "usage: dorradb [--db path] <command> ...")
		return ExitError
	}
	cmd := rest[0]
	cmdArgs := rest[1:]
	_ = verbose

	switch cmd {
	case "version":
		fmt.Println("dorradb 0.1.0")
		return ExitOK
	case "set", "get", "delete", "status", "checkpoint", "shell":
		if dbPath == "" {
			fmt.Fprintln(os.Stderr, "error: --db or DORRADB_PATH required")
			return ExitError
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
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
	case "checkpoint":
		if err := db.Checkpoint(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitError
		}
		return ExitOK
	case "shell":
		return cmdShell(db)
	}
	return ExitError
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
	fmt.Printf("last_checkpoint=%s\n", s.LastCheckpoint.Format(time.RFC3339))
	return ExitOK
}

func cmdShell(db *dorradb.DB) int {
	in := bufio.NewScanner(os.Stdin)
	fmt.Fprintln(os.Stderr, "dorradb shell — GET/SET/DELETE/STATS/CHECKPOINT/QUIT")
	for {
		fmt.Fprint(os.Stderr, "> ")
		if !in.Scan() {
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])
		switch cmd {
		case "QUIT", "EXIT":
			return ExitOK
		case "GET":
			if len(parts) < 2 {
				fmt.Fprintln(os.Stderr, "ERR usage: GET key")
				continue
			}
			val, err := db.Get([]byte(parts[1]))
			if errors.Is(err, dorradb.ErrNotFound) {
				fmt.Println("NIL")
				continue
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERR %v\n", err)
				continue
			}
			fmt.Printf("OK %s\n", string(val))
		case "SET":
			if len(parts) < 3 {
				fmt.Fprintln(os.Stderr, "ERR usage: SET key value")
				continue
			}
			if err := db.Set([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Fprintf(os.Stderr, "ERR %v\n", err)
				continue
			}
			fmt.Println("OK")
		case "DELETE", "DEL":
			if len(parts) < 2 {
				fmt.Fprintln(os.Stderr, "ERR usage: DELETE key")
				continue
			}
			if err := db.Delete([]byte(parts[1])); err != nil {
				fmt.Fprintf(os.Stderr, "ERR %v\n", err)
				continue
			}
			fmt.Println("OK")
		case "STATS":
			s := db.Stats()
			fmt.Printf("OK seq=%d keys=%d main=%d wal=%d dead=%d\n", s.Seq, s.LiveKeys, s.MainBytes, s.WALBytes, s.ApproxDeadBytes)
		case "CHECKPOINT":
			if err := db.Checkpoint(context.Background()); err != nil {
				fmt.Fprintf(os.Stderr, "ERR %v\n", err)
				continue
			}
			fmt.Println("OK")
		default:
			fmt.Fprintln(os.Stderr, "ERR unknown command")
		}
	}
	return ExitOK
}
