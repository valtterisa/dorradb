// Package cli implements the dorradb command-line tool (file ops, no network).
package cli

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"dorradb"
)

const (
	ExitOK    = 0
	ExitError = 1

	defaultDBPath = "app.dorradb"
)

// Run parses flags/subcommand and returns a process exit code.
// No args → opens interactive shell on ./app.dorradb.
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
	case "exec", "query", "status", "shell":
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
	case "exec":
		return cmdExec(db, cmdArgs)
	case "query":
		return cmdQuery(db, cmdArgs, jsonOut)
	case "status":
		return cmdStatus(db, jsonOut)
	case "shell":
		return cmdShell(db, dbPath)
	}
	return ExitError
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `dorradb — embedded SQL CLI

  go run ./cmd/dorradb              interactive shell (./app.dorradb)
  go run ./cmd/dorradb --db my.db   shell on another file

Inside the shell:
  exec CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)
  exec INSERT INTO t (id, name) VALUES (1, 'Ada')
  query SELECT name FROM t WHERE id = 1
  stats
  quit

One-shot:
  go run ./cmd/dorradb exec "INSERT INTO t (id, name) VALUES (2, 'Bob')"
  go run ./cmd/dorradb query "SELECT * FROM t"
`)
}

func cmdExec(db *dorradb.DB, args []string) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: dorradb exec <sql>")
		return ExitError
	}
	sql := strings.Join(args, " ")
	res, err := db.Exec(sql)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitError
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		fmt.Printf("rows_affected=%d\n", n)
	}
	return ExitOK
}

func cmdQuery(db *dorradb.DB, args []string, jsonOut bool) int {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: dorradb query <sql>")
		return ExitError
	}
	sql := strings.Join(args, " ")
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitError
	}
	defer rows.Close()
	cols := rows.Columns()
	if jsonOut {
		var out []map[string]any
		for rows.Next() {
			dest := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range dest {
				ptrs[i] = &dest[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitError
			}
			m := make(map[string]any, len(cols))
			for i, c := range cols {
				m[c] = dest[i]
			}
			out = append(out, m)
		}
		_ = json.NewEncoder(os.Stdout).Encode(out)
		return ExitOK
	}
	if len(cols) > 0 {
		fmt.Println(strings.Join(cols, "\t"))
	}
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitError
		}
		parts := make([]string, len(dest))
		for i, v := range dest {
			parts[i] = fmt.Sprint(v)
		}
		fmt.Println(strings.Join(parts, "\t"))
	}
	return ExitOK
}

func cmdStatus(db *dorradb.DB, jsonOut bool) int {
	s := db.Stats()
	if jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(s)
		return ExitOK
	}
	fmt.Printf("path=%s\n", s.Path)
	fmt.Printf("sequence=%d\n", s.Sequence)
	fmt.Printf("committed_sequence=%d\n", s.CommittedSequence)
	fmt.Printf("pinned_readers=%d\n", s.PinnedReaders)
	fmt.Printf("live_keys=%d\n", s.LiveKeys)
	fmt.Printf("main_bytes=%d\n", s.MainBytes)
	fmt.Printf("wal_bytes=%d\n", s.WALBytes)
	fmt.Printf("approx_dead_bytes=%d\n", s.ApproxDeadBytes)
	fmt.Printf("cache_pages=%d\n", s.CachePages)
	fmt.Printf("dirty_frames=%d\n", s.DirtyFrames)
	return ExitOK
}

func cmdShell(db *dorradb.DB, dbPath string) int {
	in := bufio.NewScanner(os.Stdin)
	fmt.Fprintf(os.Stderr, "Opened %s\n", dbPath)
	fmt.Fprintln(os.Stderr, `Type help for commands. Examples:
  exec CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)
  query SELECT * FROM t
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
			fmt.Fprintln(os.Stderr, `  exec <sql>    run INSERT/UPDATE/DELETE/CREATE
  query <sql>   run SELECT
  stats         info
  quit`)
		case "exec":
			if len(parts) < 2 {
				fmt.Fprintln(os.Stderr, "usage: exec <sql>")
				continue
			}
			sql := strings.TrimSpace(line[len(parts[0]):])
			res, err := db.Exec(sql)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			n, _ := res.RowsAffected()
			fmt.Printf("ok rows_affected=%d\n", n)
		case "query", "select":
			sql := line
			if cmd != "select" {
				if len(parts) < 2 {
					fmt.Fprintln(os.Stderr, "usage: query <sql>")
					continue
				}
				sql = strings.TrimSpace(line[len(parts[0]):])
			}
			rows, err := db.Query(sql)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			cols := rows.Columns()
			if len(cols) > 0 {
				fmt.Println(strings.Join(cols, "\t"))
			}
			for rows.Next() {
				dest := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range dest {
					ptrs[i] = &dest[i]
				}
				if err := rows.Scan(ptrs...); err != nil {
					fmt.Fprintf(os.Stderr, "error: %v\n", err)
					break
				}
				parts := make([]string, len(dest))
				for i, v := range dest {
					parts[i] = fmt.Sprint(v)
				}
				fmt.Println(strings.Join(parts, "\t"))
			}
			_ = rows.Close()
		case "stats", "status":
			s := db.Stats()
			fmt.Printf("keys=%d  sequence=%d  main=%d bytes  wal=%d bytes\n", s.LiveKeys, s.Sequence, s.MainBytes, s.WALBytes)
		default:
			fmt.Fprintf(os.Stderr, "unknown %q — type help\n", cmd)
		}
	}
	return ExitOK
}
