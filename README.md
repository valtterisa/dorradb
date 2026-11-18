# DorraDB

DorraDB is an embedded relational store for Go (Linux, macOS, Windows). It is meant to be a simple, durable library you include in your process — not a separate database server.

Use basic SQL (`CREATE TABLE` / `INSERT` / `UPDATE` / `DELETE` / `SELECT`). Under the hood: row records in slotted B+ leaf cells, one clustered tree per table, a write-ahead log, and SQLite-WAL-style MVCC (many readers, one writer).

**Outside pull requests are not accepted.** Open an [issue](https://github.com/valtterisa/dorradb/issues) if something is wrong or unclear. Changes land from the maintainer only.

Licensed under [MIT](LICENSE).

## Table of Contents

- [Getting Started](#getting-started)
  - [Opening a database](#opening-a-database)
  - [SQL](#sql)
  - [Transactions](#transactions)
  - [Options](#options)
  - [Statistics](#statistics)
- [How DorraDB works](#how-dorradb-works)
  - [Clustered B+ tree](#clustered-b-tree)
  - [WAL, buffer pool, and checkpoints](#wal-buffer-pool-and-checkpoints)
  - [On-disk format](#on-disk-format)
  - [Reading the source](#reading-the-source)
- [Caveats & Limitations](#caveats--limitations)
- [Benchmarks](#benchmarks)
- [CLI](#cli)
- [Goals](#goals)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

### Opening a database

The top-level object is a `DB`. It is one path on disk plus a few sidecar files. `Open` creates the database if it does not exist.

```go
package main

import (
	"log"

	"dorradb"
)

func main() {
	db, err := dorradb.Open("app.dorradb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	_, err = db.Exec(`INSERT INTO users (id, name) VALUES (?, ?)`, int64(1), "Ada")
}
```

Only one process may open a path at a time. A second process gets `ErrLocked` immediately (the OS also releases the lock if the holder crashes). All `DB` methods are safe for concurrent use from multiple goroutines. A single `Tx` is not — use one transaction per goroutine, or protect it yourself.

Files created next to the main path:

```text
app.dorradb         # main database (4 KiB pages; B+ tree + overflow)
app.dorradb-wal     # write-ahead log
app.dorradb-journal # page redo journal (only while a flush is in progress)
app.dorradb.lock    # exclusive advisory lock
```

OS I/O goes through an internal VFS (stdlib + [`gofrs/flock`](https://github.com/gofrs/flock)). Engine code only sees `vfs.FS`.

### SQL

Each table is its own clustered B+ tree; cell payloads are typed row records (INTEGER / TEXT / BLOB). WHERE is equality on the primary key only (for now).

```go
_, err = db.Exec(`CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  data BLOB
)`)
_, err = db.Exec(`INSERT INTO users (id, name, data) VALUES (?, ?, ?)`,
	int64(1), "alice", []byte{1, 2})
rows, err := db.Query(`SELECT name FROM users WHERE id = ?`, int64(1))
defer rows.Close()
for rows.Next() {
	var name string
	_ = rows.Scan(&name)
}
```

| Method | Notes |
| --- | --- |
| `Exec` | AUTOCOMMIT write (`INSERT` / `UPDATE` / `DELETE` / `CREATE` / `DROP`) |
| `Query` | AUTOCOMMIT read (`SELECT`); caller must `Close` rows |
| `Prepare` | Parse once; then `Stmt.Exec` / `Stmt.Query` |
| `Begin` | Explicit tx; then `Tx.Exec` / `Tx.Query` |
| `Tx.Commit` / `Tx.Rollback` | Apply or discard buffered writes |
| `Stats` / `Close` | Sizes / flush and unlock |

Also: `database/sql` driver registered as `"dorradb"`. Unsupported syntax returns a clear parse error.

### Transactions

One writable transaction at a time; many concurrent readers. Each tx sees a snapshot as of `Begin` (plus its own buffered writes).

- `DB.Exec` / `DB.Query` use short AUTOCOMMIT transactions.
- `DB.Begin` starts an explicit transaction you commit or roll back.

```go
tx, err := db.Begin(true)
if err != nil {
	return err
}
defer tx.Rollback()
_, err = tx.Exec(`INSERT INTO users (id, name) VALUES (?, ?)`, int64(2), "Bob")
_, err = tx.Exec(`INSERT INTO users (id, name) VALUES (?, ?)`, int64(3), "Cyd")
return tx.Commit()
```

| Property | Behavior |
| --- | --- |
| Atomic | All buffered ops in a tx commit together, or none do |
| Consistent | Schema + typed rows; constraint errors abort the statement |
| Isolated | Snapshot at `Begin`; one writer at a time (like SQLite WAL) |
| Durable | WAL fsync on commit unless `DisableSync` |

### Options

Pass an optional `Options` to tune durability and checkpoints. Zero fields keep `DefaultOptions`:

```go
db, err := dorradb.Open("app.dorradb", dorradb.Options{
	WALCheckpointBytes:    32 << 20,
	WALCheckpointInterval: 60 * time.Second,
	DisableSync:           true, // skip WAL fsync (faster, less durable)
	CachePages:            512,
	VacuumDeadBytes:       16 << 20,
})
```

| Field | Default | Meaning |
| --- | --- | --- |
| `WALCheckpointBytes` | 8 MiB | Checkpoint when WAL growth reaches this size |
| `WALCheckpointInterval` | 30s | Checkpoint after this long with mutations |
| `DisableSync` | `false` | If `true`, skip WAL fsync on commit |
| `CachePages` | 256 | Fixed buffer-pool frames (4 KiB each); min 2 |
| `VacuumDeadBytes` | 8 MiB | Auto-vacuum after checkpoint when dead bytes reach this; `-1` disables |

### Statistics

`DB.Stats()` returns a snapshot useful for ops and debugging (also exposed by the CLI `status` / `stats` commands):

```go
s := db.Stats()
fmt.Printf("keys=%d wal=%d main=%d pins=%d\n",
	s.LiveKeys, s.WALBytes, s.MainBytes, s.PinnedReaders)
```

Fields include path, sequence / committed sequence, pinned readers, live keys, main and WAL file sizes, approximate dead bytes, cache page count, and dirty frames.

### Errors

Check with `errors.Is`:

| Error | When |
| --- | --- |
| `ErrNotFound` | Missing row / key in engine lookups |
| `ErrTableNotFound` / `ErrTableExists` | Bad / duplicate `CREATE` / `DROP` |
| `ErrConstraint` | e.g. duplicate PK on plain `INSERT` |
| `ErrTooLarge` | Row payload > 4 MiB |
| `ErrLocked` | Another process holds the advisory lock |
| `ErrClosed` | Use after `Close` |
| `ErrTxClosed` | Use of a tx after `Commit` / `Rollback` |
| `ErrTxNotWritable` | Write on a read-only tx |
| `ErrCorrupt` | Bad magic, mid-file CRC failure, etc. |
| `ErrUnsupportedVersion` | On-disk `formatVersion` newer than this build |

## How DorraDB works

Think of three layers: SQL, the write-ahead log, and **clustered B+ trees** (one per table) on the shared buffer pool.

### Clustered B+ tree

Each SQL table is a clustered B+ tree. The bootstrap tree (header `TableRoot`) holds the catalog. “Clustered” means leaf pages *are* the table: each leaf cell stores the primary key plus a typed row-record payload (MVCC version chain), not a pointer into a separate heap.

```text
                    [internal]
                   /          \
            [internal]      [internal]
             /    \          /    \
         [leaf] [leaf]   [leaf] [leaf]  ← keys + MVCC versions (or overflow refs)
            \_____/         \_____/
           right-sibling links (leaf chain)
```

**Page layout (4 KiB):**

| Kind | Role |
| --- | --- |
| **Internal** | Separator keys + child page ids. Search descends `root → … → leaf`. |
| **Leaf** | Sorted cells. Each cell is one user key plus a **newest-first version chain**. Leaves are linked by a right-sibling pointer for ordered scans later. |
| **Overflow** | Value bytes that do not fit inline in a leaf cell (chunked page chain). |

A leaf page uses a classic slotted layout: a fixed 16-byte header (type, key count, lower/upper freelist bounds, right sibling / left child), cell payloads growing up from the header, and a slot directory growing down from the end of the page. Binary search walks the slot directory by key.

**What a leaf cell holds:**

```text
key | version₀ (newest) | version₁ | … | versionₙ
      sequence, flags (live / deleted / overflow),
      inline bytes  —or—  overflow page id + total length
```

Reads at snapshot `S` walk the chain and take the newest version with `sequence ≤ S` that is not a delete tombstone. Writes prepend a new version (or a delete flag). After each write, versions older than the oldest pinned reader are pruned from that cell. A cell that grows past half a page of footprint (too many live versions on one hot key) fails with `ErrTooBig`.

**Splits:** when a leaf (or internal) no longer fits, DorraDB splits by bytes into two pages and **promotes** the separator (smallest key on the right) up the ancestor path, creating a new root when the old root splits. Underfull leaves are **not** merged on delete today; space comes back via freelist reuse and vacuum.

**Concurrency inside the tree:** one writer at a time for `Put` / `Delete` / `Prune`; readers share a read lock so they never observe a page mid-rewrite. The buffer pool does not steal dirty frames — the tree flushes only when the whole structure is consistent (before a mutation that needs free frames, or between leaves during prune).

**Large values:** if the value does not fit inline with the cell’s other versions, it is written to overflow pages and the leaf keeps only `(OverflowPage, ValueLen)`.

**Vacuum** walks the tree after checkpoint when approximate dead bytes exceed `VacuumDeadBytes`, drops versions no snapshot needs, and removes fully tombstoned keys.

### WAL, buffer pool, and checkpoints

1. **Mutations** in a commit are appended to the WAL and fsynced once (unless `DisableSync`).
2. They are then applied to **dirty B+ tree / overflow pages** in the buffer pool; `committedSequence` is published and the call returns.
3. Later, a **checkpoint** flushes dirty pages to the main file and truncates the WAL (by default at 8 MiB growth or after 30s with mutations).

**Crash recovery on open:** replay a complete `-journal` if one exists (a torn one is discarded), load the table root from the meta page, then replay WAL records with sequence greater than the main file’s flushed sequence.

**Buffer pool:** fixed size (`CachePages`). **No-steal** — dirty pages are never evicted without a flush. The meta header is only written as part of a flush. Every flush is atomic: dirty pages plus the meta page (sequence + `TableRoot`) go to the `-journal` first (fsync), then into the main file (fsync), then the journal is removed. So the main file always holds one complete tree; the WAL covers everything after it. When the cache runs low, the tree flushes between operations — `CachePages` limits memory, not write volume. The main file grows in 256 KiB chunks.

### On-disk format

**`formatVersion = 3`** (v1/v2 files are not readable).

**Main file:** fixed **4096-byte pages**. Page 0 is meta (64-byte header + marker): magic `DORA` (`0x41524F44`), file type (main=1), sequence, WAL checkpoint offset, page size, page count, freelist head, **table root**, header CRC. Data pages are clustered **B+ tree** leaf/internal nodes; large values use overflow page chains. Page ids are implicit (`offset = pageID × 4096`).

**WAL:** little-endian frames with IEEE CRC32 — length-prefixed payload + CRC. Ops: set (1), delete (2). A torn last record is a crash tail; a mid-file CRC failure is `ErrCorrupt`. Newer format versions refuse to open (`ErrUnsupportedVersion`).

### Reading the source

Useful entry points if you want to see how the pieces connect:

- `Open` / `DB` / `Tx` — public API (`db.go`, `tx.go`)
- `internal/engine` — WAL apply, pins, checkpoint, vacuum orchestration
- `internal/btree` — clustered B+ tree, splits, versioned leaf cells
- `internal/storage` — buffer pool, pager, journal flush
- `internal/wal` — append-only log frames
- `internal/vfs` — OS file + lock abstraction

## Caveats & Limitations

- **One process per path.** Exclusive lock; no multi-process sharing.
- **One writer.** Concurrent writable `Begin` / `Exec` serialize. Readers run in parallel and do not wait on the writer goroutine (same process).
- **Keep transactions short.** Long-lived read pins block checkpoints and keep old versions around. A single hot key can only retain around 60–100 versions still needed by open snapshots (`ErrTooBig` after that), so long readers on a busy key will eventually fail writers.
- **SQL dialect is small.** PK equality `WHERE` only; no joins, secondary indexes, or rich expressions yet.
- **Underfull leaves are not merged.** Deletes leave space until vacuum / reuse patterns catch up.
- **Overflow pages** written just before a crash can leak as unused space (not corruption).
- **Reliability bar for production** (crash/recovery stress, fuzz, backup, verify, format migrations) is still in progress — see Goals.

## Benchmarks

**1:1** vs **SQLite WAL** (`modernc.org/sqlite`, pure Go): matched `cache_size` ≈ `CachePages×4KiB`, `mmap_size=0`, sync aligned. No handicap budget — who is faster wins.

```bash
go run ./cmd/bench           # writes + reads
go run ./cmd/bench writes
go run ./cmd/bench reads
```

Details: [`bench/`](bench/).

## CLI

Interactive shell on `./app.dorradb`:

```bash
go run ./cmd/dorradb
```

```text
dorradb> exec CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL)
dorradb> exec INSERT INTO t (id, name) VALUES (1, 'Ada')
dorradb> query SELECT name FROM t WHERE id = 1
dorradb> stats
dorradb> quit
```

Path: `--db PATH`, `--db=PATH`, or env `DORRADB_PATH` (default `./app.dorradb`).

One-shot:

```bash
go run ./cmd/dorradb exec "INSERT INTO t (id, name) VALUES (2, 'Bob')"
go run ./cmd/dorradb query "SELECT * FROM t"
go run ./cmd/dorradb status
```

`--json` formats `query` / `status` as JSON. `version` prints `dorradb 0.1.0`.

Exit codes: `0` ok, `1` error.

## Goals

### Engine

- [x] One writer at a time, WAL + checkpoint, exclusive file lock
- [x] Version numbers (`sequence`) on every record
- [x] **MVCC + multi-reader** (SQLite WAL-style)
- [x] **ACID Transaction API** — AUTOCOMMIT or `Begin` / `Commit` / `Rollback`; snapshot isolation

### Reliability

- [ ] Crash/recovery tests, concurrent stress, fuzz, durability docs, format migrations, backup, corruption/verify

### SQL

- [x] **Basic SQL** — CREATE / INSERT / UPDATE / DELETE / SELECT; typed rows; multi-table
- [x] **Clustered B+ tree** per table on the buffer pool
- [ ] **Query planner** / richer WHERE / indexes / joins
- [ ] **Query engine**
- [ ] **SQL** — parser + bind to the planner/engine

## Contributing

Outside PRs are not accepted. Issues, forks, and self-hosting are fine — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE) — Copyright (c) 2026 Valtteri Savonen.
