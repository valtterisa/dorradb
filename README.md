# DorraDB

Embedded key-value store for Go on **Linux, macOS, and Windows**. DorraDB provides **ACID transactions**. One process opens a path; rows live in a **clustered B+ tree** on **4 KiB pages** (leaf cells hold versioned key/value data). Mutations are group-committed through a WAL, then applied to dirty tree pages (flushed on checkpoint).

**Outside pull requests are not accepted.** Open an [issue](https://github.com/valtterisa/dorradb/issues) if something is wrong, unclear, or broken — reports and discussion are welcome. Changes land from the maintainer only.

Licensed under [MIT](LICENSE).

OS I/O goes through an internal VFS (stdlib + [`gofrs/flock`](https://github.com/gofrs/flock)). Engine code only sees `vfs.FS`.

```text
app.dorradb         # main database (4 KiB pages; table B+ tree + overflow)
app.dorradb-wal     # write-ahead log
app.dorradb-journal # page redo journal, exists only during a page flush
app.dorradb.lock    # exclusive advisory lock
```

## Goals

### Engine

- [x] One writer at a time, WAL + checkpoint, exclusive file lock
- [x] Version numbers (`sequence`) on every record
- [x] **MVCC + multi-reader** (SQLite WAL-style) — many readers + one writer; snapshot at `Begin` (AUTOCOMMIT ops are one-shot txs); readers don’t wait on the writer goroutine (same process)
- [x] **ACID Transaction API** — every op is a tx (AUTOCOMMIT `Get`/`Set`, or `Begin` / `Commit` / `Rollback`); snapshot isolation; single writer; writes buffered until commit

### Reliability

- [ ] Crash/recovery tests, concurrent stress, fuzz, durability docs, format migrations, backup, corruption/verify — treat as one reliability bar for production, not separate engine features

### Later (SQL)

- [x] **Clustered B+ tree** on the buffer pool (primary table; ordered keys / range scans)
- [ ] **Query planner**
- [ ] **Query engine** — execute plans against the store
- [ ] **SQL** — parser + bind to the planner/engine

## Embed

```go
db, err := dorradb.Open("app.dorradb")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

err = db.Set([]byte("user:1"), []byte(`{"name":"Ada"}`))
val, err := db.Get([]byte("user:1"))
ok, err := db.Has([]byte("user:1"))
err = db.Delete([]byte("user:1"))
stats := db.Stats()

tx, err := db.Begin(true)
defer tx.Rollback()
err = tx.Set([]byte("a"), []byte("1"))
err = tx.Set([]byte("b"), []byte("2"))
err = tx.Commit()
```

`Open` creates the database if missing. Only one process may hold the path (`ErrLocked` otherwise). The OS releases the lock on exit or crash.

All `DB` methods are safe for concurrent use. Every read and write runs inside a
transaction (AUTOCOMMIT for single `Get`/`Set`/`Delete`/`Has`, or explicit
`Begin` / `Commit` / `Rollback`). Do not use one `Tx` from multiple goroutines.

| Method | Notes |
| --- | --- |
| `Get` | AUTOCOMMIT read tx; copy of value, or `ErrNotFound` |
| `Set` | AUTOCOMMIT write tx; durable and visible after return when WAL sync is on (default) |
| `Delete` | AUTOCOMMIT write tx; missing keys are not an error |
| `Has` | AUTOCOMMIT read tx; existence without copying the value |
| `Begin` | Start a tx (`writable`); then `Get`/`Set`/`Delete`/`Has` on `Tx` |
| `Tx.Commit` / `Tx.Rollback` | Apply or discard buffered writes |
| `Stats` | Path, sequence, committed sequence, pinned readers, live keys, file sizes, approx dead bytes, cache pages, dirty frames |
| `Close` | Flush, unlock, close files (waits for open tx pins) |

### Transactions (ACID)

DorraDB provides **ACID transactions**:

- **Atomic** — all ops in a tx commit together or not at all; rollback discards
- **Consistent** — no torn published state; the app owns higher-level invariants (KV has no schema)
- **Isolated** — snapshot reads at `Begin`; one writable tx at a time (like SQLite WAL)
- **Durable** — WAL fsync on commit unless `DisableSync`

Writes are buffered until `Commit`. Open pins defer checkpoints — don’t hold transactions open longer than needed.

### Options

Pass an optional `Options` to tune durability and checkpoints. Zero fields keep `DefaultOptions`:

```go
db, err := dorradb.Open("app.dorradb", dorradb.Options{
    WALCheckpointBytes:    32 << 20,
    WALCheckpointInterval: 60 * time.Second,
    DisableSync:           true, // skip WAL fsync (faster, less durable)
    GroupCommitMaxOps:     128,
    GroupCommitMaxWait:    5 * time.Millisecond,
    CachePages:            512,
    VacuumDeadBytes:       16 << 20,
})
```

| Field | Default | Meaning |
| --- | --- | --- |
| `WALCheckpointBytes` | 8 MiB | Checkpoint when WAL growth reaches this size |
| `WALCheckpointInterval` | 30s | Checkpoint after this long with mutations |
| `DisableSync` | `false` | If `true`, skip WAL fsync on group commit |
| `GroupCommitMaxOps` | 64 | Max mutations per WAL sync |
| `GroupCommitMaxWait` | 2ms | Max wait to coalesce writers before sync |
| `CachePages` | 256 | Fixed buffer-pool frames (4 KiB each); min 2 |
| `VacuumDeadBytes` | 8 MiB | Auto-vacuum after checkpoint when dead bytes reach this; `-1` disables |

The buffer pool is fixed-size (`CachePages`); the main file grows in 256 KiB chunks. The primary store is a clustered B+ tree (`TableRoot` in the meta header).

### Durability

1. Mutations are **group-committed** to the WAL and fsynced (unless `DisableSync`).
2. Then applied to **dirty B+ tree / overflow pages**; then `committedSequence` is published and the call returns.
3. On open after a crash: replay a complete `-journal` if one exists (a torn one is discarded), load the table root, then replay WAL records with sequence greater than the main file’s flushed sequence.

The buffer pool is **no-steal**: dirty pages are never evicted, and the meta header is only written as part of a flush. Every flush is atomic: all dirty pages plus the meta page (recording the applied sequence and `TableRoot`) go to the `-journal` file first, which is fsynced, then into the main file, which is fsynced, and then the journal is removed. So the main file always holds one complete, consistent tree, and the WAL covers everything after it. When the cache runs low, the tree flushes on its own between operations, so `CachePages` limits memory, not write volume.

Checkpoints flush and then truncate the WAL. By default checkpoints run at 8 MiB of WAL growth or after 30s with mutations.

Updates and deletes add MVCC versions inside leaf cells. On every write, versions no reader can see anymore (older than the oldest pinned snapshot) are dropped from that cell. Large values spill to overflow pages. **Vacuum** (after checkpoint when `ApproxDeadBytes` exceeds `VacuumDeadBytes`) prunes the whole tree and drops tombstoned keys.

Limits: a single key can hold only around 60–100 versions that open snapshots still need (`ErrTooBig` after that), so long-lived transactions on a hot key will eventually fail writes. Underfull leaves are not merged. Overflow pages written just before a crash can leak (unused space, not corruption).

### Errors

Check with `errors.Is`:

| Error | When |
| --- | --- |
| `ErrNotFound` | `Get` on a missing key |
| `ErrInvalidKey` | Empty key |
| `ErrTooLarge` | Key > 1 KiB or value > 4 MiB |
| `ErrLocked` | Another process holds the advisory lock |
| `ErrClosed` | Use after `Close` |
| `ErrTxClosed` | Use of a tx after `Commit` / `Rollback` |
| `ErrTxNotWritable` | `Set` / `Delete` on a read-only tx |
| `ErrCorrupt` | Bad magic, mid-file CRC failure, etc. |
| `ErrUnsupportedVersion` | On-disk `formatVersion` newer than this build |

### Limits

- Key: 1–1024 bytes (empty keys rejected)
- Value: up to 4 MiB

## CLI

Interactive shell on `./app.dorradb`:

```bash
go run ./cmd/dorradb
```

```text
dorradb> set name Ada
saved
dorradb> get name
Ada
dorradb> del name
deleted
dorradb> stats
dorradb> quit
```

Path: `--db PATH`, `--db=PATH`, or env `DORRADB_PATH` (default `./app.dorradb`).

One-shot:

```bash
go run ./cmd/dorradb set name Ada
go run ./cmd/dorradb get name
go run ./cmd/dorradb delete name
go run ./cmd/dorradb status
go run ./cmd/dorradb set blob -   # value from stdin
```

`--json` formats `get` / `status` as JSON. `version` prints `dorradb 0.1.0`.

Exit codes: `0` ok, `1` error, `2` key not found (`get`).

## On-disk format

**`formatVersion = 3`** (v1/v2 files are not readable).

**Main file:** fixed **4096-byte pages**. Page 0 is meta (64-byte header + marker): magic `DORA` (`0x41524F44`), file type (main=1), sequence, WAL checkpoint offset, page size, page count, freelist head, **table root**, header CRC. Data pages: clustered **B+ tree** leaf/internal nodes; large values use overflow page chains. Page ids are implicit (`offset = pageID × 4096`); the buffer pool keeps a `pageID → frame` directory for cached pages only.

**WAL:** little-endian frames with IEEE CRC32 — length-prefixed payload + CRC. Ops: set (1), delete (2). Torn last record → crash tail; mid-file CRC → `ErrCorrupt`. Newer format versions refuse to open (`ErrUnsupportedVersion`).

## Contributing

Outside PRs are not accepted. Issues, forks, and self-hosting are fine — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE) — Copyright (c) 2026 Valtteri Savonen.
