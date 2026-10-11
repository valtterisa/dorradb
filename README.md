# DorraDB

Embedded key-value store for Go on **Linux and Windows**. One process opens a path; values live on disk; an in-memory index maps keys to offsets. Mutations are group-committed through a WAL, then applied to the main file.

OS I/O goes through an internal VFS (stdlib + [`gofrs/flock`](https://github.com/gofrs/flock)). Engine code only sees `vfs.FS`.

```text
app.dorradb         # main database (append-only between checkpoints)
app.dorradb-wal     # write-ahead log
app.dorradb.lock    # exclusive advisory lock
```

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
err = db.Checkpoint(context.Background())
stats := db.Stats()
```

`Open` creates the database if missing. Only one process may hold the path (`ErrLocked` otherwise). The OS releases the lock on exit or crash.

All methods are safe for concurrent use; mutations run on a single writer loop.

| Method | Notes |
| --- | --- |
| `Get` | Returns a copy of the value, or `ErrNotFound` |
| `Set` | Create or overwrite; durable after return when WAL sync is on (default) |
| `Delete` | Missing keys are not an error |
| `Has` | Existence check without copying the value |
| `Checkpoint` | Compact live keys into the main file and truncate the WAL |
| `Stats` | Path, seq, live keys, file sizes, approx dead bytes, last checkpoint |
| `Close` | Flush, unlock, close files |

### Options

Pass an optional `Options` to tune durability and checkpoints. Zero fields keep `DefaultOptions`:

```go
db, err := dorradb.Open("app.dorradb", dorradb.Options{
    WALCheckpointBytes:    32 << 20,
    WALCheckpointInterval: 60 * time.Second,
    DisableSync:           true, // skip WAL fsync (faster, less durable)
    GroupCommitMaxOps:     128,
    GroupCommitMaxWait:    5 * time.Millisecond,
})
```

| Field | Default | Meaning |
| --- | --- | --- |
| `WALCheckpointBytes` | 8 MiB | Checkpoint when WAL growth reaches this size |
| `WALCheckpointInterval` | 30s | Checkpoint after this long with mutations |
| `DisableSync` | `false` | If `true`, skip WAL fsync on group commit |
| `GroupCommitMaxOps` | 64 | Max mutations per WAL sync |
| `GroupCommitMaxWait` | 2ms | Max wait to coalesce writers before sync |

### Durability

1. Mutations are **group-committed** to the WAL and fsynced (unless `DisableSync`).
2. Then appended to the main file, the RAM index is updated, and the call returns.
3. On open after a crash: scan the main file (ignore a torn last record), then replay WAL records with seq greater than the main file’s max seq.

Checkpoints rewrite live keys into a new main file (`*.tmp` → fsync → rename → **fsync parent directory**) and truncate the WAL. By default they run at 8 MiB of WAL growth or after 30s with mutations.

The main file is append-only between checkpoints, so updates and deletes leave garbage until compact (`Stats.ApproxDeadBytes`).

### Errors

Check with `errors.Is`:

| Error | When |
| --- | --- |
| `ErrNotFound` | `Get` on a missing key |
| `ErrInvalidKey` | Empty key |
| `ErrTooLarge` | Key > 1 KiB or value > 4 MiB |
| `ErrLocked` | Another process holds the advisory lock |
| `ErrClosed` | Use after `Close` |
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
dorradb> compact
dorradb> quit
```

Path: `--db PATH`, `--db=PATH`, or env `DORRADB_PATH` (default `./app.dorradb`).

One-shot:

```bash
go run ./cmd/dorradb set name Ada
go run ./cmd/dorradb get name
go run ./cmd/dorradb delete name
go run ./cmd/dorradb status
go run ./cmd/dorradb checkpoint
go run ./cmd/dorradb set blob -   # value from stdin
```

`--json` formats `get` / `status` as JSON. `version` prints `dorradb 0.1.0`.

Exit codes: `0` ok, `1` error, `2` key not found (`get`).

## On-disk format

Little-endian frames with IEEE CRC32. Fixed 64-byte header: magic `DORA` (`0x41524F44`), file type (main=1, WAL=2), `formatVersion = 1`, seq, WAL checkpoint offset, header CRC.

Records: length-prefixed payload + CRC. Ops: set (1), delete (2). Newer format versions refuse to open (`ErrUnsupportedVersion`). A torn last record is treated as a crash tail; a CRC failure mid-file is `ErrCorrupt`.
