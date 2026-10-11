# DorraDB

Embedded key-value library for Go (**Linux and Windows**). SQLite-style files, with OS differences behind an internal **VFS** (stdlib + [`gofrs/flock`](https://github.com/gofrs/flock)):

```text
app.dorradb         # main database
app.dorradb-wal     # write-ahead log
app.dorradb.lock    # exclusive advisory lock (via gofrs/flock)
```

Engine code talks to `vfs.FS` only. The default OS VFS maps that to native open/read/write/sync/rename and platform directory sync after checkpoint rename.

## Embed

```go
db, err := dorradb.Open("app.dorradb")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

_ = db.Set([]byte("k"), []byte("v"))
v, err := db.Get([]byte("k"))
_ = db.Delete([]byte("k"))
```

`Open` creates the database if it does not exist. Only one process may open a given path at a time (`ErrLocked` if another process holds the advisory lock). The OS releases the lock when the process exits or crashes.

### Options

Pass an optional `Options` to tune durability and checkpoints. Zero fields keep defaults (`DefaultOptions`):

```go
db, err := dorradb.Open("app.dorradb", dorradb.Options{
    WALCheckpointBytes:    32 << 20,       // checkpoint after 32 MiB of WAL growth
    WALCheckpointInterval: 60 * time.Second,
    DisableSync:           true,           // skip WAL fsync (faster, less durable)
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
2. Then applied to the main file and acknowledged.
3. On crash, recovery scans the main file (ignoring a torn tail) and replays WAL records with seq greater than the main file’s max seq.

Checkpoints compact live keys into a new main file (`*.tmp` → fsync → rename → **fsync parent directory**) and truncate the WAL. By default they run when WAL growth reaches 8 MiB or 30s have passed with mutations (see Options).

The main file is append-only between checkpoints, so repeated updates of the same key leave garbage until compact (`Stats.ApproxDeadBytes`).

### Limits

- Max key: 1 KiB
- Max value: 4 MiB

## CLI

Easiest — interactive shell on `./app.dorradb`:

```bash
go run ./cmd/dorradb
```

Then:

```text
dorradb> set name Ada
saved
dorradb> get name
Ada
dorradb> del name
deleted
dorradb> quit
```

Optional: `go run ./cmd/dorradb --db my.db` or one-shot `go run ./cmd/dorradb get name`.

Exit codes: `0` ok, `1` error, `2` key not found (`get`).

## Format

Little-endian frames with CRC32. Header magic `DORA`, `formatVersion = 1`. Newer versions refuse to open (`ErrUnsupportedVersion`). Mid-file CRC failures return `ErrCorrupt`; a torn last record is treated as a crash tail.
