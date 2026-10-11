# DoraDB

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

### Durability

1. Mutations are **group-committed** to the WAL and fsynced (by default).
2. Then applied to the main file and acknowledged.
3. On crash, recovery scans the main file (ignoring a torn tail) and replays WAL records with seq greater than the main file’s max seq.

Checkpoints compact live keys into a new main file (`*.tmp` → fsync → rename → **fsync parent directory**) and truncate the WAL. Triggers: WAL growth ≥ 8 MiB or ≥ 30s since last checkpoint (with mutations).

The main file is append-only between checkpoints, so repeated updates of the same key leave garbage until compact (`Stats.ApproxDeadBytes`).

### Limits

- Max key: 1 KiB  
- Max value: 4 MiB  

Override via `Options`.

## CLI

```bash
go build -o dorradb ./cmd/dorradb

./dorradb --db app.dorradb set foo bar
./dorradb --db app.dorradb get foo
./dorradb --db app.dorradb delete foo
./dorradb --db app.dorradb status
./dorradb --db app.dorradb checkpoint
./dorradb --db app.dorradb shell
```

Exit codes: `0` ok, `1` error, `2` key not found (`get`).

`DORRADB_PATH` can replace `--db`.

## Format

Little-endian frames with CRC32. Header magic `DORA`, `formatVersion = 1`. Newer versions refuse to open (`ErrUnsupportedVersion`). Mid-file CRC failures return `ErrCorrupt`; a torn last record is treated as a crash tail.
