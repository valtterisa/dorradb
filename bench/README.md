# Benchmarks

**1:1** DorraDB vs **SQLite WAL** ([`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite), pure Go).

Matched: `journal_mode=WAL`, `cache_size` ≈ `CachePages×4KiB` (512 → 2048 KiB), `mmap_size=0`, `synchronous=FULL|OFF` ↔ DorraDB sync. No pass/fail “budget” — lower time wins.

## Suites

| Suite | What |
|-------|------|
| Durable writes | 100 puts / commit, FULL sync |
| Nosync writes | same, sync OFF |
| Hot reads | fill 100k; Gets over last 8k; one read tx |
| Cold reads | uniform Gets over 100k (matched cache) |

## Quick CLI

```bash
go run ./cmd/bench           # writes + reads
go run ./cmd/bench writes
go run ./cmd/bench reads
go run ./cmd/bench help
```

## Raw `go test` benches

```bash
go test ./bench -bench="GetHot|GetCold|PutBatch" -benchmem -count=3
```

Quote `|` in PowerShell.