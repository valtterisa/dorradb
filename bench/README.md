# Benchmarks

## Quick CLI (what you want)

```bash
go run ./cmd/bench           # writes + reads
go run ./cmd/bench writes    # batched puts (durable + nosync)
go run ./cmd/bench reads     # random gets after fill
go run ./cmd/bench help
```

Prints a small terminal scoreboard vs bbolt (time/op, rate, who wins).

## Raw `go test` benches (optional)

```bash
go test ./bench -bench="GetRand|PutBatch" -benchmem -count=3
```

Quote `|` in PowerShell.