# DorraDB test suites

Integration and reliability tests live here. White-box unit tests stay next to
`internal/storage`, `internal/storage/page`, and `internal/index`.

| Package | Owns |
| --- | --- |
| `harness` | Temp DB, `CrashCopy`, `FaultFS` |
| `api` | Public contract, options, lock, Close errors |
| `tx` | Begin/Commit/Rollback, snapshot, RYW |
| `recovery` | WAL/main crash, poison/`ErrFatal`, commit atomicity |
| `checkpoint` | Vacuum gate vs open pins |
| `corrupt` | Bad magic, unsupported version, v2 upgrade |
| `stress` | Concurrent readers + one writer (`-race`) |

```bash
go test ./tests/... -short
go test ./tests/... -race -count=1
go test ./tests/stress -race
```
