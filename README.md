# MilkyApps Core

Utilities and helpers for writing apps in Rust: concurrency primitives, an
async runtime, SMR/hazard pointers, and a durable **storage** stack (WAL +
page manager + ART) with **deterministic simulation testing** for crash
schedules.

## Docs

| Doc | Description |
|-----|-------------|
| [`TODO.md`](./TODO.md) | Storage / dual-root / DST checklist |
| [`DST.md`](./DST.md) | How deterministic simulation testing works |
| [`src/storage/storage.md`](./src/storage/storage.md) | Storage-layer tutorial |
| [`src/storage/stateofart.md`](./src/storage/stateofart.md) | Gap analysis vs production engines |

## Layout (storage & sim)

```text
src/storage/     Wal, PageManager, ArtTrie, Storage façade
src/sim/         Sim / SimFs world for DST
tests/pagemgr/   PageManager snapshot fixtures
tests/arttrie/   ART snapshot fixtures
tests/dst/       Crash / reopen DST fixtures
```

## Tests

```bash
cargo test --test main       # pagemgr snapshots
cargo test --test arttrie    # ART snapshots
just test-sim                # DST (or: cargo test --test dst)
just test-loom               # loom concurrency (optional)
just test-miri               # miri (optional)
```
