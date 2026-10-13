# Deterministic simulation testing (DST)

This document explains how crash-consistency testing works in `milkyapps-core`:
the simulated world, how storage plugs into it, how fixtures are written, and
how DST relates to loom / miri / snapshot tests.

Companion docs:

| Doc | Role |
|-----|------|
| [`TODO.md`](./TODO.md) | Project checklist (DST section tracks remaining work) |
| [`src/storage/storage.md`](./src/storage/storage.md) | Storage-layer tutorial |
| [`src/storage/stateofart.md`](./src/storage/stateofart.md) | Gap analysis vs production engines |
| `src/sim/mod.rs` | Implementation |

---

## 1. Why DST

Real filesystems reorder, tear, and drop writes across crashes
([Pillai et al., OSDI ’14](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf);
[Dan Luu, *Files are hard*](https://danluu.com/file-consistency/)). Unit tests
that call `flush` and reopen on a happy path do **not** explore those schedules.

**Deterministic simulation testing** replaces the OS I/O surface with an
in-memory model that:

1. Records every relevant op (write, sync, crash, reopen, …).
2. Distinguishes **volatile** bytes (accepted by the API) from **durable**
   bytes (survived the last sync).
3. On `crash`, keeps only durable state, then reopens the engine against that
   state.
4. Fails with a **seed + op log** so the schedule is reproducible.

DST here targets **single-node crash consistency** of WAL + pages + `Storage`.
It does **not** replace:

- **loom** — atomics / hazard-pointer interleavings (`just test-loom`)
- **miri** — UB in unsafe code (`just test-miri`)
- **insta snapshots** — deterministic dumps of page/ART layouts (`tests/pagemgr`,
  `tests/arttrie`)

---

## 2. Architecture

```text
                    ┌──────────────── Sim ─────────────────┐
                    │  seed, SimRng, op log                 │
                    │                                       │
                    │  SimFs (FsApi)                        │
                    │   files: volatile ↔ durable           │
                    │   dirs:  volatile ↔ durable names     │
                    │                                       │
                    │  pages: Path → durable page image     │
                    └───────────────┬───────────────────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
        Wal<SimFs>          PageManager              Storage<SimFs>
        (append/sync        (memory backend:         open_sim /
         via FsApi)          volatile + durable       sync_sim_pages
                             on flush)                put / get
```

### `Sim` / `SimFs` (`src/sim/`)

| Piece | Role |
|-------|------|
| `Sim::new(seed)` | Create a world; seed drives `SimRng` and labels failures |
| `Sim::fs()` | Cloneable `SimFs` handle sharing the world’s file table |
| `Sim::log` / `ops` / `failure_report` | Human-readable schedule |
| `Sim::crash` | Revert files/dirs to last durable snapshot |
| `Sim::fail_next_sync` | Next `sync_*` returns a chosen `io::ErrorKind` |
| `Sim::install_pages` / `pages_durable` | Durable page-file images for memory `PageManager` |
| `Sim::run` | Run a closure; on `Err`, append the failure report |

**Crash model for files:**

- `write` / `set_len` update **volatile** only.
- `sync_data` / `sync_all` copy volatile → durable (and `sync_all` also marks
  the directory entry durable).
- `sync_dir` makes directory names durable.
- `crash` drops non-durable names and sets `volatile = durable` for survivors.

### Page manager memory backend

Production opens use mmap (`PageManager::open_with_options`). Under DST:

- `PageManager::open_memory(durable, page_size, dirties_before_flush)` loads
  (or creates) an in-memory image.
- Flushes copy volatile ranges into an internal **durable** buffer.
- `durable_image()` exports that buffer for `Sim::install_pages`.

Existing `tests/pagemgr` / `tests/arttrie` fixtures still use real temp files +
mmap; they are unchanged.

### `Storage` under sim

```rust
let sim = Sim::new(1);
let mut opts = StorageOptions::new("/sim/demo");
opts.wal = opts.wal.clone().without_preallocate().max_file_bytes(0);

let mut db = Storage::open_sim(&sim, opts.clone())?;
db.put(b"k", b"v", AppendConfig::no_flush())?;
db.sync_sim_pages(&sim)?;   // flush pages + install durable image into Sim
drop(db);
sim.crash();                // WAL loses unsynced bytes; pages keep installed image
let mut db = Storage::open_sim(&sim, opts)?;
```

`Storage` is generic: `Storage<A: FsApi = RustStdApi>`. Production still uses
`Storage::open` / `open_in` with `RustStdApi` + mmap pages.

**Important:** before `crash` / reopen, call `sync_sim_pages` (or a fixture op
that does) so the sim retains flushed page bytes. WAL durability is independent
and follows `SimFs` sync rules.

---

## 3. Fixture runner (`tests/dst`)

Same style as `tests/pagemgr` and `tests/arttrie`:

| Piece | Path |
|-------|------|
| Runner | `tests/dst_runner.rs` (`[[test]] name = "dst"`) |
| Fixtures | `tests/dst/*.toml` |
| Recipe | `just test-sim` or `cargo test --test dst` |

Each TOML file becomes one libtest-mimic trial named `dst::<stem>`. On failure
the runner prints the error plus `Sim::failure_report()` (seed + ops).

### TOML commands

| `op` | Meaning |
|------|---------|
| `wal_append` | Append `payload` with `flush` = `none` \| `data` \| `all` |
| `wal_flush` | Flush open WAL (`flush` default `all`) |
| `wal_reopen` | Drop + reopen WAL against `SimFs` |
| `wal_expect` | Assert `records = ["…"]` via `Wal::records` |
| `storage_put` | `put(key, value)` with optional `flush` |
| `storage_flush_pages` | `flush_pages(true)` + `sync_sim_pages` |
| `storage_sync_pages` | `sync_sim_pages` only |
| `storage_reopen` | Sync pages into sim, drop, `open_sim` again |
| `storage_expect_get` | Assert get value, or `missing = true` |
| `crash` | Sync open storage pages into sim, drop handles, `Sim::crash` |

Optional fixture headers: `seed`, `page_size`, `dirties_before_flush`,
`notify_capacity`.

### Current corpus

| Fixture | Intent |
|---------|--------|
| `wal_crash_loses_unflushed.toml` | Unsynced append disappears after crash |
| `wal_flush_survives_crash.toml` | Synced append survives |
| `storage_put_flush_reopen.toml` | Flushed put + pages round-trip under sim |
| `storage_live_meta_ahead_of_wal.toml` | **Documents today’s gap:** live page meta can outlive an unsynced WAL; flip to `missing = true` once dual-root refuses live meta |

---

## 4. Mental model: one schedule

```text
seed=1
  create_dir_all(/sim/dst)
  open_segment(.../wal.log)
  sync_dir(/sim/dst)                 # name durable
  wal_append("durable", All)         # volatile+durable
  wal_append("volatile", None)       # volatile only
  crash                              # "volatile" gone
  wal_reopen
  wal_expect(["durable"])            # pass / fail + print ops
```

If an expect fails, you get something like:

```text
wal_expect: expected ["durable"], got ["durable", "volatile"]
DST failure seed=1
ops:
  [   0] create_dir_all(/sim/dst)
  …
```

Re-run the same fixture (same seed + same TOML) to reproduce.

---

## 5. How this fits the other test suites

```text
tests/
  pagemgr/*.toml     → tests/main.rs          (PageManager dumps / insta)
  arttrie/*.toml     → tests/arttrie_runner.rs (ART dumps / insta)
  dst/*.toml         → tests/dst_runner.rs     (Sim crash schedules)
```

| Suite | Backend | Asserts |
|-------|---------|---------|
| pagemgr / arttrie | Real tempfile + mmap | Deterministic text dumps (`insta`) |
| dst | `Sim` + `SimFs` + memory pages | Crash/reopen expects (records / gets) |
| loom / miri | Special `RUSTFLAGS` / nightly | Concurrency & UB |

Prefer **adding a `tests/dst/*.toml`** for new durability claims. Prefer
**pagemgr/arttrie fixtures** for layout / allocator / trie shape regressions.

---

## 6. Adding a new DST scenario

1. Create `tests/dst/my_case.toml` with a `seed` and `[[cmds]]`.
2. Run `cargo test --test dst my_case` (or `just test-sim my_case`).
3. On failure, read the printed op log; adjust expects or fix the engine.
4. If you need a new verb, extend `tests/dst_runner.rs` and document it here.

For Rust-level experiments (not TOML), use `Sim` + `Storage::open_sim` in a
`#[cfg(test)]` module (see `store.rs` / `sim/mod.rs` unit tests).

---

## 7. Limits (honest)

**Implemented**

- Volatile vs durable file bytes and directory names
- Sync-failure injection (`fail_next_sync`)
- Memory page images coordinated through `Sim`
- TOML crash/reopen ladder for WAL + Storage

**Not yet** (see `TODO.md` § DST)

- Sector reorder / mid-record tear / drop-prefix exploration
- Logical clocks (no wall-time in schedules)
- Global shadow `BTreeMap` spec across a whole run
- Dual-root `publish_durable` scenarios (blocked on the feature)
- Concurrent put + checkpoint schedules
- Nightly random-seed CI corpus

Until dual-root ships, fixtures like `storage_live_meta_ahead_of_wal` **encode
the unsafe status quo** on purpose: they fail closed later when recovery stops
trusting live meta after a crash.

---

## 8. Quick reference

```bash
just test-sim              # all DST fixtures
cargo test --test dst      # same
cargo test --test dst wal_ # filter by name fragment
cargo test --lib sim::     # unit tests inside src/sim
```

```rust
use milkyapps_core::sim::Sim;
use milkyapps_core::storage::{AppendConfig, Storage, StorageOptions};
use milkyapps_core::storage::wal::{Wal, WalOptions};
```
