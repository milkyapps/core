We have three components in this folder that compose the storage layer of a database: WAL, Art Trie and Page Manager.

So let us move them to a "storage" folder/module.

And let us create a struct Storage.

MetadataPage needs to allow "custom" data to be stored in it.

We will store one struct called StorageConsumer { wal_offset: u64 }.

Storage will be { wal: Wal, pages: PageManager, index: ArtTrie }.
Storage will store StorageConsumer in the MetadataPage.

wal_offset is the next offset of the wal that will be processed. Storage send the block bytes to WAL and immediatly send it to the index, it does not wait for the WAL to flush.

Because of this we need a way to store snapshots of the trie. A snapshot MUST be garanteed to come from a flushed and read from the WAL file. Let us brainstorm possibilities here.

Broader gap analysis vs production engines: `src/storage/stateofart.md`.
Design notes for dual-root: `src/storage/mod.rs` module docs.
How DST works (sim world, fixtures, limits): [`DST.md`](./DST.md).
Storage tutorial: `src/storage/storage.md`.

---

## Status

Done:
- Moved to `src/storage/{wal,pagemgr,arttrie,store}.rs`
- `MetadataPage::get_custom_data` / `set_custom_data` (+ `PageManager` wrappers)
- `Storage` owns `{ wal, pages, index }` (100% ownership)
- `StorageConsumer { wal_offset }` in page-0 custom data (plus live root handle for reopen)
- `put` = WAL append then immediate ART apply (no extra flush wait on the Storage path)
- Unit smoke tests in `store.rs` (`put_get_and_reopen`, `custom_data_roundtrip_via_pages`, sim reopen)
- Component snapshot tests under `tests/` for `pagemgr` and `arttrie`
- DST: `src/sim/`, `Storage::open_sim`, `tests/dst/*.toml` + `just test-sim` (see [`DST.md`](./DST.md))

Caveats on what “Done” does **not** mean yet:
- `wal_offset` is stored and advanced, but **`Storage::open` never replays** `WAL[wal_offset..]` — recovery is restore-live-root only.
- Live `(wal_offset, root)` written on every `put` is a **reopen convenience**, not a crash-safe checkpoint (can be ahead of a flushed WAL; auto dirty-page flush can persist that state).
- `put` itself does not flush, but `AppendConfig` can still sync inside `Wal::append` unless callers use a no-flush config.

---

## Pending

### Dual-root durable snapshot (need design confirm) — P0

Live path can be ahead of a crash-safe WAL flush. Dual-root keeps:

1. **live_root / live_wal_offset** — updated on every `put` (may include unflushed WAL).
2. **durable_root / durable_wal_offset** — published only after WAL flush through that offset **and** page flush of the snapshot graph, then meta sync.

Concrete work:
- [ ] On-disk meta layout for both live and durable pairs (today: single `OnDiskMeta` root + offset).
- [ ] `publish_durable(L)` (or equivalent): flush WAL through `L` → flush pages reachable from snapshot root → write durable meta → sync meta.
- [ ] Anti-aliasing so live mutation cannot corrupt durable pages: **COW**, **checkpoint clone**, or publish only while the tree is frozen/copied.
- [ ] Crash recovery: load **durable_*** only, then optionally replay `WAL[durable_wal_offset .. flushed_tip]` into a fresh live root.
- [ ] Stop treating live meta as trusted after crash unless it was published through the durability barrier.

Constraint: live and durable must not share mutable nodes (COW or checkpoint clone),
or publishing durable is only safe when the tree is frozen/copied.

### Storage façade completeness

- [ ] Wire `wal_offset` / durable cursor into real recovery (consume `Wal::records` from the checkpoint).
- [ ] Public APIs: `publish_durable`, `recover` / `replay` (and likely `delete` if the index supports it).
- [ ] Document steal / no-force policy once dual-root lands (what may be on disk vs what is durable).

### Testing gaps

- [x] Storage crash schedules under DST (`tests/dst/`; see [`DST.md`](./DST.md)).
- [ ] Flip `storage_live_meta_ahead_of_wal` to expect **missing** once dual-root
      refuses live meta after crash (today the fixture documents the unsafe reopen).
- [ ] Integration tests beyond DST expects (richer `Storage` API coverage under
      `tests/` if needed).
- [ ] Tests that recovery replays flushed WAL past `durable_wal_offset`.

### Related P0 from `stateofart.md` (not dual-root, but crash-safety)

Track detail there; call out here so they are not forgotten:

- [ ] WAL record integrity (per-record checksum).
- [ ] Torn-page / sector protection for data pages (checksum, double-write, or CoW images).
- [ ] Per-page headers: magic, checksum, LSN/epoch.

---

## DST — Deterministic simulation testing

Goal: a **seeded simulator** that replaces real FS/mmap/clocks, runs Storage ops,
crashes by keeping only what the model says is durable, reopens, and checks
invariants. Failures must print **seed + schedule/op log**.

**Full walkthrough:** [`DST.md`](./DST.md).  
Companion notes in `src/storage/stateofart.md` (§ Testing & verification).  
**Loom stays** for atomics / hazard pointers; DST is for crash consistency +
recovery (complementary, not a replacement).

Do **not** wait for a full Alice/CrashMonkey clone. Prefer a fake `FsApi` +
crash policies first. Prefer building the sim **before / alongside** dual-root
so publish/recover are proven under fault schedules.

### 1. Sim world

- [x] Module `src/sim/`: in-memory files with pending vs durable bytes after
      `sync_data` / `sync_all` / `sync_dir` (`Sim`, `SimFs`).
- [x] Fault knob: `Sim::fail_next_sync` (`EIO` / `ENOSPC` / …).
- [ ] Fault knobs: drop un-synced prefix mid-record, reorder unsynced sector
      writes, tear a page.
- [x] Seeded RNG (`SimRng`) for schedule / fault choices.
- [ ] Optional logical clock (avoid wall-clock `Instant` on paths under test).

### 2. Widen I/O seams

- [x] `SimFs: FsApi` (WAL plugs in via `Wal::with_options_and_fs`).
- [x] `PageManager` memory backend (`open_memory` / durable image) for DST;
      mmap path unchanged for production + existing `tests/pagemgr` fixtures.
- [x] `Storage<A: FsApi>` + `Storage::open_sim` (sim WAL + memory pages);
      `sync_sim_pages` publishes durable page image into `Sim` before crash.

### 3. Harness (small, not a framework)

- [x] `Sim::run` + op log; failures print seed + schedule
      (`Sim::failure_report`).
- [x] Fixture runner `tests/dst_runner.rs` + `tests/dst/*.toml` (same
      libtest-mimic / TOML style as `pagemgr` / `arttrie`).
- [ ] Sequential shadow spec (`BTreeMap`) for expected post-recovery state
      under documented durability rules (beyond per-fixture expects).

### 4. Scenario ladder (grow in this order)

- [x] **WAL-only:** `wal_crash_loses_unflushed`, `wal_flush_survives_crash`.
- [x] **Live meta ahead of WAL (today’s bug class):**
      `storage_live_meta_ahead_of_wal` documents unsafe live reopen; flip to
      `missing = true` once dual-root refuses to trust live meta.
- [x] **Happy path Storage:** `storage_put_flush_reopen`.
- [ ] **Dual-root publish barrier** once `publish_durable` / recover exist.
- [ ] **Concurrent put + checkpoint + crash** (seeded interleavings; use loom
      only when debugging atomic-level races).

### 5. CI wiring

- [x] `just test-sim` / `cargo test --test dst`.
- [ ] Fixed seed corpus in CI + optional nightly random seeds.
- [x] Loom / miri recipes left separate; DST does not replace them.

### Out of scope early

- Simulating the whole `async_rt` before Storage crash paths are covered.
- Claiming DST “covers dual-root” before publish/recover exist — until then it
  documents that trusting live reopen after crash is unsafe.
