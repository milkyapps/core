# State of the art — gap analysis & TODO list

This document is a **working TODO** for `milkyapps-core` storage
(`src/storage/`). It lists what we still lack relative to modern, production
storage engines and the research that defines “state of the art.”

Companion docs:

- Tutorial: [`storage.md`](./storage.md)
- DST walkthrough: [`../../DST.md`](../../DST.md)
- Working checklist: [`../../TODO.md`](../../TODO.md)

**How to read this file**

- Each item is a checkbox TODO.
- Priority tags: **P0** (correctness / crash safety), **P1** (performance /
  operability), **P2** (advanced / optional).
- Citations point at papers, books, and essays you should read before
  implementing the item.

We are **not** claiming every checkbox is required for every product. State of
the art is a **menu**; pick based on threat model and workload.

---

## Table of contents

1. [Crash consistency & recovery](#1-crash-consistency--recovery)
2. [Write-ahead log](#2-write-ahead-log)
3. [Buffer pool / page manager](#3-buffer-pool--page-manager)
4. [Indexing](#4-indexing)
5. [Transactions & concurrency](#5-transactions--concurrency)
6. [Snapshots & versioning](#6-snapshots--versioning)
7. [I/O stack & hardware](#7-io-stack--hardware)
8. [Operability & ecosystem](#8-operability--ecosystem)
9. [Testing & verification](#9-testing--verification)
10. [Reading list (canonical)](#10-reading-list-canonical)

---

## 1. Crash consistency & recovery

### Dual-root / checkpoint publish — **P0**

- [ ] Implement durable publish: flush WAL through `L`, flush reachable pages,
      persist `(durable_root, durable_wal_offset)`, sync meta.
- [ ] Crash recovery must load **durable_*** only, never trust live meta unless
      published through that barrier.
- [ ] Choose and implement anti-aliasing: **COW**, **checkpoint clone**, or
      **meta + WAL replay only**.

Today `put` advances the live ART without waiting for WAL durability
(`storage/mod.rs`). That is intentional for latency; without publish + recovery
it is not crash-safe for the index.

> “A snapshot MUST be guaranteed to come from a flushed [prefix of] the WAL.”
> — project TODO / design note

**Read:** Mohan et al., *ARIES* (TODS 1992) — checkpoint + redo from last
consistent LSN  
<https://cs.stanford.edu/people/chrismre/cs345/rl/aries.pdf>

**Read:** Pillai et al., OSDI ’14 — apps that skip barriers corrupt state  
<https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf>

### Full recovery manager (ARIES-class) — **P1**

- [ ] Page LSNs (or equivalent) so redo knows whether a page is stale.
- [ ] Distinguish **redo** vs **undo** (or commit to No-Undo / shadow paging
      only and document it).
- [ ] Steal / No-Force policy made explicit in docs + tests.
- [ ] Analysis / redo / undo passes (or a simpler documented subset).

> “Write-ahead logging (WAL) is the fundamental and widely used approach for
> ensuring atomicity and durability of transactions.”  
> — Mohan et al., ARIES abstract (paraphrased from standard ARIES framing)

**Read:** Gray & Reuter, *Transaction Processing* (1993), Ch. on logging &
recovery.  
**Read:** Petrov, *Database Internals* (2019), recovery chapters.

### Torn-page / sector protection — **P0**

- [ ] Detect incomplete page writes (checksum per page, or double-write buffer,
      or CoW full-page images).
- [ ] Document assumption about atomic sector size (often 4 KiB on modern
      disks—but not a POSIX guarantee).

Pillai et al. show **atomicity** assumptions are a top bug class. Our WAL
frames records; data pages currently have **no** torn-page defense.

**Read:** PostgreSQL “full_page_writes” / doublewrite discussions; InnoDB
doublewrite buffer design notes.

---

## 2. Write-ahead log

### Record integrity — **P0**

- [ ] Per-record checksum (CRC32C / xxHash) covering length + payload.
- [ ] Optional chain checksum / epoch so corruption doesn’t silently splice
      records.

Dan Luu, [*Files are hard*](https://danluu.com/file-consistency/):

> “you need to checksum your data since you will get silent errors and
> corruption. The only questions are whether or not you detect the errors and
> whether or not your record format only destroys a single record when
> corruption happens, or if it destroys the entire database.”

Today payloads are opaque and **unchecked** at the WAL layer.

### Group commit & pipelined durability — **P1**

- [ ] Batch many `append`s into one `fdatasync` (group commit).
- [ ] Separate “accepted” vs “durable” sequences for callers.
- [ ] Latency/throughput knobs (max wait, max bytes, max records).

**Read:** Classic group commit literature; Petrov *Database Internals* on
batching fsyncs. Postgres `commit_delay` / `commit_siblings` as a production
reference.

### Log segmentation, recycling, archival — **P1**

- [ ] Automatic segment roll (we only error on `SegmentFull` today).
- [ ] Truncate / recycle segments once durable checkpoint advances past them.
- [ ] Optional shipping / archival hooks.

**Read:** ARIES log space management; Kafka / bookkeeper-style segment
lifecycle for operational inspiration (different domain, similar file
hygiene).

### Stronger sync backends — **P1**

- [ ] `FsApi` impl with Linux `fallocate` / `fdatasync` tuning.
- [ ] macOS `F_FULLFSYNC` path (plain `fsync` has historically been weaker).
- [ ] Optional `O_DIRECT` / io_uring backend for controlled I/O.

Luu notes OS X / some ext configs where `fsync` is not a true device flush.
Our `RustStdApi` is a baseline, not the ceiling.

### Parallel / sharded logs — **P2**

- [ ] Per-partition WALs for multi-core ingest (with cross-partition
      transaction story—or forbid cross-partition txns).

**Read:** Shore-MT, Silo, FOEDUS — multicore logging designs.

### Compression & encryption of log records — **P2**

- [ ] Optional record compression.
- [ ] Optional encryption at rest (key management is the hard part).

---

## 3. Buffer pool / page manager

### True buffer pool semantics — **P1**

- [ ] Frame table with pin/unpin, eviction policy (LRU / CLOCK / CAR).
- [ ] Separate **cache residency** from **file capacity** (today mmap ≈ entire
      file is addressible; large DBs need eviction).
- [ ] Prefetch / sequential hints.

**Read:** Effelsberg & Haerder, buffer manager classics; Petrov *Database
Internals*, buffer pool chapter.  
**Read:** CLOCK-Pro / CAR papers for modern replacement policies.

### Checksums & page headers — **P0**

- [ ] Per-page header: magic, checksum, LSN / epoch, page type.
- [ ] Verify on read; fail loud on mismatch.

### Space management — **P1**

- [ ] Free-space map / vacuum for variable-length records (ART blobs grow via
      power-of-two slots; fragmentation & reclaim are limited).
- [ ] Compaction / defragmentation pass.
- [ ] File shrink / hole punch after compaction.

### Allocation beyond power-of-two slabs — **P2**

- [ ] Variable-size records with slotted pages (Postgres heap style) if ART
      leaves shouldn’t own separate blobs.
- [ ] Huge pages / explicit alignment for performance.

### Durability of metadata updates — **P0**

- [ ] Ordering protocol when updating free lists + custom meta + WAL
      (explicit barriers; dir sync when renaming).

Again: Pillai et al.—**ordering** bugs dominate.

---

## 4. Indexing

### ART completeness — **P1**

- [ ] `delete` / range delete.
- [ ] Range scan / ordered iteration iterators.
- [ ] Optimistic lock coupling or other concurrent ART (Leis et al. follow-on
      work on synchronization).
- [ ] Persist root + structural invariants tested under crash injection.

**Paper:** Leis, Kemper, Neumann, *The Adaptive Radix Tree* (ICDE 2013)  
<https://db.in.tum.de/~leis/papers/ART.pdf>

> “ART adaptively chooses compact and efficient data structures for sparse and
> dense nodes.”

We have Node4/16/48/256 + prefix compression; we do **not** yet match a
full production ART (concurrency, deletion, scanners).

### Alternative / complementary indexes — **P2**

- [ ] Evaluate B+tree or Bw-Tree for disk-oriented page access patterns.
- [ ] Evaluate LSM (RocksDB-style) if write amplification / range scans dominate.
- [ ] Secondary indexes + consistency with primary.

**Read:** O’Neil et al., *The Log-Structured Merge-Tree* (1996).  
**Read:** Mao et al., *Masstree* (EuroSys 2012).  
**Read:** Levandoski et al., *The Bw-Tree* (ICDE 2013).

### Cache-conscious & SIMD optimizations — **P2**

- [ ] SIMD searching inside Node16/Node48 (ART paper discusses this).
- [ ] Prefetch child nodes on descent.

---

## 5. Transactions & concurrency

### ACID transactions — **P1** (if we are a database)

- [ ] Begin / commit / abort API.
- [ ] Atomic multi-key updates (today each `put` is independent).
- [ ] Isolation level story (READ COMMITTED / SI / SSI).

**Read:** Gray & Reuter; Berenson et al., *Critique of ANSI SQL Isolation
Levels* (SIGMOD 1995).  
**Read:** Cahill, Röhm, Fekete — Serializable SI (SSI).

### Concurrency control — **P1**

- [ ] Latch/lock coupling for ART, or lock-free / OCC design.
- [ ] WAL concurrent append (today effectively single-writer).
- [ ] Dual-root publish interacting safely with writers.

**Read:** Leis et al. on synchronizing ART; OLTP engines (Silo, TicToc,
Sundial) for OCC/MVCC inspiration.

### Multi-version concurrency (MVCC) — **P2**

- [ ] Version chains or COW versions for readers without blocking writers.
- [ ] Snapshot reads aligned with durable epochs.

**Read:** Postgres MVCC docs; Bernstein & Goodman; *Database Internals*.

---

## 6. Snapshots & versioning

### User-visible snapshots — **P1**

- [ ] API: `snapshot() -> SnapshotId` that is crash-safe by construction.
- [ ] Read-only open of a snapshot (second root or frozen file).
- [ ] List / drop snapshots; retain WAL until oldest snapshot released.

### Time travel / PITR — **P2**

- [ ] Retain WAL segments for point-in-time restore.
- [ ] Backup: base checkpoint + log ship.

**Read:** Postgres PITR; SQLite backup API design for a small-engine contrast.

### COW page versions — **P1** (if dual-root needs it)

- [ ] Immutable pages after publish; copy on first live write.
- [ ] Reference counting / GC for unreachable page versions.

**Analogues:** LMDB (COW B+trees), append-only / purely functional trees.

---

## 7. I/O stack & hardware

### Asynchronous & batched I/O — **P1**

- [ ] io_uring (Linux) or IOCP completion ports for flushes & reads.
- [ ] Background checkpointer thread driven by `WalEvent` / timers.

**Read:** Linux io_uring docs; RocksDB / WiredTiger background flush threads.

### Direct I/O vs mmap — **P1**

- [ ] Measure mmap page faults vs `O_DIRECT` buffered pool under target
      workloads.
- [ ] Document failure modes of mmap durability (`msync` vs `fdatasync`).

mmap is convenient; many engines moved critical paths off shared mmap for
control (see various Postgres / InnoDB discussions historically).

### SSD / NVMe aware layout — **P2**

- [ ] Align pages to device blocks; avoid RMW where possible.
- [ ] Consider zoned storage / FDP only if relevant.

**Read:** He et al. / NVMe papers; “The Unwritten Contract of Solid State
Drives” (He, Du, et al., EuroSys 2017) for SSD assumptions apps violate.

### Persistent memory — **P2**

- [ ] Optional PMem path with different failure model (byte granularity,
      different barriers).

**Read:** Mnemosyne, NV-Heaps, RECIPE; PMem indexes survey papers.

---

## 8. Operability & ecosystem

### Observability — **P1**

- [ ] Metrics: append latency, fsync latency, dirty page count, WAL bytes,
      checkpoint lag (`live_offset - durable_offset`).
- [ ] Structured tracing on publish / recovery.

### Tooling — **P1**

- [ ] `wal dump` / `pages dump` / `fsck`-like checker.
- [ ] Corrupt / inject tools for local debugging.

Gunawi et al. (OSDI ’08) show **fsck itself can make things worse** if
ordering of repairs is wrong—write checkers carefully.

### Configuration & compatibility — **P2**

- [ ] On-disk format version + migrations.
- [ ] Feature flags for checksum / compression / sync mode.

### Encryption, multi-tenant, quotas — **P2**

- [ ] At-rest encryption, key rotation.
- [ ] Per-namespace size limits.

---

## 9. Testing & verification

Walkthrough of the in-tree simulator: **[`DST.md`](../../DST.md)**.

### Crash / fault injection — **P0**

- [x] In-memory `FsApi` (`SimFs`) with volatile vs durable bytes + `Sim::crash`
      (see `src/sim/`, `tests/dst/`).
- [x] `fsync` failure injection (`Sim::fail_next_sync` for `EIO` / `ENOSPC` / …).
- [ ] Record richer FS traces and explore reorder / tear / drop-prefix of
      unsynced writes (Pillai-style schedules beyond “keep last sync”).
- [ ] Power-fail tests on real devices / VMs.

> Conventional testing isn’t enough; “really basic … fault injection can pay
> for themselves … the first time you use them.”  
> — Dan Luu, [*Files are hard*](https://danluu.com/file-consistency/)

**Tools / inspiration:** our `Sim` (start here); Alice (Pillai lineage),
CrashMonkey, filesystem model checkers; Jepsen for distributed—but single-node
FS faults first.

### Deterministic simulation — **P1** (foundation landed)

- [x] Fake `FsApi` (`SimFs`) + memory `PageManager` + `Storage::open_sim`.
- [x] Seeded fixtures (`tests/dst/*.toml`, `just test-sim`) with seed + op log
      on failure.
- [ ] Explore reorderings / torn sectors systematically (not only whole-file
      sync barriers).
- [ ] Seeded stress: concurrent put + checkpoint + reopen.
- [ ] Flip `storage_live_meta_ahead_of_wal` to fail closed once dual-root
      refuses live meta after crash.

### Property tests & format fuzzing — **P1**

- [ ] Fuzz WAL scanner / page decoder.
- [x] Insta/snapshot tests cover ART / page dumps (`tests/arttrie`,
      `tests/pagemgr`).
- [ ] Extend to recovery transcripts under DST (beyond per-fixture expects).

### Formal / semi-formal specs — **P2**

- [ ] TLA+ or Alloy model of dual-root publish + recovery.
- [ ] Refinement against a sequential spec (put/get linearizability under
      single writer).

---

## 10. Reading list (canonical)

Keep these on the shelf while clearing TODOs above.

| Resource | Why |
|----------|-----|
| Dan Luu, [*Files are hard*](https://danluu.com/file-consistency/) | Crash consistency intuition; checksums; barriers |
| Pillai et al., OSDI ’14 | Measured FS properties; app bug taxonomy |
| Mohan et al., ARIES (TODS 1992) | WAL + recovery gold standard |
| Gray & Reuter, *Transaction Processing* (1993) | Steal/No-Force, logging, isolation |
| Alex Petrov, *Database Internals* (2019) | Modern engine map (WAL, B-tree, LSM, MVCC) |
| Leis et al., ART (ICDE 2013) | Our index family |
| O’Neil et al., LSM-tree (1996) | If write-heavy / compaction path is needed |
| Berenson et al., ANSI isolation critique (1995) | Isolation levels done right |
| He et al., SSD “Unwritten Contract” (EuroSys 2017) | Device assumptions |
| PostgreSQL / SQLite / RocksDB / LMDB source & docs | Production reference behaviors |

---

## Suggested implementation order (opinionated)

If the goal is “serious single-node embedded store” with minimal regret:

1. **P0** WAL checksums + torn-page strategy  
2. **P0** Dual-root publish + recovery (+ COW or clone)  
3. **P0** Crash-injection / DST over `FsApi` — **foundation landed**
   (`Sim` / `tests/dst/`; deepen tear/reorder + dual-root scenarios next;
   see [`DST.md`](../../DST.md))  
4. **P1** Group commit + background checkpointer on `WalEvent`  
5. **P1** ART delete + range scan  
6. **P1** Metrics + `fsck` tooling  
7. **P1/P2** Transactions / MVCC only once single-key durability is boringly solid  

Until (1)–(2) are done (and DST scenarios assert them), treat the engine as
**experimental**: fast, educational, and unsafe as a system of record.

---

## Living document

Update checkboxes as work lands. When an item is done, link the PR / module
docs from the checkbox line. When research changes the recommendation (e.g.
new io_uring patterns, new index), amend the citation—not only the code.
