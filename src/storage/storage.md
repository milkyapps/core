# Storage layer tutorial

This document is a guided tour of `milkyapps-core`’s durable storage stack under
`src/storage/`. It explains **what** the pieces are, **why** they look the way
they do, and **how** they fit together—especially around crash safety.

If you only read one external essay before hacking on the WAL, read Dan Luu’s
[*Files are hard*](https://danluu.com/file-consistency/). Much of our flush and
framing design is a direct response to the failure modes surveyed there.

Related project docs:

- Gap analysis / TODOs: [`stateofart.md`](./stateofart.md)
- Crash schedules (DST): [`../../DST.md`](../../DST.md)
- Working checklist: [`../../TODO.md`](../../TODO.md)

---

## Table of contents

1. [Big picture](#1-big-picture)
2. [Mental model: three owned components](#2-mental-model-three-owned-components)
3. [Write path (`Storage::put`)](#3-write-path-storageput)
4. [Page manager](#4-page-manager)
5. [Adaptive Radix Tree (ART)](#5-adaptive-radix-tree-art)
6. [Write-ahead log (WAL)](#6-write-ahead-log-wal)
7. [Filesystem consistency (why flushes exist)](#7-filesystem-consistency-why-flushes-exist)
8. [Dual-root snapshots (design)](#8-dual-root-snapshots-design)
9. [Hands-on: open, put, get, reopen](#9-hands-on-open-put-get-reopen)
10. [Deterministic simulation testing](#10-deterministic-simulation-testing)
11. [Module map](#11-module-map)
12. [Further reading](#12-further-reading)
13. [Glossary](#13-glossary)

---

## 1. Big picture

A database storage engine usually separates:

- a **redo log** (what happened, in order),
- a **page / buffer pool** (where structured bytes live),
- an **index** (how to find values by key).

In this crate those roles map to:

| Role | Type | File(s) |
|------|------|---------|
| Redo log | `Wal` | `wal.rs` (+ segment under the storage dir) |
| Page / slab allocator | `PageManager` | `pagemgr.rs` (`pages.db` by default) |
| Secondary index | `ArtTrie` | `arttrie.rs` (nodes allocated *inside* the page file) |
| Façade | `Storage` | `store.rs` |

`Storage` **owns all three**. The ART does not own the page file; it only holds
a root `AllocId` and borrows `&mut PageManager` when mutating or searching.

```text
┌──────────────────────── Storage ────────────────────────┐
│                                                         │
│   put(key, value)                                       │
│        │                                                │
│        ├──── append opaque record ────► Wal             │
│        │                                                │
│        └──── insert into trie ────────► ArtTrie         │
│                                              │          │
│                                              ▼          │
│                                         PageManager     │
│                                    (mmap or memory DST) │
│                                                         │
│   page 0 custom meta: StorageConsumer + live root       │
└─────────────────────────────────────────────────────────┘
```

**Important performance / durability trade-off (today):**
`Storage::put` appends to the WAL and **immediately** updates the ART. It does
**not** wait for a WAL flush before returning. That is great for latency and
exactly why we need a careful **snapshot / dual-root** story for crash recovery
(see [§8](#8-dual-root-snapshots-design)).

---

## 2. Mental model: three owned components

```rust
pub struct Storage<A: FsApi = RustStdApi> {
    wal: Wal<A>,
    pages: PageManager,
    index: ArtTrie,
    // plus an optional Receiver<WalEvent> for notifications
    // and pages_path for sim / reopen
}
```

Production opens use `Storage::open` / `open_in` (`RustStdApi` + mmap pages).
Crash schedules use `Storage::open_sim` (`SimFs` + memory-backed pages); see
[§10](#10-deterministic-simulation-testing) and [`DST.md`](../../DST.md).

### Why ownership is split this way

- The **page manager** owns the page image (mmap file or in-memory DST
  backend), size classes, and dirty flushing.
- The **ART** is “just” a graph of `AllocId`s. Keeping it root-only makes it
  reusable under different owners (tests open `(PageManager, ArtTrie)` pairs;
  `Storage` owns both separately).
- The **WAL** is a sequential byte stream with its own durability knobs
  (`FlushStrategy`), parameterized over [`FsApi`] so production (`RustStdApi`)
  and DST (`SimFs`) share the same append/scan logic.

### Consumer cursor

```rust
pub struct StorageConsumer {
    pub wal_offset: u64, // next WAL byte offset to process / recover from
}
```

On disk (page-0 **custom data** region) we currently store a small header:

```text
[wal_offset:u64 LE][root_size:u64][root_page:u64][root_slot:u64]
```

`StorageConsumer` is the public view of `wal_offset`. The root fields let a
process **reopen** the live ART without replaying the entire WAL (this is *not*
yet a durable dual-root publish—see §8).

---

## 3. Write path (`Storage::put`)

For each `put(key, value, append_cfg)`:

1. Encode an opaque WAL payload:
   ```text
   [key_len:u32 LE][key bytes][value bytes]
   ```
2. `wal.append(payload, append_cfg)` — length-prefixed record; optional flush
   per `AppendConfig`.
3. `index.insert(&mut pages, key, value)` — update the live trie.
4. Advance `wal_offset` to the end of the new record and persist custom meta
   (including the live root handle).

Reads (`Storage::get`) only consult the live ART; they do not scan the WAL.

Notifications: after each successful WAL append, a `WalEvent { offset, payload_len }`
is sent on the crate’s bounded channel so background workers can wake.

---

## 4. Page manager

Module: `pagemgr.rs`.

### What it is

A **size-classed slab allocator** backed by one page image. Think of it as a
tiny buffer pool / heap that survives process restart.

**Backends:**

- **mmap** (`open` / `open_with_options`) — production and `tests/pagemgr` /
  `tests/arttrie` fixtures (real tempfile).
- **memory** (`open_memory`) — DST: volatile working set + durable snapshot
  updated on flush; `durable_image()` exports bytes into [`Sim`](../../DST.md).

Logical layout:

```text
┌──────────────────────────── page 0 ────────────────────────────┐
│ MetadataPage (page_size, free_list_by_size[32], flush thresh)  │
│ ………… custom data (rest of page 0) …………………………… │
└────────────────────────────────────────────────────────────────┘
┌──────────────────────────── page N≥1 ──────────────────────────┐
│ [prev][next][class_id][bitmap…][pad][slot 0][slot 1]…          │
└────────────────────────────────────────────────────────────────┘
```

- **Logical page size** is stored in metadata and need **not** equal the host
  OS/VM page size (4 KiB vs 16 KiB Apple Silicon, etc.). Portability of the
  file format depends on the on-disk `page_size`, not on `sysconf(_SC_PAGESIZE)`.
- Size class `i` holds objects of size `2^i`. Free pages for each class hang
  off `MetadataPage::free_list_by_size[i]`.
- Occupancy inside a data page is a **bitmap** (derived geometry; slot counts
  are not stored on disk).

### Custom metadata API

Anything the storage façade needs to remember across restarts (consumer cursor,
roots, …) goes in the **custom data** region after the fixed `MetadataPage`
header:

```rust
MetadataPage::get_custom_data(page0_bytes) -> &[u8]
MetadataPage::set_custom_data(page0_bytes, data) -> io::Result<()>

// ergonomic wrappers:
pages.get_custom_data()
pages.set_custom_data(data)  // marks page 0 dirty
```

Capacity is `page_size - size_of::<MetadataPage>()`. Oversized writes error.

### Flushes

- `flush_page` / `flush_dirty` / `flush_all` push dirty ranges to stable storage
  (mmap `flush_range` / `flush`, or copy into the memory backend’s durable
  buffer).
- `dirties_before_flush` can auto-flush during alloc/dealloc (and after
  `set_custom_data`) when the dirty set grows large—this is a **performance**
  knob, not a substitute for the WAL durability protocol.
- Under DST, call `Storage::sync_sim_pages` before `Sim::crash` so the sim
  retains the durable page image (WAL sync remains separate via `SimFs`).

---

## 5. Adaptive Radix Tree (ART)

Module: `arttrie.rs`.

### What it is

An **Adaptive Radix Tree** maps arbitrary `&[u8]` keys to `&[u8]` values. Inner
nodes adapt their fanout (Node4 → Node16 → Node48 → Node256) as children are
added. Paths use **prefix compression** (up to 64 bytes inline per node).

The classic paper is:

> Viktor Leis, Alfons Kemper, Thomas Neumann.  
> **“The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases.”**  
> *ICDE 2013*.  
> <https://db.in.tum.de/~leis/papers/ART.pdf>

Quote from the abstract (emphasis added):

> “The **Adaptive Radix Tree (ART)** is a fast and space-efficient index
> structure for databases built on modern hardware. … ART **adaptively**
> chooses compact and efficient data structures for sparse and dense nodes.”

### How we embed ART in pages

Every node/leaf/blob is a `Copy` struct allocated through `PageManager`
(`TypedAlloc` / `alloc_slot`). Child pointers are `AllocId`s, not raw pointers,
so the graph remains meaningful after remap/reopen.

Public shape:

```rust
pub struct ArtTrie {
    root: Option<AllocId>,
}

impl ArtTrie {
    pub fn insert(&mut self, pages: &mut PageManager, key: &[u8], value: &[u8]);
    pub fn get<'a>(&'a mut self, pages: &'a mut PageManager, key: &[u8]) -> GetResult<'a>;
}
```

`GetResult` includes search stats (nodes visited, prefix bytes matched, …)
useful for tests and teaching path compression.

### Related index lore

Radix / trie indexes for databases are an old idea; ART’s contribution is
**space-efficient adaptive nodes** on modern CPUs. For broader context on
main-memory indexing trade-offs, see also work comparing ART, Masstree, and
B+trees in the early-2010s main-memory DB literature (Leis et al., and surveys
in *Database Internals* below).

---

## 6. Write-ahead log (WAL)

Module: `wal.rs`.

### What it is

An append-only log of **opaque** byte blocks. The WAL does **not** serialize
application objects; callers pass `&[u8]`.

On-disk framing:

```text
[len: u32 LE][payload: len bytes]
```

- `len == 0` means unused / preallocated space (end of logical log when scanning).
- Empty payloads are rejected (`WalError::EmptyPayload`).
- On open, the WAL scans for the last **complete** record and continues from
  that logical end; a torn trailing record is truncated.

### Append API

```rust
wal.append(block, AppendConfig { flush: FlushStrategy::… })
```

| `FlushStrategy` | Meaning |
|-----------------|--------|
| `None` | No explicit sync (fastest; crash may lose recent appends still in page cache) |
| `Data` | `File::sync_data` ≈ `fdatasync` |
| `All` | `File::sync_all` ≈ `fsync` |

Long-lived options (`Wal::with_options`): directory, file name, preallocate
chunk size, soft max segment size, notify channel capacity.

### Filesystem backend trait

```rust
pub trait FsApi { /* open, write, sync_data/all, sync_dir, set_len, … */ }
pub struct RustStdApi; // std::fs / std::io today
```

Future backends (ext4 `fallocate`, macOS `F_FULLFSYNC`, …) can plug in without
rewriting append logic. This follows Luu’s closing advice: package the hard
parts into a library rather than scattering raw syscalls through the app.

Classic systems text on write-ahead logging:

> C. Mohan et al. **“ARIES: A Transaction Recovery Method Supporting
> Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging.”**  
> *ACM TODS*, 1992.  
> <https://cs.stanford.edu/people/chrismre/cs345/rl/aries.pdf>

ARIES is far richer than our log (compensation records, CLRs, Steal/No-Force,
…); we cite it as the intellectual ancestor of “log first, then structure.”

---

## 7. Filesystem consistency (why flushes exist)

POSIX filesystems do **not** give you the mental model many programmers assume
(“`write` finishes ⇒ bytes are durable and ordered”).

Dan Luu, [*Files are hard*](https://danluu.com/file-consistency/):

> “Correctly reasoning about re-ordering behavior and inserting barriers
> correctly is hard. But even though shared memory concurrency is considered a
> hard problem that requires great care, **writing to files isn't treated the
> same way**, even though it's actually harder in a number of ways.”

The empirical backbone of that essay is:

> Thanumalayan Sankaranarayana Pillai et al.  
> **“All File Systems Are Not Created Equal: On the Complexity of Crafting
> Crash-Consistent Applications.”**  
> *OSDI 2014*.  
> <https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai>  
> PDF: <https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf>

Pillai et al. show that applications (LevelDB, HDFS, ZooKeeper, git, …)
commonly assume ordering or atomicity properties that specific filesystems
violate. The two most common bug classes they report:

1. assuming **ordering** between syscalls without barriers,
2. assuming **atomicity** of multi-sector updates.

### Design points we encode in `Wal` / `FsApi`

| # | Lesson | Our response |
|---|--------|--------------|
| 1 | Multi-byte writes can tear | Length prefix + scan/truncate incomplete tails |
| 2 | Writes reorder without barriers | Explicit `FlushStrategy` |
| 3 | File `fsync` ≠ directory durability | `FsApi::sync_dir` after creating a new segment |
| 4 | `data=ordered` / `writeback` / `journal` differ | Never assume cross-file order without flushes |
| 5 | Size can advance before data (`writeback`) | `len==0` ends scan; empty payloads forbidden |
| 6 | `fsync` may not be a true device flush | `FsApi` seam for stronger barriers later |
| 7 | Silent disk corruption happens | Callers may checksum inside opaque blocks; framing limits blast radius |
| 8 | Metadata updates on extend are expensive | Optional preallocation chunks |
| 9 | Ad-hoc syscalls are hazardous | Centralize in `FsApi` |

On ext* `data=` modes, the kernel docs themselves are famously informal. Luu
quotes the manpage’s “rumour” language about `writeback` performance—treat
filesystem folklore as folklore; verify with barriers you control.

For macOS history: full durability historically required
`fcntl(F_FULLFSYNC)` rather than plain `fsync`. Our `RustStdApi` uses Rust’s
`sync_data` / `sync_all`; a platform-specific `FsApi` can go further.

Disks that ignore flush commands are discussed in Rajimwale et al. (DSN); if
you care about lying disks, that rabbit hole is separate from application-level
protocols.

---

## 8. Dual-root snapshots (design)

### The problem

Because `put` updates the ART **before** the WAL is necessarily durable, the
live trie can be **ahead** of what a crash would retain in the log.

A snapshot that is safe to reload must be **implied by a flushed WAL prefix**:
every key/value reachable from that snapshot’s root must appear in
`WAL[0 .. durable_wal_offset)` as flushed bytes.

### Dual-root idea

Keep two logical cursors:

| | Live | Durable |
|--|------|---------|
| Root | `live_root` | `durable_root` |
| WAL cursor | `live_wal_offset` | `durable_wal_offset` |
| Updated when | every `put` | explicit publish only |
| After crash | discard | load this |

**Publish protocol (sketch):**

1. Let `L` be the current live WAL offset (end of last applied record).
2. Flush the WAL through `L`.
3. Flush every page reachable from the trie version you want to freeze.
4. Persist `(durable_root, durable_wal_offset = L)` in page-0 custom meta and
   sync meta (and preferably the directory if names changed).

**Recovery:** load durable pair; optionally replay
`WAL[durable_wal_offset .. flushed_tip]` into a new live root.

### The aliasing trap

If `live_root` and `durable_root` share mutable nodes, the next live `put`
**mutates history** that crash recovery still considers durable.

So you need one of:

1. **Copy-on-write (COW)** — after publish, durable pages become immutable;
   live allocates copies on write (classic shadow paging / MVCC pages).
2. **Checkpoint clone** — durable is a separate frozen graph or file copy.
3. **Checkpoint meta + WAL replay only** — don’t keep a second live root;
   durable is just `(wal_offset)` (and maybe a root taken while quiet), and
   recovery rebuilds by replaying.

Status in this repo: the dual-root **protocol is documented**
(`storage/mod.rs`); a full publish API (and COW vs clone choice) is still
pending product/design confirmation.

### Systems analogies

- **ARIES** separates volatile buffer pool state from durable log + page LSNs.
- **LMDB** uses COW B+trees and a single-writer model so readers see a stable
  snapshot without blocking writers.
- **Postgres** checkpoints push dirty buffers and advance a redo location;
  crash recovery replays WAL from the last checkpoint.

Our dual-root sketch is closer to “explicit checkpoint root + redo cursor”
than to full ARIES, but the *invariant* is the same: never claim a structure
is durable past the log prefix that justifies it.

---

## 9. Hands-on: open, put, get, reopen

```rust
use milkyapps_core::storage::{
    AppendConfig, FlushStrategy, Storage, StorageOptions,
};

fn demo(dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Storage::open(StorageOptions::new(dir))?;

    // Fast path: no WAL sync on this append.
    db.put(b"user:1", b"alice", AppendConfig::no_flush())?;

    // Stronger: sync data after this record.
    db.put(b"user:2", b"bob", AppendConfig::flush_data())?;

    assert_eq!(db.get(b"user:1").value, Some(b"alice".as_slice()));

    // Optional: push WAL / pages explicitly.
    db.flush_wal(FlushStrategy::All)?;
    db.flush_pages(true)?;

    let cursor = db.consumer();
    println!("next wal offset to process: {}", cursor.wal_offset);
    Ok(())
}
```

Standalone ART tests open a pair:

```rust
let (mut pages, mut trie) = ArtTrie::open_with_options(path, 65536, 16)?;
trie.insert(&mut pages, b"k", b"v");
let got = trie.get(&mut pages, b"k");
```

WAL-only:

```rust
let (mut wal, rx) = Wal::with_options(WalOptions::new(dir, "wal.log"))?;
let ev = wal.append(b"hello", AppendConfig::flush_data())?;
assert_eq!(rx.recv(), Some(ev));
```

Keep the notify `Receiver` alive for as long as you append; dropping it closes
the channel and makes later `append` fail with `NotifyClosed`.

---

## 10. Deterministic simulation testing

Crash schedules live under `tests/dst/` and are driven by `src/sim/`.

Full guide: **[`DST.md`](../../DST.md)** (architecture, TOML ops, limits,
how DST relates to loom / miri / insta).

Minimal Rust sketch:

```rust
use milkyapps_core::sim::Sim;
use milkyapps_core::storage::{AppendConfig, Storage, StorageOptions};

let sim = Sim::new(1);
let mut opts = StorageOptions::new("/sim/demo");
opts.wal = opts.wal.clone().without_preallocate().max_file_bytes(0).notify_capacity(8);

{
    let mut db = Storage::open_sim(&sim, opts.clone())?;
    db.put(b"k", b"v", AppendConfig::flush_all())?;
    db.sync_sim_pages(&sim)?;
}
sim.crash();
let mut db = Storage::open_sim(&sim, opts)?;
assert_eq!(db.get(b"k").value, Some(b"v".as_slice()));
```

Or author a fixture:

```bash
just test-sim                 # cargo test --test dst
# edit tests/dst/*.toml
```

---

## 11. Module map

```text
src/
  sim/            // Sim, SimFs, SimRng — DST world (see DST.md)
  storage/
    mod.rs        // module docs (dual-root), re-exports
    storage.md    // this tutorial
    stateofart.md // production-engine gap checklist
    store.rs      // Storage<A: FsApi>, StorageConsumer, on-disk meta
    wal.rs        // Wal, FsApi, RustStdApi, framing, flush strategies
    pagemgr.rs    // PageManager (mmap | memory), MetadataPage, AllocId
    arttrie.rs    // ArtTrie over PageManager slots

tests/
  pagemgr/        // PageManager insta fixtures (mmap tempfile)
  arttrie/        // ART insta fixtures
  dst/            // DST crash/reopen fixtures (Sim)
```

Crate entry:

- `milkyapps_core::storage::*` — preferred path for the storage stack
- `milkyapps_core::sim::*` — DST world (`Sim`, `SimFs`, …)
- `milkyapps_core::pagemgr` / `::wal` — re-exported modules for older paths
- `milkyapps_core::collections::arttrie` — re-exports `storage::arttrie`

---

## 12. Further reading

### Crash consistency & filesystems

- Dan Luu, [*Files are hard*](https://danluu.com/file-consistency/) — narrative
  survey; best on-ramp.
- Pillai et al., OSDI ’14, *All File Systems Are Not Created Equal* — measurement
  of FS properties and app bugs  
  (<https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf>).
- Prabhakaran et al., SOSP ’05 — how file systems handle disk faults (error
  propagation; cited by Luu).
- Gunawi et al., FAST ’08 / OSDI ’08 — static analysis of ignored errors; `fsck`
  recovery hazards.
- Bairavasundaram et al., SIGMETRICS ’07 — latent sector errors & silent
  corruption rates.
- Chidambaram et al., FAST ’12 — separating ordering vs durability primitives.
- Rajimwale et al., DSN — protocols when disks ignore flush.

### Databases & logging

- Mohan et al., *ARIES* (TODS 1992) — write-ahead logging bible  
  (<https://cs.stanford.edu/people/chrismre/cs345/rl/aries.pdf>).
- Alex Petrov, ***Database Internals*** (O’Reilly, 2019) — modern tour of
  storage engines, B-trees, LSM, WAL, and recovery at a systems level.
- Gray & Reuter, ***Transaction Processing: Concepts and Techniques***
  (Morgan Kaufmann, 1993) — classical Steal/No-Force, checkpointing, logging.

### Indexes

- Leis, Kemper, Neumann, *The Adaptive Radix Tree* (ICDE 2013)  
  (<https://db.in.tum.de/~leis/papers/ART.pdf>).
- Mao, Kohler, Morris, *Cache Craftiness for Fast Multicore Key-Value Storage*
  (Masstree; EuroSys 2012) — another high-performance in-memory trie/tree design
  for comparison.

### mmap & allocators

- POSIX `mmap` / `msync` man pages — what `flush_range` is approximating.
- Size-class / slab allocators (jemalloc, mimalloc design notes) — conceptual
  cousins of `PageManager`’s power-of-two classes (ours are persistence-oriented).

---

## 13. Glossary

| Term | Meaning here |
|------|----------------|
| **AllocId** | Opaque handle `(size, page_id, slot_id)` naming a live slot in the page file. |
| **ART** | Adaptive Radix Tree — byte-key index with adaptive node sizes and prefix compression. |
| **Checkpoint** | Point where structure + log cursor are made jointly durable. |
| **COW (copy-on-write)** | Updates allocate new pages instead of mutating shared durable pages. |
| **Custom data** | Bytes in page 0 after `MetadataPage`; used for `StorageConsumer` / roots. |
| **DST** | Deterministic simulation testing — see [`DST.md`](../../DST.md). |
| **Durable (sim)** | Bytes / names that survive `Sim::crash` (last successful sync). |
| **Durable root** | ART root safe to load after crash; justified by a flushed WAL prefix. |
| **fdatasync / fsync** | Syscalls asking the OS to push file data (and for `fsync`, metadata) toward stable storage. |
| **FlushStrategy** | Per-append WAL durability: `None`, `Data`, or `All`. |
| **FsApi** | Trait abstracting filesystem ops so WAL logic isn’t tied to `std` alone (`RustStdApi`, `SimFs`). |
| **Live root** | Current ART root, possibly ahead of durable WAL. |
| **Logical page size** | File-format slab size in `MetadataPage`; independent of OS page size. |
| **LSN** | Log sequence number (ARIES); analogous role to our WAL byte offsets. |
| **MetadataPage** | Fixed header at the start of page 0 (page size, free lists, dirty threshold). |
| **Opaque bytes** | WAL payloads the log does not interpret; framing is only length (+ scan rules). |
| **PageManager** | Size-class slab allocator / mini buffer pool (mmap or memory DST backend). |
| **Preallocation** | Extending WAL file length ahead of the write cursor to reduce metadata churn. |
| **Redo** | Re-applying logged work after a crash (as opposed to undo). |
| **Sim / SimFs** | In-memory DST world and its `FsApi` implementation (`src/sim/`). |
| **Size class** | Bucket of slots with size `2^i`. |
| **Steal / No-Force** | ARIES buffer policies: dirty pages may be written before commit (Steal); commits need not force all pages (No-Force)—the log carries redo. |
| **StorageConsumer** | `{ wal_offset }` cursor: next WAL offset to process for recovery / catch-up. |
| **Torn write** | Crash leaves a partial multi-byte write; neither old nor new value intact. |
| **Volatile (sim)** | Bytes accepted by write APIs but not yet synced; lost on `Sim::crash`. |
| **WAL** | Write-ahead log — append-only redo stream consulted for durability/recovery. |
| **WalEvent** | Notification `{ offset, payload_len }` after a successful append. |

---

## Closing checklist for contributors

1. Treat every cross-file ordering assumption as false until an explicit flush
   says otherwise ([Pillai et al.][pillai], [Luu][luu]).
2. Prefer extending `FsApi` over sprinkling platform `#cfg` in call sites.
3. Never publish a durable ART root past a WAL offset that hasn’t been flushed.
4. If dual-root shares pages with live updates, you need COW or a clone—aliases
   will silently corrupt “durable” history.
5. Keep WAL payloads opaque at the log layer; put checksums / encodings inside
   the bytes if you need them.
6. Add crash/durability claims as `tests/dst/*.toml` fixtures ([`DST.md`][dst]);
   keep layout regressions in pagemgr/arttrie insta suites.
7. When unsure, re-read [*Files are hard*][luu] and the OSDI ’14 paper before
   adding a clever “optimization” that skips a sync.

[dst]: ../../DST.md

[luu]: https://danluu.com/file-consistency/
[pillai]: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf
