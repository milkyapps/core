//! Durable storage layer: write-ahead log, page manager, and ART index.
//!
//! # Dual-root snapshots (design)
//!
//! The live path appends to the WAL and updates the ART **without** waiting for
//! a WAL flush. That means the in-memory / page-file trie can be **ahead** of
//! what is crash-safe on the WAL.
//!
//! A **dual-root** design keeps two logical roots over the same page file:
//!
//! - **`live_root` + `live_wal_offset`** — applied immediately on every write.
//!   May include records whose WAL bytes are still only in the kernel page
//!   cache (not `fdatasync`/`fsync`’d).
//! - **`durable_root` + `durable_wal_offset`** — published only after:
//!   1. every WAL byte in `[0, durable_wal_offset)` is flushed, and
//!   2. every page reachable from `durable_root` is flushed,
//!   3. then the durable pair is written into page-0 custom metadata (and
//!      preferably synced).
//!
//! After a crash you load **`durable_*` only**, then optionally replay
//! `WAL[durable_wal_offset .. flushed_tip]` into a fresh live root. You never
//! trust `live_*` from disk unless it was published through that barrier.
//!
//! Critical detail: if `live_root` and `durable_root` share nodes, mutating the
//! live trie **corrupts** the durable snapshot. So either:
//! - use copy-on-write so durable pages become immutable after publish, or
//! - treat “durable” as a checkpoint of `(root, wal_offset)` taken while the
//!   tree is quiet / cloned, and accept that in-memory live continues on a
//!   diverging graph.
//!
//! Snapshot APIs for dual-root are pending confirmation of this approach.

#![allow(clippy::module_name_repetitions)]

/// Adaptive radix tree index.
pub mod arttrie;
/// Memory-mapped size-class page / slab manager.
pub mod pagemgr;
/// Storage façade composing WAL + pages + ART.
mod store;
/// Append-only write-ahead log.
pub mod wal;

pub use arttrie::{ArtTrie, GetResult, GetStats};
pub use pagemgr::{
    AllocId, DEFAULT_PAGE_SIZE, MetadataPage, PageId, PageManager, SlotId, TypedAlloc,
};
pub use store::{Storage, StorageConsumer, StorageError, StorageOptions};
pub use wal::{
    AppendConfig, DEFAULT_MAX_FILE_BYTES, DEFAULT_NOTIFY_CAPACITY, DEFAULT_PREALLOCATE_BYTES,
    FlushStrategy, FsApi, LENGTH_PREFIX_LEN, RustStdApi, Wal, WalError, WalEvent, WalOptions,
};
