/// Freelist module
pub mod freelist;
// /// Multipool module
// pub mod multipool;
// NOTE: temporarily disabled — the multipool WIP in `src/allocators/multipool.rs`
// does not compile yet (uses `FreeList` instead of `Freelist`, treats
// `alloc()`'s `Option<TaggedPtr>` as a raw pointer, non-exhaustive match).
// Re-enable once that module is finished. The freelist / hazard-pointer /
// ptr / sync tests do not depend on it.
