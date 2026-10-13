//! Reusable collection data structures.

/// A bounded, lock-free multi-producer multi-consumer ring buffer.
pub mod ringbuffer;

#[doc(inline)]
pub use crate::storage::arttrie;
