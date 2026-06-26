//! Set of utils and helpers to write apps in Rust.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]

/// Allocators
pub mod allocators;
// Pointer module
pub(crate) mod ptr;
/// SIMD helper functions
pub mod simd;
/// Safe Memory Reclamation
pub mod smr;
/// Concurrency primitives
pub(crate) mod sync;
/// Thread primitives
pub(crate) mod thread;
