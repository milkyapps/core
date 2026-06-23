//! Set of utils and helpers to write apps in Rust.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]

/// Hazard Pointers module
pub mod hazard_ptrs;
/// SIMD helper functions
pub mod simd;
/// Concurrency primitives
pub(crate) mod sync;
/// Thread primitives
pub(crate) mod thread;
