//! A minimal, complete async runtime backed by a fixed-size thread pool.
//!
//! # Overview
//!
//! [`Runtime`] owns a set of worker threads and a shared task queue. Futures
//! can be driven to completion with [`Runtime::block_on`] or spawned onto the
//! pool with [`Handle::spawn`]. The runtime implements the standard [`Future`]
//! contract using a custom [`std::task::Waker`] that reschedules tasks onto the
//! queue.
//!
//! # Example
//!
//! ```
//! use milkyapps_core::async_rt::Runtime;
//!
//! let rt = Runtime::new(2);
//! let handle = rt.handle();
//!
//! let value = rt.block_on(async move {
//!     handle.spawn(async { 42 }).await
//! });
//!
//! assert_eq!(value, 42);
//! ```

pub mod join_handle;
pub mod runtime;
pub mod task;
pub mod waker;
#[allow(clippy::module_inception)]
pub(crate) mod worker;

pub use join_handle::JoinHandle;
pub use runtime::{Handle, Runtime};
pub use task::yield_now;
