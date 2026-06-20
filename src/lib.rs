//! A lock-free Treiber stack with hazard-pointer-based memory reclamation.
//!
//! See [`TreiberStack`] for the main API.
//!
//! The crate exposes the hazard-pointer machinery used internally in
//! [`hazard`]. It is generic and could be reused by other lock-free
//! structures, though it is intentionally minimal.
//!
//! # Example
//!
//! ```ignore
//! use treiber_stack::TreiberStack;
//!
//! let s = TreiberStack::<i32>::new();
//! s.push(1);
//! s.push(2);
//! assert_eq!(s.pop(), Some(2));
//! assert_eq!(s.pop(), Some(1));
//! assert_eq!(s.pop(), None);
//! ```

#![warn(missing_docs)]

pub mod hazard_ptrs;
