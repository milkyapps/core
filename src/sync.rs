//! Re-exports [`std::sync`] (and [`std::cell::UnsafeCell`]) in normal builds, and
//! the equivalent [`loom`] primitives when the `loom` cfg is enabled.

// `UnsafeCell` is always re-exported from `std`: loom's `UnsafeCell` exposes a
// different API (its `get` returns `ConstPtr`/`MutPtr` rather than `*mut T`) and
// the hazard pointers code relies on `*mut T` from `get`. The concurrent
// mutation of the cell's contents happens through the (loom-modeled) atomics
// stored inside it, so keeping `std::cell::UnsafeCell` here does not weaken
// loom's modeling.
pub use std::cell::UnsafeCell;

#[cfg(not(loom))]
#[allow(unused_imports)]
pub(crate) use std::sync::{
    Arc, Barrier,
    atomic::{self, AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

#[cfg(loom)]
#[allow(unused_imports)]
pub use loom::sync::{
    Arc,
    atomic::{self, AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

// `loom::sync::Barrier` is currently only a stub that panics, so under the
// `loom` cfg a real implementation backed by `loom::sync::Mutex` and
// `loom::sync::Condvar` is provided instead.
#[cfg(loom)]
mod barrier {
    //! A loom-modelable [`Barrier`].
    //!
    //! `loom::sync::Barrier` is currently only a stub that panics, so a real
    //! implementation backed by `loom::sync::Mutex` and `loom::sync::Condvar` is
    //! provided here, mirroring [`std::sync::Barrier`].

    use loom::sync::{Condvar, Mutex};

    struct State {
        /// Number of threads still required to reach the barrier.
        count: usize,
        /// Current barrier generation, incremented each time the barrier is tripped.
        generation: usize,
    }

    /// A reusable barrier, mirroring [`std::sync::Barrier`].
    pub struct Barrier {
        n: usize,
        state: Mutex<State>,
        cvar: Condvar,
    }

    impl Barrier {
        /// Creates a new barrier that blocks until `n` threads have called
        /// [`Barrier::wait`].
        #[must_use]
        pub fn new(n: usize) -> Barrier {
            Barrier {
                n,
                state: Mutex::new(State {
                    count: 0,
                    generation: 0,
                }),
                cvar: Condvar::new(),
            }
        }

        /// Blocks the current thread until all `n` threads have called `wait`.
        ///
        /// # Panics
        ///
        /// Panics if the internal [`Mutex`] or [`Condvar`] is poisoned. This never
        /// happens under loom (which runs on a single OS thread), and mirrors the
        /// poisoning behavior of [`std::sync::Barrier`].
        pub fn wait(&self) {
            let mut guard = self.state.lock().unwrap();
            let generation = guard.generation;
            guard.count += 1;
            if guard.count == self.n {
                guard.count = 0;
                guard.generation = generation + 1;
                self.cvar.notify_all();
            } else {
                while guard.generation == generation {
                    guard = self.cvar.wait(guard).unwrap();
                }
            }
        }
    }
}

#[cfg(loom)]
pub use barrier::*;

/// Runs `f` inside a loom model when the `loom` cfg is enabled, otherwise
/// runs `f` directly. Test bodies run inside the model so the loom-modeled
/// atomics have a runtime to execute against.
#[cfg(loom)]
#[cfg(test)]
pub(crate) fn model<F>(f: F)
where
    F: Fn() + Send + Sync + 'static,
{
    loom::model(f);
}

#[cfg(not(loom))]
#[cfg(test)]
pub(crate) fn model<F>(f: F)
where
    F: FnOnce(),
{
    f();
}
