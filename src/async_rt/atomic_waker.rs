use std::{
    cell::UnsafeCell,
    hint::spin_loop,
    sync::atomic::Ordering,
    task::{Context, Waker},
};

use crate::sync::AtomicUsize;

/// A simple AtomicWaker-like structure for a oneshot channel.
///
/// States:
/// 0: Unregistered (No waker exists)
/// 1: Registered (A valid waker is stored)
/// 2: `PendingRegistration` (Someone is currently writing to the `UnsafeCell`)
pub struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

impl Default for AtomicWaker {
    fn default() -> Self {
        Self {
            state: AtomicUsize::new(0), // Start as Unregistered
            waker: UnsafeCell::new(None),
        }
    }
}

impl AtomicWaker {
    /// Registers the waker from the Context.
    /// If a waker is already registered, it updates it.
    pub fn register(&self, context: &Context) {
        let mut state = self.state.load(Ordering::Acquire);

        loop {
            // If we are in "Pending" state (2), another thread is registering.
            while state == 2 {
                spin_loop();
                state = self.state.load(Ordering::Acquire);
            }

            // Attempt to move from Unregistered (0) or Registered (1) into Pending (2)
            // This ensures only one thread can write to the waker at a time.
            if state == 0 || state == 1 {
                match self
                    .state
                    .compare_exchange(state, 2, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => {
                        // We "own" the registration process now.
                        let waker = context.waker().clone();
                        unsafe {
                            *self.waker.get() = Some(waker);
                        }

                        // Set state to Registered (1)
                        self.state.store(1, Ordering::Release);
                        break;
                    }
                    Err(s) => {
                        state = s;
                    }
                }
            }
        }
    }

    /// Returns the waker if one is registered.
    pub fn take(&self) -> Option<Waker> {
        let state = self.state.load(Ordering::Acquire);
        if state == 1 {
            // Safety: State is 1, so we know a valid Waker exists.
            unsafe { (*self.waker.get()).take() }
        } else {
            None
        }
    }
}
