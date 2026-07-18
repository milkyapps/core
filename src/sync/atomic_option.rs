#[cfg(not(loom))]
use crate::sync::spinlock::Spinlock;
use std::cell::UnsafeCell;

/// A single-slot cell holding `Option<T>` under exclusive access.
pub struct AtomicOption<T> {
    lock: Spinlock,
    data: UnsafeCell<Option<T>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for AtomicOption<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicOption").finish_non_exhaustive()
    }
}

impl<T> Default for AtomicOption<T> {
    fn default() -> Self {
        Self {
            lock: Spinlock::default(),
            data: UnsafeCell::new(None),
        }
    }
}

// SAFETY: both backing implementations provide exclusive access to the inner
// `Option<T>`; transferring access transfers ownership of `T`. `T: Send` is
// sufficient (the value is never shared as a borrow across threads).
unsafe impl<T: Send> Sync for AtomicOption<T> {}
unsafe impl<T: Send> Send for AtomicOption<T> {}

impl<T> AtomicOption<T> {
    /// Replace the current value with `item` and return the previous value.
    ///
    /// Spins until it acquires exclusive write access (std) or blocks on the
    /// modeled `Mutex` (Loom).
    pub fn replace(&self, item: T) -> Option<T> {
        let _g = self.lock.lock();
        // SAFETY: we hold the lock, so no other thread accesses `data`.
        unsafe { (*self.data.get()).replace(item) }
    }

    /// Return the current value and leave `None` in its place.
    pub fn take(&self) -> Option<T> {
        let _g = self.lock.lock();
        // SAFETY: we hold the lock, so no other thread accesses `data`.
        unsafe { (*self.data.get()).take() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::model;

    /// `take` on a fresh cell returns `None`.
    #[test]
    fn take_returns_none_when_empty() {
        model(|| {
            let a = AtomicOption::<u32>::default();
            assert!(a.take().is_none());
        });
    }

    /// `replace` stores a value and `take` retrieves it exactly once.
    #[test]
    fn replace_then_take_roundtrips() {
        model(|| {
            let a = AtomicOption::default();
            assert!(a.replace(7).is_none(), "first replace returns None");
            assert_eq!(a.take(), Some(7));
            assert!(a.take().is_none(), "second take returns None");
        });
    }

    /// `replace` returns the previously stored value when overwriting.
    #[test]
    fn replace_returns_previous() {
        model(|| {
            let a = AtomicOption::default();
            a.replace(1);
            assert_eq!(a.replace(2), Some(1));
            assert_eq!(a.take(), Some(2));
        });
    }

    /// Regression test for the `AtomicWaker` data race (BUG 1).
    ///
    /// The previous lock-free `AtomicWaker::take` performed a non-atomic
    /// `Option::take` on the `UnsafeCell` with no exclusion, so two concurrent
    /// `take`s were a data race. `AtomicOption` provides exclusive access to
    /// the inner `Option<T>` (spin lock in prod, `Mutex` under Loom), so two
    /// concurrent `take`s must be mutually exclusive: one gets the value, the
    /// other `None`. Fails under Miri if the exclusion is broken.
    #[test]
    fn take_and_take_do_not_race() {
        model(|| {
            let a = AtomicOption::default();
            a.replace(1u32);

            crate::thread::scope(|s| {
                let a = &a;
                s.spawn(move || {
                    let _ = a.take();
                });
                s.spawn(move || {
                    let _ = a.take();
                });
            });
        });
    }

    /// Regression test for the `AtomicWaker` data race (BUG 1).
    ///
    /// A concurrent `replace` (writer) and `take` (reader/writer) must be
    /// mutually exclusive. Under the old `AtomicWaker` this was a data race
    /// because `take` did not participate in the writer side's state machine.
    /// Under `AtomicOption` both go through the exclusive-access path, so this
    /// must pass under Miri.
    #[test]
    fn replace_and_take_do_not_race() {
        model(|| {
            let a = AtomicOption::default();

            crate::thread::scope(|s| {
                let a = &a;
                s.spawn(move || {
                    for i in 0..8u32 {
                        a.replace(i);
                    }
                });
                s.spawn(move || {
                    for _ in 0..8 {
                        let _ = a.take();
                    }
                });
            });
        });
    }
}
