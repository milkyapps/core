use crate::sync::atomic::AtomicUsize;
use std::{cell::UnsafeCell, hint::spin_loop, sync::atomic::Ordering};

/// Option protected by a spin lock.
pub struct AtomicOption<T> {
    lock: AtomicUsize,
    data: UnsafeCell<Option<T>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for AtomicOption<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicOption")
            .field("lock", &self.lock)
            .field("data", &self.data)
            .finish()
    }
}

impl<T> Default for AtomicOption<T> {
    fn default() -> Self {
        Self {
            lock: AtomicUsize::new(0),
            data: UnsafeCell::new(None),
        }
    }
}

unsafe impl<T: Send> Sync for AtomicOption<T> {}
unsafe impl<T: Send> Send for AtomicOption<T> {}

impl<T> AtomicOption<T> {
    fn lock(&self) {
        while self
            .lock
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            spin_loop();
        }
    }

    fn unlock(&self) {
        unsafe {
            self.lock
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .unwrap_unchecked();
        }
    }

    /// Replace the current value with item, and return the old value.
    /// This fucntion will spin until it get unique write access.
    pub fn replace(&self, item: T) -> Option<T> {
        self.lock();
        let old = unsafe { (*self.data.get()).replace(item) };
        self.unlock();

        old
    }

    /// Return the current value and put `None` in place.
    pub fn take(&self) -> Option<T> {
        self.lock();
        let old = unsafe { (*self.data.get()).take() };
        self.unlock();

        old
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

    /// Regression test for the `AtomicWaker` data race (BUG 1 in BUGS.txt).
    ///
    /// The previous lock-free `AtomicWaker::take` performed a non-atomic
    /// `Option::take` on the `UnsafeCell` with no exclusion, so two concurrent
    /// `take`s (or a `take` racing a `register`) were a data race. `AtomicOption`
    /// guards every cell access with a CAS spin lock, so two concurrent `take`s
    /// must now be mutually exclusive. This test fails under Miri (data race on
    /// `*self.data.get()`) if the locking is removed or bypassed, and passes
    /// under Miri with the lock in place.
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

    /// Regression test for the `AtomicWaker` data race (BUG 1 in BUGS.txt).
    ///
    /// A concurrent `replace` (writer) and `take` (reader/writer) must be
    /// mutually exclusive. Under the old `AtomicWaker` this was a data race
    /// because `take` did not participate in the writer side's state machine.
    /// Under `AtomicOption` both go through the spin lock, so this must pass
    /// under Miri.
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
