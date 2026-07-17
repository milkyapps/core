use crate::sync::atomic::AtomicUsize;
use std::{
    cell::UnsafeCell,
    sync::{Arc, atomic::Ordering},
};

/// Creates a `oneshot` channel. Which means the sender can only send one item,
/// and the receiver side can only take one item.
pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        lock: AtomicUsize::new(0),
        value: UnsafeCell::new(None),
    });

    let s = Sender {
        shared: shared.clone(),
    };

    let r = Receiver { shared };

    (s, r)
}

struct Shared<T> {
    // 0 = No Value, 1 = Copying, 2 = Done
    lock: AtomicUsize,
    value: UnsafeCell<Option<T>>,
}

/// Sender side of the oneshot.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

unsafe impl<T: Send> Send for Sender<T> {}

impl<T> Sender<T> {
    /// Send the unique value for the channel. That consumer the `Sender`.
    pub fn send(self, item: T) {
        // lock
        unsafe {
            self.shared
                .lock
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .unwrap_unchecked();
        }

        unsafe {
            *self.shared.value.get() = Some(item);
        }

        //unlock
        unsafe {
            self.shared
                .lock
                .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
                .unwrap_unchecked();
        }
    }
}

/// Receiver side of the oneshot channel
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

unsafe impl<T: Send> Sync for Receiver<T> {}
unsafe impl<T: Send> Send for Receiver<T> {}

impl<T> Receiver<T> {
    /// Checks if the channel has its unique value
    #[must_use]
    pub fn has_value(&self) -> bool {
        self.shared.lock.load(Ordering::Acquire) == 2
    }

    /// Take the channel's unique value, if there is any.
    /// If not, returns `None`.
    #[must_use]
    pub fn try_recv(&self) -> Option<T> {
        if self.has_value() {
            unsafe { (*self.shared.value.get()).take() }
        } else {
            None
        }
    }
}
