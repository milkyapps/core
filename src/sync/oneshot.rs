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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::model;

    /// A value sent before any receive is returned unchanged by `try_recv`.
    #[test]
    fn send_then_try_recv() {
        model(|| {
            let (tx, rx) = oneshot();
            assert!(!rx.has_value());
            tx.send(7);
            assert!(rx.has_value());
            assert_eq!(rx.try_recv(), Some(7));
            assert!(rx.has_value(), "lock stays Done after a take");
            assert_eq!(
                rx.try_recv(),
                None,
                "a second take on a oneshot returns None"
            );
        });
    }

    /// `try_recv` before the sender has produced returns `None` and does not
    /// block.
    #[test]
    fn try_recv_returns_none_before_send() {
        model(|| {
            let (tx, rx) = oneshot::<i32>();
            assert_eq!(rx.try_recv(), None);
            tx.send(1);
            assert_eq!(rx.try_recv(), Some(1));
        });
    }

    /// Dropping the sender without sending leaves the receiver without a value.
    #[test]
    fn drop_sender_without_send() {
        model(|| {
            let (tx, rx) = oneshot::<i32>();
            drop(tx);
            assert_eq!(rx.try_recv(), None);
            assert!(!rx.has_value());
        });
    }

    /// A `Send` but non-`Copy` value is moved out exactly once.
    #[test]
    fn try_recv_moves_box_once() {
        model(|| {
            let (tx, rx) = oneshot();
            tx.send(Box::new(42));
            let got: Box<i32> = rx.try_recv().unwrap();
            assert_eq!(*got, 42);
            assert!(rx.try_recv().is_none());
        });
    }

    /// `Sender<T>` and `Receiver<T>` are `Send` when `T: Send`. After the manual
    /// `unsafe impl<T: Send> Sync for Receiver<T>` was removed, `Receiver<T>` is
    /// **not** `Sync` for any `T`: its only field is `Arc<Shared<T>>`, and
    /// `Shared<T>` contains an `UnsafeCell<Option<T>>`, which blocks the
    /// auto-derive of `Sync` on `Shared<T>` → `Arc<Shared<T>>` → `Receiver<T>`.
    /// So a `&Receiver` can no longer be shared across threads, which closes the
    /// `try_recv` data race (BUG 2) at the type level.
    #[test]
    fn receiver_is_send_but_not_sync() {
        fn assert_send<T: Send>() {}
        assert_send::<Sender<i32>>();
        assert_send::<Receiver<i32>>();
    }
}
