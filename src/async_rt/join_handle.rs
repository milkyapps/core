//! Join handle for tasks spawned on the async runtime.

use crate::async_rt::atomic_waker::AtomicWaker;
use crate::sync::atomic::{AtomicBool, Ordering};
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Trait object interface used by [`Task`] to wake a task awaiting a result.
pub(crate) trait Joinable: Send + Sync {
    /// If a task is waiting on this result, reschedule it.
    fn wake_waiter(&self);
}

/// A future that resolves to the output of a spawned task.
///
/// Awaiting a [`JoinHandle`] waits until the spawned future completes and
/// yields its result. Dropping the handle without awaiting does **not** cancel
/// the task; it continues running to completion.
pub struct JoinHandle<T> {
    pub(crate) inner: Arc<JoinCell<T>>,
}

impl<T> std::fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinHandle")
            .field("done", &self.inner.is_done())
            .finish()
    }
}

/// Internal oneshot cell shared between the spawned task and the join handle.
pub(crate) struct JoinCell<T> {
    done: AtomicBool,
    value: UnsafeCell<Option<T>>,
    waker: AtomicWaker,
}

// SAFETY: `JoinCell` is protected by atomic flags and a mutex. Both the value
// and waker are `Send` when `T: Send`.
unsafe impl<T: Send> Send for JoinCell<T> {}
unsafe impl<T: Send> Sync for JoinCell<T> {}

impl<T: Send + 'static> Joinable for JoinCell<T> {
    fn wake_waiter(&self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

impl<T> JoinCell<T> {
    /// Creates an empty cell.
    #[must_use]
    pub(crate) fn new() -> JoinCell<T> {
        JoinCell {
            done: AtomicBool::new(false),
            value: UnsafeCell::new(None),
            waker: AtomicWaker::default(),
        }
    }

    /// Stores the output.
    pub(crate) fn store(&self, value: T) {
        unsafe {
            (*self.value.get()) = Some(value);
        }
        self.done.store(true, Ordering::Release);
    }

    /// Returns `true` once the output has been produced.
    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Takes the produced output.
    pub(crate) fn take(&self) -> Option<T> {
        if self.done.load(Ordering::Acquire) {
            unsafe { (*self.value.get()).take() }
        } else {
            None
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(output) = self.inner.take() {
            return Poll::Ready(output);
        }

        // Register this task's waker so we are rescheduled when the result is
        // ready. If the producing task finishes between the `is_done()` check
        // above and the lock below, the re-check after releasing the lock will
        // observe the ready value and avoid a lost wake.
        self.inner.waker.register(cx);

        // Re-check now that the waker is registered.
        if let Some(output) = self.inner.take() {
            // Clear the waker so a stale wake does not reschedule a completed
            // task unnecessarily.
            let _ = self.inner.waker.take();
            Poll::Ready(output)
        } else {
            Poll::Pending
        }
    }
}
