//! Join handle for tasks spawned on the async runtime.

use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

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

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinHandle")
            .field("done", &self.inner.is_done())
            .finish()
    }
}

/// Internal oneshot cell shared between the spawned task and the join handle.
pub(crate) struct JoinCell<T> {
    done: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
    /// The waker of the task currently awaiting this result, if any.
    ///
    /// A mutex is used so that the producing task can take the waker and wake
    /// it without racing with the consumer that stores it.
    waker: std::sync::Mutex<Option<Waker>>,
}

// SAFETY: `JoinCell` is protected by atomic flags and a mutex. Both the value
// and waker are `Send` when `T: Send`.
unsafe impl<T: Send> Send for JoinCell<T> {}
unsafe impl<T: Send> Sync for JoinCell<T> {}

impl<T: Send + 'static> Joinable for JoinCell<T> {
    fn wake_waiter(&self) {
        let mut guard = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(waker) = guard.take() {
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
            value: UnsafeCell::new(MaybeUninit::uninit()),
            waker: std::sync::Mutex::new(None),
        }
    }

    /// Stores the output.
    ///
    /// # Safety
    ///
    /// Must be called exactly once by the producing task.
    pub(crate) unsafe fn set_output(&self, value: T) {
        unsafe {
            (*self.value.get()).write(value);
        }
        self.done.store(true, Ordering::Release);
    }

    /// Returns `true` once the output has been produced.
    pub(crate) fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Takes the produced output.
    ///
    /// # Safety
    ///
    /// Must only be called after `is_done()` returns `true`, and at most once.
    pub(crate) unsafe fn take_output(&self) -> T {
        unsafe { (*self.value.get()).assume_init_read() }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        if self.inner.is_done() {
            // SAFETY: `is_done()` guarantees the value is initialized.
            return Poll::Ready(unsafe { self.inner.take_output() });
        }

        // Register this task's waker so we are rescheduled when the result is
        // ready. If the producing task finishes between the `is_done()` check
        // above and the lock below, the re-check after releasing the lock will
        // observe the ready value and avoid a lost wake.
        {
            let mut guard = self
                .inner
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = Some(cx.waker().clone());
        }

        // Re-check now that the waker is registered.
        if self.inner.is_done() {
            // Clear the waker so a stale wake does not reschedule a completed
            // task unnecessarily.
            let mut guard = self
                .inner
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.take();
            // SAFETY: `is_done()` guarantees the value is initialized.
            return Poll::Ready(unsafe { self.inner.take_output() });
        }
        Poll::Pending
    }
}
