//! Join handle for tasks spawned on the async runtime.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::sync::atomic_option::AtomicOption;

/// A future that resolves to the output of a spawned task.
///
/// Awaiting a [`JoinHandle`] waits until the spawned future completes and
/// yields its result. Dropping the handle without awaiting does **not** cancel
/// the task; it continues running to completion.
pub struct JoinHandle<T> {
    pub(crate) receiver: crate::sync::oneshot::Receiver<T>,
    pub(crate) waker: Arc<AtomicOption<Waker>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        if let Some(output) = self.receiver.try_recv() {
            return Poll::Ready(output);
        }

        // Register this task's waker so we are rescheduled when the result is
        // ready. If the producing task finishes between the `is_done()` check
        // above and the lock below, the re-check after releasing the lock will
        // observe the ready value and avoid a lost wake.
        self.waker.replace(cx.waker().clone());

        // Re-check now that the waker is registered.
        if let Some(output) = self.receiver.try_recv() {
            // Clear the waker so a stale wake does not reschedule a completed
            // task unnecessarily.
            let _ = self.waker.take();
            Poll::Ready(output)
        } else {
            Poll::Pending
        }
    }
}
