//! Shared task queue used by the async runtime.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

use crate::async_rt::task::Task;

/// A mutex-backed FIFO queue of runnable tasks plus a shutdown flag.
pub(crate) struct Queue {
    inner: Mutex<VecDeque<Arc<Task>>>,
    condvar: Condvar,
    shutdown: AtomicBool,
}

impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.inner.lock().unwrap();
        f.debug_struct("Queue")
            .field("len", &guard.len())
            .field("shutdown", &self.shutdown.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Queue {
    /// Creates a new empty queue.
    #[must_use]
    pub(crate) fn new() -> Arc<Queue> {
        Arc::new(Queue {
            inner: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
        })
    }

    /// Pushes a task onto the back of the queue and wakes one waiter.
    pub(crate) fn push(&self, task: Arc<Task>) {
        let mut guard = self.inner.lock().unwrap();
        guard.push_back(task);
        self.condvar.notify_one();
    }

    /// Pops a task from the front of the queue.
    ///
    /// Blocks until a task is available or [`Queue::shutdown`] has been called.
    /// Uses a short timeout so that idle workers can observe shutdown even if
    /// they miss a spurious notification.
    pub(crate) fn pop(&self) -> Option<Arc<Task>> {
        use std::time::Duration;

        let mut guard = self.inner.lock().unwrap();
        loop {
            if let Some(task) = guard.pop_front() {
                return Some(task);
            }
            if self.shutdown.load(Ordering::Acquire) {
                return None;
            }
            let (new_guard, timed_out) = self
                .condvar
                .wait_timeout(guard, Duration::from_millis(50))
                .unwrap();
            let _ = timed_out;
            guard = new_guard;
        }
    }

    /// Signals all waiting consumers to exit.
    pub(crate) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.condvar.notify_all();
    }

    /// Pops a task from the front of the queue if one is available.
    ///
    /// Returns `None` immediately if the queue is empty, without blocking.
    pub(crate) fn try_pop(&self) -> Option<Arc<Task>> {
        let mut guard = self.inner.lock().unwrap();
        guard.pop_front()
    }
}
