//! Task representation and scheduling for the async runtime.

use crate::sync::atomic::{AtomicBool, Ordering};
use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::async_rt::join_handle::Joinable;
use crate::async_rt::waker::waker_from_task;
use crate::sync::bounded::Sender;

/// A runnable unit of work.
///
/// Each task owns a type-erased future, its scheduling state, and a back
/// pointer to the runtime queue so that wakers can reschedule it.
pub(crate) struct Task {
    scheduled: AtomicBool,
    running: AtomicBool,
    completed: AtomicBool,
    sender: Sender<Arc<Task>>,
    future: UnsafeCell<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    pub(crate) join: Option<Arc<dyn Joinable + Send + Sync>>,
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("scheduled", &self.scheduled.load(Ordering::Relaxed))
            .field("running", &self.running.load(Ordering::Relaxed))
            .field("completed", &self.completed.load(Ordering::Relaxed))
            .field("has_join", &self.join.is_some())
            .finish_non_exhaustive()
    }
}

// SAFETY: The future is only accessed through `Task::run`, which takes the
// `running` lock. `Task` itself is `Send` because the future is `Send`.
unsafe impl Send for Task {}
// SAFETY: Only one thread at a time calls `poll` thanks to `running`. The
// future is `Send`, and the queue pointer is immutable.
unsafe impl Sync for Task {}

impl Task {
    /// Creates a new task that will run the given future to completion.
    #[must_use]
    pub(crate) fn new<F>(
        future: F,
        sender: Sender<Arc<Task>>,
        join: Option<Arc<dyn Joinable + Send + Sync>>,
    ) -> Arc<Task>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Arc::new(Task {
            scheduled: AtomicBool::new(false),
            running: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            sender,
            future: UnsafeCell::new(Some(Box::pin(future))),
            join,
        })
    }

    /// Schedules this task onto the runtime queue if it is not already
    /// scheduled, running, or completed.
    pub(crate) fn schedule(self: &Arc<Task>) {
        let completed = self.completed.load(Ordering::Acquire);
        let running = self.running.load(Ordering::Acquire);
        if completed {
            return;
        }

        // If the task is currently being polled, just set the scheduled flag.
        // The poller will re-enqueue it after the current poll returns Pending.
        if running {
            self.scheduled.store(true, Ordering::Release);
            return;
        }

        if self.scheduled.swap(true, Ordering::AcqRel) {
            return;
        }

        self.sender.send(self.clone()).unwrap();
    }

    /// Polls the task's future once.
    ///
    /// This is the heart of the executor. It ensures only one thread polls
    /// the future at a time and handles rescheduling when the future returns
    /// [`Poll::Pending`].
    pub(crate) fn run(self: Arc<Task>) {
        if self.completed.load(Ordering::Acquire) {
            return;
        }

        // Acquire the poll lock. If another thread is already polling, mark
        // the task as scheduled so it gets another turn.
        if self.running.swap(true, Ordering::AcqRel) {
            self.schedule();
            return;
        }

        // Clear the scheduled flag before polling. Any wake that happens from
        // now until we finish will set it again and cause a reschedule.
        self.scheduled.store(false, Ordering::Release);

        let waker = waker_from_task(self.clone());
        let mut cx = Context::from_waker(&waker);

        let poll_result = {
            // SAFETY: We hold the `running` lock, so no other thread is
            // accessing the future. The future is pinned in place on the heap.
            let fut_opt = unsafe { &mut *self.future.get() };
            if let Some(fut) = fut_opt {
                fut.as_mut().poll(&mut cx)
            } else {
                Poll::Ready(())
            }
        };

        match poll_result {
            Poll::Ready(()) => {
                // The future has completed. Drop it and mark the task done.
                // SAFETY: We still hold the running lock.
                let fut_opt = unsafe { &mut *self.future.get() };
                *fut_opt = None;
                self.completed.store(true, Ordering::Release);
                self.running.store(false, Ordering::Release);

                // Wake any task waiting on this task's result. Root tasks have
                // no join handle, so there is nothing to wake in that case.
                if let Some(join) = &self.join {
                    join.wake_waiter();
                }
            }
            Poll::Pending => {
                self.running.store(false, Ordering::Release);
                let was_scheduled = self.scheduled.swap(false, Ordering::AcqRel);
                // If the future was woken while it was polling, schedule a
                // new turn.
                if was_scheduled {
                    self.sender.send(self.clone()).unwrap();
                }
            }
        }
    }
}

/// Yields execution back to the runtime, allowing other tasks to run.
///
/// The current task is rescheduled and will be polled again.
pub async fn yield_now() {
    struct YieldNow {
        yielded: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    YieldNow { yielded: false }.await;
}
