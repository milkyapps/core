//! Task representation and scheduling for the async runtime.

use crate::async_rt::waker::waker_from_task;
use crate::sync::atomic::{AtomicU8, Ordering};
use crate::sync::atomic_option::AtomicOption;
use crate::sync::bounded::Sender;
use std::cell::UnsafeCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// Scheduling state for a task, encoded in a single atomic word.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
enum State {
    /// Not running, not queued, not completed. Waiting to be woken.
    Idle = 0,
    /// Queued (or about to be handed to the queue), not running.
    Scheduled = 1,
    /// Being polled, with no outstanding wake.
    Running = 2,
    /// Being polled, and a wake arrived during the poll. The poller's
    /// `Pending` branch must re-enqueue the task.
    RunningNotified = 3,
    /// Completed; terminal.
    Completed = 4,
}

/// Decodes a raw byte back into a [`State`].
///
/// Only values written via [`AtomicState::store`] are ever observed, so the
/// `unreachable!` arm cannot fire; it exists only to keep the match total.
fn decode_state(value: u8) -> State {
    match value {
        0 => State::Idle,
        1 => State::Scheduled,
        2 => State::Running,
        3 => State::RunningNotified,
        4 => State::Completed,
        _ => unreachable!("invalid task state {value}"),
    }
}

/// A single-word atomic holding a [`State`].
struct AtomicState {
    inner: AtomicU8,
}

impl AtomicState {
    fn new(state: State) -> Self {
        Self {
            inner: AtomicU8::new(state as u8),
        }
    }

    fn load(&self, ord: Ordering) -> State {
        decode_state(self.inner.load(ord))
    }

    fn store(&self, state: State, ord: Ordering) {
        self.inner.store(state as u8, ord);
    }

    fn compare_exchange(
        &self,
        current: State,
        new: State,
        success: Ordering,
        failure: Ordering,
    ) -> Result<State, State> {
        match self
            .inner
            .compare_exchange(current as u8, new as u8, success, failure)
        {
            Ok(v) => Ok(decode_state(v)),
            Err(v) => Err(decode_state(v)),
        }
    }
}

/// A runnable unit of work.
///
/// Each task owns a type-erased future, its scheduling state, and a back
/// pointer to the runtime queue so that wakers can reschedule it.
pub(crate) struct Task {
    state: AtomicState,
    sender: Sender<Arc<Task>>,
    future: UnsafeCell<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    pub(crate) waker: Arc<AtomicOption<Waker>>,
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("state", &self.state.load(Ordering::Relaxed))
            .field("waker", &self.waker)
            .finish_non_exhaustive()
    }
}

// SAFETY: The future is only accessed through `Task::run`, which takes the
// poll lock (the `Running` state). `Task` itself is `Send` because the future
// is `Send`.
unsafe impl Send for Task {}
// SAFETY: Only one thread at a time calls `poll` thanks to the `Running`
// state. The future is `Send`, and the queue pointer is immutable.
unsafe impl Sync for Task {}

impl Task {
    /// Creates a new task that will run the given future to completion.
    #[must_use]
    pub(crate) fn new<F>(
        future: F,
        sender: Sender<Arc<Task>>,
        waker: Arc<AtomicOption<Waker>>,
    ) -> Arc<Task>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Arc::new(Task {
            state: AtomicState::new(State::Idle),
            sender,
            future: UnsafeCell::new(Some(Box::pin(future))),
            waker,
        })
    }

    /// Schedules this task onto the runtime queue if it is not already
    /// queued, running, or completed.
    ///
    /// Each branch is a single atomic CAS, so a wake can never be left
    /// recorded in the state without someone acting on it:
    ///
    /// * `Idle` -> `Scheduled` enqueues the task ourselves.
    /// * `Running` -> `RunningNotified` leaves a flag for the active poller's
    ///   `Pending` branch to re-enqueue.
    /// * `Scheduled` / `RunningNotified` / `Completed` need no action.
    pub(crate) fn schedule(self: &Arc<Task>) {
        loop {
            // `Completed` is kept as its own arm even though it shares a body
            // with the "already covered" arm: it is a terminal state with a
            // different reason (drop a stale wake), not a redundant enqueue.
            #[allow(clippy::match_same_arms)]
            match self.state.load(Ordering::Acquire) {
                State::Completed => return,
                State::Idle => {
                    if self
                        .state
                        .compare_exchange(
                            State::Idle,
                            State::Scheduled,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.sender.send(self.clone()).unwrap();
                        return;
                    }
                    // Lost the race (state changed since the load); retry.
                }
                State::Running => {
                    if self
                        .state
                        .compare_exchange(
                            State::Running,
                            State::RunningNotified,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                    // Lost the race; retry.
                }
                // Already queued, or already notified-while-running: nothing
                // to do.
                State::Scheduled | State::RunningNotified => return,
            }
        }
    }

    /// Polls the task's future once.
    ///
    /// This is the heart of the executor. It ensures only one thread polls
    /// the future at a time and handles rescheduling when the future returns
    /// [`Poll::Pending`].
    pub(crate) fn run(self: Arc<Task>) {
        // Acquire the poll lock: `Scheduled` -> `Running`.
        loop {
            match self.state.load(Ordering::Acquire) {
                State::Completed => return,
                State::Scheduled => {
                    if self
                        .state
                        .compare_exchange(
                            State::Scheduled,
                            State::Running,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                    // Lost the race; retry.
                }
                // Already running (concurrent dequeue of the same task), or a
                // stale enqueue of an `Idle` task: hand it back to the
                // scheduler.
                _ => {
                    self.schedule();
                    return;
                }
            }
        }

        let waker = waker_from_task(self.clone());
        let mut cx = Context::from_waker(&waker);

        let poll_result = {
            // SAFETY: We hold the poll lock (`Running`), so no other thread is
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
                // SAFETY: We still hold the poll lock.
                let fut_opt = unsafe { &mut *self.future.get() };
                *fut_opt = None;
                // Mark completed *before* waking the joiner so a concurrent
                // `schedule()` observes the terminal state and becomes a
                // no-op instead of re-enqueuing a finished task.
                self.state.store(State::Completed, Ordering::Release);

                // Wake any task waiting on this task's result. Root tasks have
                // no join handle, so there is nothing to wake in that case.
                if let Some(waker) = self.waker.take() {
                    waker.wake();
                }
            }
            Poll::Pending => {
                // Atomically release the poll lock and observe whether a wake
                // arrived during the poll. This single CAS is what makes the
                // protocol race-free: `schedule()`'s `Running` ->
                // `RunningNotified` CAS and this CAS contend on the same
                // word, so exactly one of them wins and the wake is never
                // lost.
                //
                // * If `schedule()` won, the state is `RunningNotified` and we
                //   re-enqueue ourselves.
                // * If we won, the state becomes `Idle`; `schedule()`'s CAS
                //   then fails, it retries, sees `Idle`, and enqueues us.
                loop {
                    match self.state.load(Ordering::Acquire) {
                        State::Running => {
                            if self
                                .state
                                .compare_exchange(
                                    State::Running,
                                    State::Idle,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                return;
                            }
                            // A wake arrived; retry to observe `RunningNotified`.
                        }
                        State::RunningNotified => {
                            if self
                                .state
                                .compare_exchange(
                                    State::RunningNotified,
                                    State::Scheduled,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                self.sender.send(self.clone()).unwrap();
                                return;
                            }
                            // Retry.
                        }
                        // We hold the poll lock; only `schedule()` can mutate
                        // the state while we are `Running`/`RunningNotified`.
                        _ => unreachable!("task state changed while holding the poll lock"),
                    }
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
