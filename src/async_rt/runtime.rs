//! Runtime and handle for the async runtime.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use crate::async_rt::join_handle::{JoinCell, JoinHandle, Joinable};
use crate::async_rt::task::Task;
use crate::async_rt::worker;
use crate::sync::channel::{Receiver, Sender, bounded};

/// Shared state between a [`Runtime`] and its [`Handle`]s.
struct Shared {
    sender: Sender<Arc<Task>>,
    receiver: Receiver<Arc<Task>>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").finish()
    }
}

/// A thread-pool based async runtime.
///
/// The runtime owns a fixed number of worker threads that pull tasks from a
/// shared channel.
pub struct Runtime {
    shared: Arc<Shared>,
    threads: Vec<crate::thread::JoinHandle<()>>,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("threads", &self.threads.len())
            .field("shared", &self.shared)
            .finish()
    }
}

impl Runtime {
    /// Creates a runtime with the given number of worker threads.
    ///
    /// # Panics
    ///
    /// Panics if `worker_threads` cannot be used as a `Vec` capacity, which
    /// only happens for extremely large values.
    #[must_use]
    pub fn new(worker_threads: usize) -> Runtime {
        let capacity = (worker_threads * 1024).max(1024);
        let (sender, receiver) = bounded(capacity);

        let mut threads = Vec::with_capacity(worker_threads);
        for _ in 0..worker_threads {
            threads.push(worker::spawn(receiver.clone()));
        }

        let shared = Arc::new(Shared { sender, receiver });
        Runtime { shared, threads }
    }

    /// Returns a cloneable handle that can be used to spawn tasks.
    #[must_use]
    pub fn handle(&self) -> Handle {
        Handle {
            shared: self.shared.clone(),
        }
    }

    /// Runs a future to completion on the current thread.
    ///
    /// The calling thread drives the root future directly. `Runtime` must
    /// have worker threads to drive other tasks into completion.
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        use std::task::{Context, Poll};

        let mut future = Box::pin(future);
        let waker = crate::async_rt::waker::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // Drive the root future until it is ready
        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => {}
            }

            if let Some(task) = self.shared.receiver.try_recv() {
                task.run();
            } else {
                #[cfg(not(loom))]
                {
                    std::thread::park_timeout(std::time::Duration::from_millis(1));
                }

                #[cfg(loom)]
                {
                    loom::thread::yield_now();
                }
            }
        }
    }

    /// Shuts down the runtime, waits for worker threads to finish, and drops
    /// the runtime.
    ///
    /// Any tasks still in the queue are abandoned; in-flight tasks run until
    /// they next return [`Poll::Pending`], at which point they will observe
    /// the shutdown flag and stop.
    #[allow(clippy::must_use_candidate)]
    pub fn shutdown(mut self) -> bool {
        self.shutdown_impl()
    }

    fn shutdown_impl(&mut self) -> bool {
        self.shared.sender.shutdown();

        #[cfg(loom)]
        {
            let clean_shutdown = true;
            for t in self.threads.drain(..) {
                let _ = t.join();
            }
            clean_shutdown
        }

        #[cfg(not(loom))]
        {
            use std::time::{Duration, Instant};
            let mut clean_shutdown = true;
            let deadline = Instant::now() + Duration::from_secs(10);
            for t in self.threads.drain(..) {
                while Instant::now() < deadline && !t.is_finished() {
                    std::thread::park_timeout(Duration::from_millis(5));
                }

                if t.is_finished() {
                    let _ = t.join();
                } else {
                    // stuck in a non-yielding future: detach
                    clean_shutdown = false;
                }
            }
            clean_shutdown
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown_impl();
    }
}

/// A handle to a [`Runtime`] that can spawn tasks from any thread.
///
/// [`Handle`] is cheaply cloneable and is `Send + Sync`.
#[derive(Clone)]
pub struct Handle {
    shared: Arc<Shared>,
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("shared", &self.shared)
            .finish()
    }
}

// SAFETY: `Handle` only holds immutable shared state that is `Send + Sync`.
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Handle {
    /// Spawns a future onto the runtime.
    ///
    /// The task runs concurrently with other tasks and its result can be
    /// retrieved by awaiting the returned [`JoinHandle`].
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let join = Arc::new(JoinCell::<F::Output>::new());
        let join_clone = join.clone();
        let join_dyn: Arc<dyn Joinable + Send + Sync> = join.clone();

        let wrapped = async move {
            let result = future.await;
            // SAFETY: This cell is private to the spawned task and is read
            // only after `is_done()` becomes true.
            unsafe { join_clone.set_output(result) };
        };

        let task = Task::new(wrapped, self.shared.sender.clone(), Some(join_dyn));
        task.schedule();

        JoinHandle { inner: join }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_rt::task::yield_now;
    use crate::sync::model;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// `block_on` must return the value produced by a simple async block.
    #[test]
    fn block_on_returns_value() {
        model(|| {
            let rt = Runtime::new(1);
            let value = rt.block_on(async { 42 });
            assert_eq!(value, 42);
            rt.shutdown();
        });
    }

    /// A spawned task can be awaited from another task.
    #[test]
    fn spawn_and_await() {
        model(|| {
            let rt = Runtime::new(1);
            let handle = rt.handle();

            let value = rt.block_on(async move { handle.spawn(async { 7 * 6 }).await });

            assert_eq!(value, 42);
            rt.shutdown();
        });
    }

    /// `yield_now` lets two spawned tasks make progress.
    #[test]
    fn yield_now_interleaves_tasks() {
        model(|| {
            let rt = Runtime::new(1);
            let handle = rt.handle();
            let counter = Arc::new(AtomicUsize::new(0));

            let c1 = counter.clone();
            let c2 = counter.clone();

            let value = rt.block_on(async move {
                let a = handle.spawn(async move {
                    yield_now().await;
                    c1.fetch_add(10, Ordering::SeqCst);
                });
                let b = handle.spawn(async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    yield_now().await;
                    c2.fetch_add(100, Ordering::SeqCst);
                });

                a.await;
                b.await;
                counter.load(Ordering::SeqCst)
            });

            assert_eq!(value, 111);
            rt.shutdown();
        });
    }

    /// Many spawned tasks all complete and the collected sum is correct.
    #[test]
    fn many_spawned_tasks_sum_correctly() {
        model(|| {
            const N: usize = 100;
            let rt = Runtime::new(2);
            let handle = rt.handle();

            let sum = rt.block_on(async move {
                let mut handles = Vec::with_capacity(N);
                for i in 0..N {
                    handles.push(handle.spawn(async move { i }));
                }

                let mut total = 0usize;
                for h in handles {
                    total += h.await;
                }
                total
            });

            let expected: usize = (0..N).sum();
            assert_eq!(sum, expected);
            assert!(rt.shutdown());
        });
    }

    /// A runtime with no worker threads still works: `block_on` drives the
    /// root future itself.
    #[test]
    fn zero_worker_threads_runs_on_caller() {
        model(|| {
            let rt = Runtime::new(0);
            let value = rt.block_on(async { 99 });
            assert_eq!(value, 99);
            rt.shutdown();
        });
    }

    /// `Handle::spawn` works from a worker thread and the result is awaited
    /// inside `block_on`.
    #[test]
    fn nested_spawn() {
        model(|| {
            let rt = Runtime::new(1);
            let handle = rt.handle();
            let handle2 = handle.clone();

            let value = rt.block_on(async move {
                handle
                    .spawn(async move { handle2.spawn(async { 21 + 21 }).await })
                    .await
            });

            assert_eq!(value, 42);
            rt.shutdown();
        });
    }

    /// Dropping a runtime cleanly terminates its worker threads without an
    /// explicit call to [`Runtime::shutdown`].
    #[test]
    fn drop_without_shutdown() {
        model(|| {
            let worker = crate::thread::spawn(|| {
                let rt = Runtime::new(1);
                let handle = rt.handle().spawn(async { 123 });
                assert_eq!(rt.block_on(handle), 123);
                // `rt` is dropped here; if workers did not exit, this thread would
                // hang in `Drop`.
            });
            worker.join().unwrap();
        });
    }

    /// Mirrors the module-level doctest so that hangs are caught as unit tests.
    #[test]
    fn doctest_equivalent() {
        model(|| {
            let rt = Runtime::new(2);
            let handle = rt.handle();

            let value = rt.block_on(async move { handle.spawn(async { 42 }).await });

            assert_eq!(value, 42);
        });
    }
}
