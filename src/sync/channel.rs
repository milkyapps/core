//! A blocking bounded channel built on top of the lock-free [`RingBuffer`].
//!
//! The channel exposes a fast path that delegates to the ring buffer directly,
//! and a slow path that uses a [`Mutex`] + [`Condvar`] to block the caller when
//! the buffer is full (senders) or empty (receivers).

#[cfg(loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

use crate::sync::Arc;
use crate::sync::AtomicUsize;
use crate::sync::atomic::Ordering;

use crate::collections::ringbuffer::RingBuffer;

/// Creates a bounded channel with room for at least `capacity` items.
///
/// # Examples
///
/// ```
/// use milkyapps_core::sync::channel::bounded;
///
/// let (tx, rx) = bounded::<i32>(4);
/// tx.send(1);
/// assert_eq!(rx.recv(), Some(1));
/// ```
///
/// `Sender` is currently not `Clone`, so sharing a sender requires wrapping it
/// in an `Arc`:
///
/// ```compile_fail
/// use milkyapps_core::sync::channel::bounded;
///
/// let (tx, _rx) = bounded::<i32>(4);
/// let _tx2 = tx.clone();
/// ```
///
/// The public constructor does not require `T: Send`, but moving a non-`Send`
/// sender across threads is rejected at the call site:
///
/// ```compile_fail
/// use milkyapps_core::sync::channel::bounded;
/// use std::rc::Rc;
///
/// let (tx, _rx) = bounded::<Rc<i32>>(4);
/// std::thread::spawn(move || {
///     tx.send(Rc::new(1));
/// });
/// ```
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        qty_waiting: AtomicUsize::new(0),
        waiting: Mutex::new(()),
        waiting_cv: Condvar::new(),
        buffer: RingBuffer::with_capacity(capacity),
    });

    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

struct Shared<T> {
    qty_waiting: AtomicUsize,
    waiting: Mutex<()>,
    waiting_cv: Condvar,
    buffer: RingBuffer<T>,
}

impl<T> Shared<T> {
    fn notify_if_needed(&self, g: MutexGuard<'_, ()>) {
        if self.qty_waiting.load(Ordering::Acquire) > 0 {
            self.waiting_cv.notify_all();
            drop(g);
        }
    }

    fn wait(&self, g: MutexGuard<'_, ()>) {
        self.qty_waiting.fetch_add(1, Ordering::Release);
        let g = self.waiting_cv.wait(g).unwrap();
        drop(g);
        self.qty_waiting.fetch_sub(1, Ordering::Release);
    }
}

/// The sending half of a bounded channel.
///
/// Currently `Sender` is neither `Clone` nor `Copy`; there can be at most one
/// owned sender per call to [`bounded`]. Wrapping it in [`Arc`] allows multiple
/// threads to share the same send endpoint.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Sender<T> {
    /// Sends `item` to the channel.
    ///
    /// If the ring buffer is full, the caller blocks until a receiver makes
    /// space.
    pub fn send(&self, item: T) {
        let mut item = item;

        loop {
            // Fast path
            match self.shared.buffer.push(item) {
                Ok(()) => {
                    self.shared
                        .notify_if_needed(self.shared.waiting.lock().unwrap());
                    return;
                }
                Err(i) => {
                    item = i;
                }
            }

            // Slow path
            let g = self.shared.waiting.lock().unwrap();
            match self.shared.buffer.push(item) {
                Ok(()) => {
                    self.shared.notify_if_needed(g);
                    return;
                }
                Err(i) => {
                    item = i;
                }
            }

            self.shared.wait(g);
        }
    }
}

/// The receiving half of a bounded channel.
///
/// `Receiver` is not `Clone`. Its [`recv`](Receiver::recv) method returns
/// `Some(item)` when data is available and blocks otherwise. It currently
/// **never returns `None`**, because the channel has no disconnect/close logic.
///
/// ```compile_fail
/// use milkyapps_core::sync::channel::bounded;
///
/// let (_tx, rx) = bounded::<i32>(4);
/// let _rx2 = rx.clone();
/// ```
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Receiver<T> {
    /// Receives the next item from the channel.
    ///
    /// Returns `Some(item)` when an item is available. Blocks until a sender
    /// provides one. At the moment this method never returns `None`, because
    /// dropping all [`Sender`]s is not detected.
    pub fn recv(&self) -> Option<T> {
        loop {
            // Fast path
            if let Some(item) = self.shared.buffer.pop() {
                self.shared
                    .notify_if_needed(self.shared.waiting.lock().unwrap());
                return Some(item);
            }

            // Slow path
            let g = self.shared.waiting.lock().unwrap();
            if let Some(item) = self.shared.buffer.pop() {
                self.shared.notify_if_needed(g);
                return Some(item);
            }

            self.shared.wait(g);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::sync::model;
    use crate::sync::{Arc, Barrier};
    use crate::thread::scope;
    #[cfg(not(loom))]
    use std::time::Duration;

    /// Run `f` on a background thread and return its result, or `None` if it
    /// does not complete within `timeout`. Used by non-loom tests to detect
    /// calls that block forever instead of returning.
    #[cfg(not(loom))]
    fn with_timeout<T: Send + 'static>(
        f: impl FnOnce() -> T + Send + 'static,
        timeout: Duration,
    ) -> Option<T> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(timeout).ok()
    }

    #[test]
    fn send_then_recv_single() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            tx.send(7);
            assert_eq!(rx.recv(), Some(7));
        });
    }

    #[test]
    fn fifo_order() {
        model(|| {
            let (tx, rx) = bounded::<usize>(4);
            for i in 0..4 {
                tx.send(i);
            }
            for i in 0..4 {
                assert_eq!(rx.recv(), Some(i));
            }
        });
    }

    /// Repeatedly send one item and receive one item, keeping the buffer near
    /// empty. This exercises the fast path through many empty/full transitions.
    #[test]
    fn alternating_send_recv() {
        model(|| {
            let (tx, rx) = bounded::<usize>(4);
            for i in 0..50 {
                tx.send(i);
                assert_eq!(rx.recv(), Some(i));
            }
        });
    }

    /// Filling a power-of-two buffer exactly to capacity and then draining it
    /// must preserve FIFO order and leave the buffer empty.
    #[test]
    fn fill_to_capacity_then_drain() {
        model(|| {
            for cap in [2usize, 4, 8, 16] {
                let (tx, rx) = bounded::<usize>(cap);
                for i in 0..cap {
                    tx.send(i);
                }
                for i in 0..cap {
                    assert_eq!(rx.recv(), Some(i));
                }
            }
        });
    }

    #[test]
    fn empty_recv_blocks_until_send() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            let barrier = Barrier::new(2);
            scope(|s| {
                s.spawn(|| {
                    barrier.wait();
                    assert_eq!(rx.recv(), Some(42));
                });
                s.spawn(|| {
                    barrier.wait();
                    tx.send(42);
                });
            });
        });
    }

    #[test]
    fn full_send_blocks_until_recv() {
        model(|| {
            let (tx, rx) = bounded::<i32>(2);
            tx.send(1);
            tx.send(2);

            let tx = Arc::new(tx);
            let rx = Arc::new(rx);

            scope(|s| {
                let tx = Arc::clone(&tx);
                s.spawn(move || {
                    tx.send(3);
                });

                let rx = Arc::clone(&rx);
                s.spawn(move || {
                    assert_eq!(rx.recv(), Some(1));
                    assert_eq!(rx.recv(), Some(2));
                    assert_eq!(rx.recv(), Some(3));
                });
            });
        });
    }

    #[test]
    fn wrap_around_preserves_order() {
        model(|| {
            let (tx, rx) = bounded::<usize>(4);
            for round in 0..10 {
                let base = round * 4;
                for i in 0..4 {
                    tx.send(base + i);
                }
                for i in 0..4 {
                    assert_eq!(rx.recv(), Some(base + i));
                }
            }
        });
    }

    /// Non-power-of-two capacities are inherited from the underlying
    /// `RingBuffer`, which currently computes its mask from the requested
    /// capacity instead of the rounded-up slot count. With capacity 3 the
    /// receiver hangs forever in `pop`, so this test runs under a timeout.
    #[cfg(not(loom))]
    #[test]
    fn capacity_non_power_of_two_fifo() {
        let (tx, rx) = bounded::<usize>(3);
        for i in 0..3 {
            tx.send(i);
        }
        let result = with_timeout(
            move || {
                let mut out = Vec::new();
                for _ in 0..3 {
                    out.push(rx.recv().unwrap());
                }
                out
            },
            Duration::from_millis(500),
        );
        assert_eq!(
            result,
            Some(vec![0, 1, 2]),
            "non-power-of-two capacity is broken: recv hangs or returns wrong values"
        );
    }

    /// A capacity of one is broken in the underlying ring buffer. The wrapper's
    /// `send` has no error path, so the second send blocks forever; even if it
    /// returned, the single slot would be overwritten.
    #[cfg(not(loom))]
    #[test]
    fn capacity_one_does_not_overwrite() {
        let (tx, rx) = bounded::<i32>(1);
        tx.send(1);
        let result = with_timeout(
            move || {
                tx.send(2);
                rx.recv()
            },
            Duration::from_millis(500),
        );
        assert_eq!(
            result,
            Some(Some(1)),
            "capacity-1 channel should reject or not overwrite the first item"
        );
    }

    /// A capacity of zero rounds up to two slots in the ring buffer, but the
    /// wrapper's `RingBuffer::with_capacity(0)` records `cap == 0`, so `push`
    /// reports full immediately and the sender blocks forever. Detect this
    /// outside loom; an intentional deadlock inside loom would abort during
    /// cleanup because the ring-buffer destructor touches loom-modeled
    /// atomics outside an active thread.
    #[cfg(not(loom))]
    #[test]
    fn zero_capacity_send_times_out() {
        let (tx, _rx) = bounded::<i32>(0);
        let result = with_timeout(move || tx.send(1), Duration::from_millis(500));
        assert!(
            result.is_none(),
            "send on a zero-capacity channel should block forever"
        );
    }

    /// The `Option<T>` return type suggests disconnect support, but there is no
    /// sender-counting logic, so `recv` blocks forever after the sender is
    /// dropped. Detect this outside loom; an intentional deadlock inside loom
    /// would abort during cleanup because the ring-buffer destructor touches
    /// loom-modeled atomics outside an active thread.
    #[cfg(not(loom))]
    #[test]
    fn recv_returns_none_when_all_senders_dropped() {
        let (tx, rx) = bounded::<i32>(4);
        drop(tx);
        let result = with_timeout(move || rx.recv(), Duration::from_millis(500));
        assert!(
            result.is_some(),
            "recv blocked forever instead of returning None after all senders dropped"
        );
        assert_eq!(result.unwrap(), None);
    }

    /// Dropping the whole channel (all senders and all receivers) must run the
    /// destructors of any items still resident in the buffer.
    #[test]
    fn drop_drains_resident_items() {
        #[derive(Debug)]
        struct Counter {
            live: Arc<AtomicUsize>,
        }
        impl Counter {
            fn new(live: &Arc<AtomicUsize>) -> Self {
                live.fetch_add(1, Ordering::Relaxed);
                Self {
                    live: Arc::clone(live),
                }
            }
        }
        impl Drop for Counter {
            fn drop(&mut self) {
                self.live.fetch_sub(1, Ordering::Relaxed);
            }
        }

        model(|| {
            let live = Arc::new(AtomicUsize::new(0));
            {
                let (tx, _rx) = bounded::<Counter>(4);
                tx.send(Counter::new(&live));
                tx.send(Counter::new(&live));
                assert_eq!(live.load(Ordering::Relaxed), 2);
            }
            assert_eq!(live.load(Ordering::Relaxed), 0, "buffered items leaked");
        });
    }

    /// A popped item must be dropped exactly once, and the channel must not
    /// retain a second copy.
    #[test]
    fn popped_item_dropped_once() {
        #[derive(Debug)]
        struct Counter {
            live: Arc<AtomicUsize>,
        }
        impl Counter {
            fn new(live: &Arc<AtomicUsize>) -> Self {
                live.fetch_add(1, Ordering::Relaxed);
                Self {
                    live: Arc::clone(live),
                }
            }
        }
        impl Drop for Counter {
            fn drop(&mut self) {
                self.live.fetch_sub(1, Ordering::Relaxed);
            }
        }

        model(|| {
            let live = Arc::new(AtomicUsize::new(0));
            let (tx, rx) = bounded::<Counter>(4);
            tx.send(Counter::new(&live));
            let item = rx.recv().unwrap();
            assert_eq!(live.load(Ordering::Relaxed), 1);
            drop(item);
            assert_eq!(live.load(Ordering::Relaxed), 0);
        });
    }

    /// Stress test: a single producer and a single consumer exchange many
    /// items. This is the only shape that is safe to exercise under loom with
    /// the current ring-buffer implementation, because the underlying
    /// `push`/`pop` call `std::thread::park_timeout` on contention.
    #[test]
    fn spsc_stress() {
        model(|| {
            const N: usize = 200;
            let (tx, rx) = bounded::<usize>(8);
            let done = Arc::new(AtomicUsize::new(0));

            scope(|s| {
                s.spawn(|| {
                    for i in 0..N {
                        tx.send(i);
                    }
                });

                let done = Arc::clone(&done);
                s.spawn(move || {
                    let mut expected = 0usize;
                    let mut received = 0usize;
                    while received < N {
                        if let Some(v) = rx.recv() {
                            assert_eq!(v, expected, "FIFO order violated");
                            expected += 1;
                            received += 1;
                        }
                    }
                    done.store(1, Ordering::SeqCst);
                });
            });

            assert_eq!(done.load(Ordering::SeqCst), 1);
        });
    }

    /// Multiple producers share a single `Sender` through an `Arc`. This is the
    /// only way to get MPMC behaviour today because `Sender` is not `Clone`.
    /// Run only outside loom: the required producer contention exercises the
    /// ring buffer's `std::thread::park_timeout` path, which loom does not
    /// model and which would block the single OS thread.
    #[cfg(not(loom))]
    #[test]
    fn shared_sender_mpmc() {
        const PRODUCERS: usize = 3;
        const PER_PRODUCER: usize = 100;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;
        let expected_sum: usize = (0..TOTAL).sum();

        let (tx, rx) = bounded::<usize>(16);
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let sum = Arc::new(AtomicUsize::new(0));
        let count = Arc::new(AtomicUsize::new(0));

        scope(|s| {
            for p in 0..PRODUCERS {
                let tx = Arc::clone(&tx);
                s.spawn(move || {
                    for i in 0..PER_PRODUCER {
                        tx.send(p * PER_PRODUCER + i);
                    }
                });
            }

            let rx = Arc::clone(&rx);
            let sum = Arc::clone(&sum);
            let count = Arc::clone(&count);
            s.spawn(move || {
                let mut local_sum = 0usize;
                let mut local_count = 0usize;
                while local_count < TOTAL {
                    if let Some(v) = rx.recv() {
                        local_sum += v;
                        local_count += 1;
                    }
                }
                sum.store(local_sum, Ordering::SeqCst);
                count.store(local_count, Ordering::SeqCst);
            });
        });

        assert_eq!(
            count.load(Ordering::SeqCst),
            TOTAL,
            "lost or duplicated items"
        );
        assert_eq!(sum.load(Ordering::SeqCst), expected_sum, "item corruption");
    }

    /// Multiple consumers share a single `Receiver` through an `Arc`. One
    /// producer sends all items. Consumers are serialized by an auxiliary mutex
    /// so the test can terminate cleanly despite the channel's lack of
    /// disconnect/close support. Run outside loom because the serialization
    /// mutex is not loom-modeled.
    #[cfg(not(loom))]
    #[test]
    fn shared_receiver_mpmc() {
        const CONSUMERS: usize = 3;
        const TOTAL: usize = 300;
        let expected_sum: usize = (0..TOTAL).sum();

        let (tx, rx) = bounded::<usize>(16);
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let count = Arc::new(AtomicUsize::new(0));
        let guard = Arc::new(std::sync::Mutex::new(()));
        let (collector_tx, collector_rx) = std::sync::mpsc::channel();

        scope(|s| {
            s.spawn(move || {
                for i in 0..TOTAL {
                    tx.send(i);
                }
            });

            for _ in 0..CONSUMERS {
                let rx = Arc::clone(&rx);
                let count = Arc::clone(&count);
                let guard = Arc::clone(&guard);
                let collector_tx = collector_tx.clone();
                s.spawn(move || {
                    let mut local = Vec::new();
                    loop {
                        let g = guard.lock().unwrap();
                        let current = count.load(Ordering::SeqCst);
                        if current >= TOTAL {
                            drop(g);
                            break;
                        }
                        if let Some(v) = rx.recv() {
                            count.fetch_add(1, Ordering::SeqCst);
                            local.push(v);
                        }
                        drop(g);
                    }
                    for v in local {
                        let _ = collector_tx.send(v);
                    }
                });
            }
        });

        let mut received = Vec::with_capacity(TOTAL);
        for _ in 0..TOTAL {
            received.push(collector_rx.recv().unwrap());
        }
        received.sort_unstable();
        assert_eq!(received, (0..TOTAL).collect::<Vec<_>>());
        assert_eq!(
            received.iter().sum::<usize>(),
            expected_sum,
            "item corruption"
        );
    }

    /// `Sender<T>` and `Receiver<T>` should be `Send` and `Sync` whenever
    /// `T: Send`, because the channel moves values across threads.
    #[test]
    fn sender_and_receiver_are_send_and_sync_when_t_is_send() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Sender<i32>>();
        assert_send::<Receiver<i32>>();
        assert_sync::<Sender<i32>>();
        assert_sync::<Receiver<i32>>();
    }

    /// Items sent after the receiver has been dropped are buffered. They leak
    /// until the last sender is dropped, because there is no receiver to pop
    /// them.
    #[test]
    fn send_after_receiver_dropped() {
        #[derive(Debug)]
        struct Counter {
            live: Arc<AtomicUsize>,
        }
        impl Counter {
            fn new(live: &Arc<AtomicUsize>) -> Self {
                live.fetch_add(1, Ordering::Relaxed);
                Self {
                    live: Arc::clone(live),
                }
            }
        }
        impl Drop for Counter {
            fn drop(&mut self) {
                self.live.fetch_sub(1, Ordering::Relaxed);
            }
        }

        model(|| {
            let live = Arc::new(AtomicUsize::new(0));
            let (tx, rx) = bounded::<Counter>(4);
            tx.send(Counter::new(&live));
            drop(rx);
            assert_eq!(
                live.load(Ordering::Relaxed),
                1,
                "item must still be alive after receiver dropped"
            );
            drop(tx);
            assert_eq!(
                live.load(Ordering::Relaxed),
                0,
                "item must be dropped when channel is dropped"
            );
        });
    }

    /// When several producers are blocked on a full buffer, one consumer pop
    /// wakes all of them. Only one can succeed; the rest go back to sleep. The
    /// consumer must keep popping until every producer has finished its send.
    /// Run outside loom because it needs multiple producers.
    #[cfg(not(loom))]
    #[test]
    fn notify_all_wakes_multiple_producers() {
        const P: usize = 3;
        let (tx, rx) = bounded::<usize>(P - 1);
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let ready = Arc::new(AtomicUsize::new(0));

        // Fill the buffer so every producer will block on its next send.
        for _ in 0..P - 1 {
            tx.send(0);
        }

        scope(|s| {
            for _ in 0..P {
                let tx = Arc::clone(&tx);
                let ready = Arc::clone(&ready);
                s.spawn(move || {
                    tx.send(99);
                    ready.fetch_add(1, Ordering::SeqCst);
                });
            }

            let rx = Arc::clone(&rx);
            let ready = Arc::clone(&ready);
            s.spawn(move || {
                while ready.load(Ordering::SeqCst) < P {
                    let _ = rx.recv();
                }
            });
        });

        assert_eq!(ready.load(Ordering::SeqCst), P);
    }
}
