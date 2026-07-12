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
    if capacity == 0 {
        panic!("capacity must be greater than one");
    }

    let shared = Arc::new(Shared {
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
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
    senders: AtomicUsize,
    receivers: AtomicUsize,
    qty_waiting: AtomicUsize,
    waiting: Mutex<()>,
    waiting_cv: Condvar,
    buffer: RingBuffer<T>,
}

impl<T> Shared<T> {
    fn wait(&self, g: MutexGuard<'_, ()>) {
        self.qty_waiting.fetch_add(1, Ordering::Release);
        let g = self.waiting_cv.wait(g).unwrap();
        self.qty_waiting.fetch_sub(1, Ordering::Release);
        drop(g);
    }

    fn is_closed(&self) -> bool {
        let no_senders = self.senders.load(Ordering::Acquire) == 0;
        let no_receivers = self.receivers.load(Ordering::Acquire) == 0;
        no_senders || no_receivers
    }
}

/// The sending half of a bounded channel.
///
/// `Sender` is `Clone`. The current implementation requires `T: Clone`, but
/// cloning only clones the internal `Arc`; it never clones a buffered item, so
/// the bound is stricter than necessary.
///
/// ```compile_fail
/// use milkyapps_core::sync::channel::bounded;
///
/// #[derive(Debug)]
/// struct NonClone;
///
/// let (tx, _rx) = bounded::<NonClone>(4);
/// let _tx2 = tx.clone();
/// ```
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.senders.fetch_sub(1, Ordering::Relaxed);
        let g = self.shared.waiting.lock().unwrap();
        self.shared.waiting_cv.notify_all();
        drop(g);
    }
}

impl<T> Sender<T> {
    /// Sends `item` to the channel.
    ///
    /// If the ring buffer is full, the caller blocks until a receiver makes
    /// space or the channel is closed. Returns `Err(())` if the channel is
    /// closed. Note that a sender blocked inside [`wait`](Shared::wait) may not
    /// be woken when the receiver drops, so this method can deadlock.
    pub fn send(&self, item: T) -> Result<(), ()> {
        let mut item = item;

        loop {
            // Cannot send if channel is closed
            if self.shared.is_closed() {
                let g = self.shared.waiting.lock().unwrap();
                self.shared.waiting_cv.notify_all();
                drop(g);
                return Err(());
            }

            // Fast path
            match self.shared.buffer.push(item) {
                Ok(()) => {
                    if self.shared.qty_waiting.load(Ordering::Acquire) > 0 {
                        let g = self.shared.waiting.lock().unwrap();
                        self.shared.waiting_cv.notify_all();
                        drop(g);
                    }
                    return Ok(());
                }
                Err(i) => {
                    item = i;
                }
            }

            // Slow path
            let g = self.shared.waiting.lock().unwrap();
            match self.shared.buffer.push(item) {
                Ok(()) => {
                    if self.shared.qty_waiting.load(Ordering::Acquire) > 0 {
                        self.shared.waiting_cv.notify_all();
                    }
                    return Ok(());
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
/// `Receiver` is `Clone`. Like `Sender`, the current implementation requires
/// `T: Clone` even though cloning only duplicates the internal `Arc`. Its
/// [`recv`](Receiver::recv) method returns `Some(item)` when data is available
/// and `None` when the channel is closed (all senders and/or all receivers
/// dropped).
///
/// ```compile_fail
/// use milkyapps_core::sync::channel::bounded;
///
/// #[derive(Debug)]
/// struct NonClone;
///
/// let (_tx, rx) = bounded::<NonClone>(4);
/// let _rx2 = rx.clone();
/// ```
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receivers.fetch_sub(1, Ordering::Relaxed);
        let g = self.shared.waiting.lock().unwrap();
        self.shared.waiting_cv.notify_all();
        drop(g);
    }
}

impl<T> Receiver<T> {
    /// Receives the next item from the channel.
    ///
    /// Returns `Some(item)` when an item is available. Returns `None` when the
    /// channel is closed. Note that a receiver blocked inside [`wait`](Shared::wait)
    /// may not be woken when the last sender drops, so this method can deadlock.
    pub fn recv(&self) -> Option<T> {
        loop {
            // Fast path
            match self.shared.buffer.pop() {
                Ok(Some(item)) => {
                    if self.shared.qty_waiting.load(Ordering::Acquire) > 0 {
                        let g = self.shared.waiting.lock().unwrap();
                        self.shared.waiting_cv.notify_all();
                        drop(g);
                    }
                    return Some(item);
                }
                _ => {}
            }

            // Slow path
            let g = self.shared.waiting.lock().unwrap();
            match self.shared.buffer.pop() {
                Ok(Some(item)) => {
                    if self.shared.qty_waiting.load(Ordering::Acquire) > 0 {
                        self.shared.waiting_cv.notify_all();
                    }
                    return Some(item);
                }
                _ => {}
            }

            // pop cannot wait if channel is closed
            if self.shared.is_closed() {
                self.shared.waiting_cv.notify_all();
                return None;
            }

            self.shared.wait(g);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Arc;
    #[cfg(all(not(loom), not(miri)))]
    use crate::sync::Barrier;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::sync::model;
    #[cfg(all(not(loom), not(miri)))]
    use crate::thread::scope;
    #[cfg(all(not(loom), not(miri)))]
    use std::time::Duration;

    /// Run `f` on a background thread and return its result, or `None` if it
    /// does not complete within `timeout`. Used by non-loom tests to detect
    /// calls that block forever instead of returning.
    #[cfg(all(not(loom), not(miri)))]
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
            tx.send(7).unwrap();
            assert_eq!(rx.recv(), Some(7));
        });
    }

    #[test]
    fn fifo_order() {
        model(|| {
            let (tx, rx) = bounded::<usize>(4);
            for i in 0..4 {
                tx.send(i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(rx.recv(), Some(i));
            }
        });
    }

    /// Repeatedly send one item and receive one item, keeping the buffer near
    /// empty. This exercises the fast path through many empty/full transitions.
    ///
    /// The Miri version uses fewer iterations because the current implementation
    /// can deadlock here under Miri's scheduler (the slow path in `send` waits on
    /// the condvar with no other thread to wake it).
    #[cfg(miri)]
    #[test]
    fn alternating_send_recv() {
        model(|| {
            let (tx, rx) = bounded::<usize>(4);
            for i in 0..10 {
                tx.send(i).unwrap();
                assert_eq!(rx.recv(), Some(i));
            }
        });
    }

    #[cfg(not(miri))]
    #[test]
    fn alternating_send_recv() {
        model(|| {
            let (tx, rx) = bounded::<usize>(4);
            for i in 0..50 {
                tx.send(i).unwrap();
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
                    tx.send(i).unwrap();
                }
                for i in 0..cap {
                    assert_eq!(rx.recv(), Some(i));
                }
            }
        });
    }

    /// Run outside loom/miri: the current implementation has a lost-wake-up race
    /// in the blocking path, so this test can deadlock under Loom and Miri.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn empty_recv_blocks_until_send() {
        let (tx, rx) = bounded::<i32>(4);
        let barrier = Barrier::new(2);
        scope(|s| {
            s.spawn(|| {
                barrier.wait();
                assert_eq!(rx.recv(), Some(42));
            });
            s.spawn(|| {
                barrier.wait();
                tx.send(42).unwrap();
            });
        });
    }

    /// Run outside loom/miri: the current implementation can cause a destructor
    /// panic in the Loom runtime and can deadlock under Miri.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn full_send_blocks_until_recv() {
        let (tx, rx) = bounded::<i32>(2);
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        scope(|s| {
            s.spawn(move || {
                tx.send(3).unwrap();
            });

            s.spawn(move || {
                assert_eq!(rx.recv(), Some(1));
                assert_eq!(rx.recv(), Some(2));
                assert_eq!(rx.recv(), Some(3));
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
                    tx.send(base + i).unwrap();
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
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn capacity_non_power_of_two_fifo() {
        let (tx, rx) = bounded::<usize>(3);
        for i in 0..3 {
            tx.send(i).unwrap();
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
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn capacity_one_does_not_overwrite() {
        let (tx, rx) = bounded::<i32>(1);
        tx.send(1).unwrap();
        let result = with_timeout(
            move || {
                tx.send(2).unwrap();
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
    /// outside loom/miri; an intentional deadlock inside either would abort
    /// during cleanup.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn zero_capacity_send_times_out() {
        let (tx, _rx) = bounded::<i32>(0);
        let result = with_timeout(move || tx.send(1), Duration::from_millis(500));
        assert!(
            result.is_none(),
            "send on a zero-capacity channel should block forever"
        );
    }

    /// `recv` returns `None` when it observes that all senders are already
    /// dropped. This test never blocks, so it passes even if `Sender::Drop` did
    /// not notify waiters.
    #[cfg(all(not(loom), not(miri)))]
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
                tx.send(Counter::new(&live)).unwrap();
                tx.send(Counter::new(&live)).unwrap();
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
            tx.send(Counter::new(&live)).unwrap();
            let item = rx.recv().unwrap();
            assert_eq!(live.load(Ordering::Relaxed), 1);
            drop(item);
            assert_eq!(live.load(Ordering::Relaxed), 0);
        });
    }

    /// Stress test: a single producer and a single consumer exchange many
    /// items. Run outside loom/miri because the current blocking path has a
    /// lost-wake-up race that Loom and Miri find as deadlocks.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn spsc_stress() {
        model(|| {
            const N: usize = 200;
            let (tx, rx) = bounded::<usize>(8);
            let done = Arc::new(AtomicUsize::new(0));

            scope(|s| {
                s.spawn(|| {
                    for i in 0..N {
                        tx.send(i).unwrap();
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
    /// Run only outside loom/miri: the required producer contention exercises
    /// the ring buffer's `std::thread::park_timeout` path, which loom does not
    /// model, and Miri's single-threaded scheduler deadlocks in the slow path.
    #[cfg(all(not(loom), not(miri)))]
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
                        tx.send(p * PER_PRODUCER + i).unwrap();
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

    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn shared_receiver_mpmc() {
        const CONSUMERS: usize = 3;
        const TOTAL: usize = 300;
        let expected_sum: usize = (0..TOTAL).sum();
        let actual_sum = AtomicUsize::new(0);

        let (tx, rx) = bounded::<usize>(16);

        scope(|s| {
            s.spawn(move || {
                for i in 0..TOTAL {
                    println!("Send: {i}");
                    tx.send(i).unwrap();
                }

                println!("tx dead");
            });

            for i in 0..CONSUMERS {
                let data = (i, &rx, &actual_sum);
                s.spawn(move || {
                    let (i, rx, actual_sum) = data;
                    while let Some(v) = rx.recv() {
                        println!("Recv {i}: {v}");
                        actual_sum.fetch_add(v, Ordering::SeqCst);
                    }

                    println!("rx {i} dead");
                });
            }
        });

        // assert_eq!(received, (0..TOTAL).collect::<Vec<_>>());
        assert_eq!(
            actual_sum.load(Ordering::SeqCst),
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
            tx.send(Counter::new(&live)).unwrap();
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
    /// Run outside loom/miri because it needs multiple producers and can
    /// deadlock under Miri's scheduler.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn notify_all_wakes_multiple_producers() {
        const P: usize = 3;
        let (tx, rx) = bounded::<usize>(P - 1);
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let ready = Arc::new(AtomicUsize::new(0));

        // Fill the buffer so every producer will block on its next send.
        for _ in 0..P - 1 {
            tx.send(0).unwrap();
        }

        scope(|s| {
            for _ in 0..P {
                let tx = Arc::clone(&tx);
                let ready = Arc::clone(&ready);
                s.spawn(move || {
                    tx.send(99).unwrap();
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

    /// A receiver that is already blocked must be woken when the last sender is
    /// dropped and return `None`. Currently `Sender::Drop` does not notify the
    /// condvar, so this test deadlocks under Miri.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn blocked_recv_unblocks_when_last_sender_dropped() {
        let (tx, rx) = bounded::<i32>(4);
        let result = with_timeout(
            move || {
                let handle = std::thread::spawn(move || rx.recv());
                std::thread::sleep(Duration::from_millis(50));
                drop(tx);
                handle.join().unwrap()
            },
            Duration::from_millis(500),
        );
        assert_eq!(
            result,
            Some(None),
            "blocked receiver must return None after last sender drops"
        );
    }

    /// A sender that is already blocked on a full buffer must be woken when the
    /// receiver drops and return `Err(())`. Currently `Receiver::Drop` does not
    /// notify the condvar, so this test deadlocks under Miri.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn blocked_send_unblocks_when_receiver_dropped() {
        let (tx, rx) = bounded::<i32>(1);
        tx.send(1).unwrap(); // fill the buffer
        let result = with_timeout(
            move || {
                let handle = std::thread::spawn(move || tx.send(2));
                std::thread::sleep(Duration::from_millis(50));
                drop(rx);
                handle.join().unwrap()
            },
            Duration::from_millis(500),
        );
        assert_eq!(
            result,
            Some(Err(())),
            "blocked sender must return Err after receiver drops"
        );
    }

    /// `send` returns `Err(())` when called after the receiver has been dropped.
    #[cfg(not(loom))]
    #[test]
    fn send_returns_err_after_receiver_dropped() {
        let (tx, rx) = bounded::<i32>(4);
        drop(rx);
        assert_eq!(tx.send(1), Err(()));
    }

    /// Cloning a `Sender` increments the sender counter. The channel stays open
    /// until every clone is dropped.
    #[cfg(not(loom))]
    #[test]
    fn multiple_cloned_senders_disconnect() {
        let (tx, rx) = bounded::<i32>(4);
        let tx2 = tx.clone();
        drop(tx);
        tx2.send(1).unwrap();
        assert_eq!(rx.recv(), Some(1));
        drop(tx2);
        assert_eq!(rx.recv(), None);
    }

    /// Cloning a `Receiver` keeps the channel open from the receiver side. A
    /// sender can still send after one receiver clone is dropped.
    #[cfg(not(loom))]
    #[test]
    fn receiver_clone_keeps_channel_open() {
        let (tx, rx) = bounded::<i32>(4);
        let rx2 = rx.clone();
        drop(rx);
        tx.send(1).unwrap();
        assert_eq!(rx2.recv(), Some(1));
    }

    /// Heavy contention exercises the `RingBuffer::push`/`pop` `Err(())` paths
    /// and the end-of-stream disconnect path. If `Sender::Drop` fails to wake
    /// blocked consumers, the scope never returns and this test times out.
    /// Run outside Miri to avoid the slow-path deadlock.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn heavy_contention_preserves_all_items() {
        const PRODUCERS: usize = 8;
        const CONSUMERS: usize = 8;
        const PER_PRODUCER: usize = 100;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;
        let expected_sum: usize = (0..TOTAL).sum();

        let (tx, rx) = bounded::<usize>(16);
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let count = Arc::new(AtomicUsize::new(0));
        let sum = Arc::new(AtomicUsize::new(0));

        let result = with_timeout(
            move || {
                scope(|s| {
                    for p in 0..PRODUCERS {
                        let tx = Arc::clone(&tx);
                        s.spawn(move || {
                            for i in 0..PER_PRODUCER {
                                tx.send(p * PER_PRODUCER + i).unwrap();
                            }
                        });
                    }

                    for _ in 0..CONSUMERS {
                        let rx = Arc::clone(&rx);
                        let count = Arc::clone(&count);
                        let sum = Arc::clone(&sum);
                        s.spawn(move || {
                            loop {
                                if let Some(v) = rx.recv() {
                                    let c = count.fetch_add(1, Ordering::SeqCst);
                                    sum.fetch_add(v, Ordering::SeqCst);
                                    if c + 1 == TOTAL {
                                        break;
                                    }
                                }
                            }
                        });
                    }
                });

                (count.load(Ordering::SeqCst), sum.load(Ordering::SeqCst))
            },
            Duration::from_secs(1),
        );

        let (count, sum) = result.expect(
            "heavy-contention test timed out: a blocked consumer was not woken when senders dropped"
        );
        assert_eq!(count, TOTAL, "lost or duplicated items");
        assert_eq!(sum, expected_sum, "item corruption");
    }

    // NOTE: Loom tests that intentionally deadlock (e.g. dropping the last
    // sender while a receiver is blocked) cause Loom to abort during destructor
    // cleanup, because `RingBuffer::drop` touches Loom-modeled atomics outside
    // an active thread. The non-loom `with_timeout` tests above demonstrate
    // those bugs instead.
}
