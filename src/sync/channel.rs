//! A blocking bounded channel with a Sender and a Receiver side.

#[cfg(loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

use crate::collections::ringbuffer::PushError;
use crate::collections::ringbuffer::RingBuffer;
use crate::sync::Arc;
use crate::sync::AtomicUsize;
use crate::sync::atomic::Ordering;

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
/// # Panics
///
/// Panics if `capacity` is `0`.
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");

    let shared = Arc::new(Shared {
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
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
    waiting: Mutex<()>,
    waiting_cv: Condvar,
    buffer: RingBuffer<T>,
}

impl<T> Shared<T> {
    fn wait(&self, g: MutexGuard<'_, ()>) {
        if self.is_closed() {
            return;
        }
        let g = self.waiting_cv.wait(g).unwrap();
        drop(g);
    }

    fn is_closed(&self) -> bool {
        let no_senders = self.senders.load(Ordering::SeqCst) == 0;
        let no_receivers = self.receivers.load(Ordering::SeqCst) == 0;
        no_senders || no_receivers
    }
}

/// The sending half of a bounded channel.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::SeqCst);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.senders.fetch_sub(1, Ordering::SeqCst);
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
    /// closed.
    #[allow(clippy::missing_panics_doc)]
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
                    let g = self.shared.waiting.lock().unwrap();
                    self.shared.waiting_cv.notify_all();
                    drop(g);
                    return Ok(());
                }
                Err(PushError::Full(i) | PushError::HighContention(i)) => {
                    item = i;
                }
            }

            // Slow path
            let g = self.shared.waiting.lock().unwrap();
            match self.shared.buffer.push(item) {
                Ok(()) => {
                    self.shared.waiting_cv.notify_all();
                    return Ok(());
                }
                Err(PushError::Full(i) | PushError::HighContention(i)) => {
                    item = i;
                }
            }

            self.shared.wait(g);
        }
    }
}

/// The receiving half of a bounded channel.
pub struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.receivers.fetch_add(1, Ordering::SeqCst);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receivers.fetch_sub(1, Ordering::SeqCst);
        let g = self.shared.waiting.lock().unwrap();
        self.shared.waiting_cv.notify_all();
        drop(g);
    }
}

impl<T> Receiver<T> {
    /// Receives the next item from the channel.
    ///
    /// Returns `Some(item)` when an item is available. Returns `None` when the
    /// channel is closed.
    pub fn recv(&self) -> Option<T> {
        loop {
            // Fast path
            match self.shared.buffer.pop() {
                Ok(Some(item)) => {
                    let g = self.shared.waiting.lock().unwrap();
                    self.shared.waiting_cv.notify_all();
                    drop(g);
                    return Some(item);
                }
                _ => {}
            }

            // Slow path
            let g = self.shared.waiting.lock().unwrap();
            match self.shared.buffer.pop() {
                Ok(Some(item)) => {
                    self.shared.waiting_cv.notify_all();
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
    use crate::sync::Barrier;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use crate::sync::model;
    use crate::thread::scope;

    /// A single item sent on a live channel is received unchanged: the basic
    /// end-to-end `send` → `recv` path with no blocking.
    #[test]
    fn send_then_recv_single() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            tx.send(7).unwrap();
            assert_eq!(rx.recv(), Some(7));
        });
    }

    /// Filling a buffer under capacity and draining it yields items in
    /// push order (FIFO), exercising the ring's slot sequencing without
    /// wrapping.
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
                    tx.send(42).unwrap();
                });
            });
        });
    }

    #[test]
    fn full_send_blocks_until_recv() {
        model(|| {
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
        });
    }

    /// Across many fill-then-drain rounds the writer/reader cursors wrap past
    /// the capacity, and FIFO order is preserved every round with the buffer
    /// empty after each drain. Guards the slot `sequence` recycle logic.
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

    /// A non-power-of-two requested capacity (3, rounded up to 4 physical
    /// slots) still delivers exactly 3 items in FIFO order. Guards the
    /// `len == self.cap` full check against the rounded-up physical slot
    /// count.
    #[test]
    fn capacity_non_power_of_two_fifo() {
        model(|| {
            let (tx, rx) = bounded::<usize>(3);
            let mut received = Vec::new();

            for i in 0..3 {
                tx.send(i).unwrap();
            }

            for _ in 0..3 {
                received.push(rx.recv().unwrap());
            }

            assert_eq!(
                received,
                vec![0, 1, 2],
                "non-power-of-two capacity is broken: recv hangs or returns wrong values"
            );
        });
    }

    /// `bounded(0)` must panic (the underlying `RingBuffer` requires at least
    /// one slot).
    #[test]
    #[should_panic(expected = "capacity must be greater than zero")]
    fn zero_capacity_is_rejected() {
        let _ = bounded::<i32>(0);
    }

    /// `recv` returns `None` when it observes that all senders are already
    /// dropped. This test never blocks, so it passes even if `Sender::Drop` did
    /// not notify waiters.
    #[test]
    fn recv_returns_none_when_all_senders_dropped() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            drop(tx);
            let result = rx.recv();
            assert_eq!(result, None);
        });
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

    /// Single-producer/single-consumer stress: 200 items flow through a small
    /// (cap 8) channel with the producer and consumer on separate scoped
    /// threads. Every value must arrive in order with no loss or duplication.
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

    /// Multiple cloned senders (3 producers) feeding one receiver: the sum and
    /// count of all received items must equal the multiset of pushed items.
    /// Exercises `Sender::clone` refcount increment and the `push` contention
    /// path.
    #[test]
    fn shared_sender_mpmc() {
        model(|| {
            const PRODUCERS: usize = 3;
            const PER_PRODUCER: usize = 100;
            const TOTAL: usize = PRODUCERS * PER_PRODUCER;
            let expected_sum: usize = (0..TOTAL).sum();

            let (tx, rx) = bounded::<usize>(16);
            let sum = Arc::new(AtomicUsize::new(0));
            let count = Arc::new(AtomicUsize::new(0));

            scope(|s| {
                for p in 0..PRODUCERS {
                    let tx = tx.clone();
                    s.spawn(move || {
                        for i in 0..PER_PRODUCER {
                            tx.send(p * PER_PRODUCER + i).unwrap();
                        }
                    });
                }

                let rx = rx.clone();
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
        });
    }

    /// One producer feeding multiple cloned receivers (3 consumers): the sum
    /// of every received value must equal the sum of every pushed value, so
    /// no item is lost or duplicated across competing `recv` calls. Exercises
    /// `Receiver::clone` and the `pop` contention path.
    #[test]
    fn shared_receiver_mpmc() {
        model(|| {
            const CONSUMERS: usize = 3;
            const TOTAL: usize = 300;
            let expected_sum: usize = (0..TOTAL).sum();

            let actual_sum = AtomicUsize::new(0);

            let (tx, rx) = bounded::<usize>(16);

            scope(|s| {
                s.spawn(move || {
                    for i in 0..TOTAL {
                        tx.send(i).unwrap();
                    }
                });

                for i in 0..CONSUMERS {
                    let data = (i, &rx, &actual_sum);
                    s.spawn(move || {
                        let (_, rx, actual_sum) = data;
                        while let Some(v) = rx.recv() {
                            actual_sum.fetch_add(v, Ordering::SeqCst);
                        }
                    });
                }
            });

            assert_eq!(
                actual_sum.load(Ordering::SeqCst),
                expected_sum,
                "item corruption"
            );
        });
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

    /// A receiver that is already blocked must be woken when the last sender is
    /// dropped and return `None`.
    #[test]
    fn blocked_recv_unblocks_when_last_sender_dropped() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            scope(|s| {
                s.spawn(|| {
                    rx.recv();
                });
                drop(tx);
            });
        });
    }

    /// A sender that is already blocked on a full buffer must be woken when the
    /// receiver drops and return `Err(())`.
    #[test]
    fn blocked_send_unblocks_when_receiver_dropped() {
        model(|| {
            let (tx, rx) = bounded::<i32>(1);
            tx.send(1).unwrap(); // fill the buffer

            scope(|s| {
                s.spawn(|| {
                    let _ = tx.send(2);
                });
                drop(rx);
            });
        });
    }

    /// `send` returns `Err(())` when called after the receiver has been dropped.
    #[test]
    fn send_returns_err_after_receiver_dropped() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            drop(rx);
            assert_eq!(tx.send(1), Err(()));
        });
    }

    /// Cloning a `Sender` increments the sender counter. The channel stays open
    /// until every clone is dropped.
    #[test]
    fn multiple_cloned_senders_disconnect() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            let tx2 = tx.clone();
            drop(tx);
            tx2.send(1).unwrap();
            assert_eq!(rx.recv(), Some(1));
            drop(tx2);
            assert_eq!(rx.recv(), None);
        });
    }

    /// Cloning a `Receiver` keeps the channel open from the receiver side. A
    /// sender can still send after one receiver clone is dropped.
    #[test]
    fn receiver_clone_keeps_channel_open() {
        model(|| {
            let (tx, rx) = bounded::<i32>(4);
            let rx2 = rx.clone();
            drop(rx);
            tx.send(1).unwrap();
            assert_eq!(rx2.recv(), Some(1));
        });
    }

    /// Heavy contention exercises the `RingBuffer::push`/`pop` `Err(())` paths
    /// and the end-of-stream disconnect path. If `Sender::Drop` fails to wake
    /// blocked consumers, the scope never returns and this test times out.
    #[test]
    fn heavy_contention_preserves_all_items() {
        model(|| {
            #[cfg(loom)]
            const PARAMS: (usize, usize) = (2, 32);
            #[cfg(not(loom))]
            const PARAMS: (usize, usize) = (8, 100);

            const PRODUCERS: usize = PARAMS.0;
            const CONSUMERS: usize = PARAMS.0;
            const PER_PRODUCER: usize = PARAMS.1;
            const TOTAL: usize = PRODUCERS * PER_PRODUCER;
            let expected_sum: usize = (0..TOTAL).sum();

            let (tx, rx) = bounded::<usize>(16);
            let count = Arc::new(AtomicUsize::new(0));
            let sum = Arc::new(AtomicUsize::new(0));

            scope(|s| {
                for p in 0..PRODUCERS {
                    let tx = tx.clone();
                    s.spawn(move || {
                        for i in 0..PER_PRODUCER {
                            tx.send(p * PER_PRODUCER + i).unwrap();
                        }
                    });
                }
                drop(tx);

                for _ in 0..CONSUMERS {
                    let rx = rx.clone();
                    let count = Arc::clone(&count);
                    let sum = Arc::clone(&sum);
                    s.spawn(move || {
                        while let Some(v) = rx.recv() {
                            let c = count.fetch_add(1, Ordering::SeqCst);
                            sum.fetch_add(v, Ordering::SeqCst);
                            if c + 1 == TOTAL {
                                break;
                            }
                        }
                    });
                }
                drop(rx);
            });

            assert_eq!(
                count.load(Ordering::SeqCst),
                TOTAL,
                "lost or duplicated items"
            );
            assert_eq!(sum.load(Ordering::SeqCst), expected_sum, "item corruption");
        });
    }

    /// When all senders are dropped while items are still buffered, `recv`
    /// must drain every resident item in FIFO order and only then return
    /// `None`.
    ///
    /// This pins the channel's *drain-on-close* property: the channel does not
    /// discard buffered items when the producer side disconnects.
    #[test]
    fn recv_drains_buffered_items_then_returns_none_on_close() {
        model(|| {
            let (tx, rx) = bounded::<usize>(4);
            for i in 0..4 {
                tx.send(i).unwrap();
            }
            drop(tx);
            for i in 0..4 {
                assert_eq!(rx.recv(), Some(i), "buffered item lost on close");
            }
            assert_eq!(rx.recv(), None, "recv must return None after draining");
        });
    }
}
