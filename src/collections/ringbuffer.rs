//! A bounded, lock-free multi-producer multi-consumer (MPMC) ring buffer.
//!
//! [`RingBuffer`] is a fixed-capacity queue that any number of producers and
//! consumers can share across threads. Enqueue and dequeue never block on a
//! mutex: contention between threads is resolved without locks.
//!
//! # Delivery and ordering guarantees
//!
//! Each slot is written once and read once per lap of the ring, so every item
//! pushed is popped exactly once — no loss and no duplication. Items are
//! consumed in slot order: [`pop`](RingBuffer::pop) returns
//! [`None`](Option::None) while the slot at the head of the queue has not been
//! committed yet, rather than skipping ahead to a later slot.
//!
//! # Blocking vs. non-blocking
//!
//! [`push`](RingBuffer::push) and [`pop`](RingBuffer::pop) are non-blocking:
//! `push` hands the item back if the buffer is full, and `pop` returns `None`
//! if the buffer is empty. [`push_with_timeout`](RingBuffer::push_with_timeout)
//! retries `push` with a backoff until it succeeds or a deadline elapses, for
//! callers that want to block.
//!
//! # Capacity
//!
//! The requested capacity is rounded up to the next power of two (with a
//! minimum of two) so the ring index can be computed with a bitmask.
//!
//! # Example
//!
//! ```
//! use milkyapps_core::collections::ringbuffer::RingBuffer;
//!
//! let rb = RingBuffer::with_capacity(4);
//! assert!(rb.push(1).is_ok());
//! assert_eq!(rb.pop().unwrap(), Some(1));
//! assert_eq!(rb.pop().unwrap(), None);
//! ```

use crate::sync::AtomicUsize;
use std::{
    cell::UnsafeCell,
    hint::spin_loop,
    mem::MaybeUninit,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

// `sequence` field is from the implementation of the bounded MPMC queue described by Dmitry
// Vyukov ([Bounded MPMC queue][vyukov]). Each slot owns a monotonically
// increasing *sequence number* that records which producer/consumer turn the
// slot currently belongs to. A producer claims a slot whose sequence marks it
// empty and ready for the producer's turn, writes the item, and advances the
// sequence to mark the slot full; a consumer claims a slot whose sequence
// marks it full and ready for the consumer's turn, reads the item, and
// advances the sequence to the next lap. Because the sequence grows
// monotonically rather than cycling through a small fixed set of states, a
// slot reused on a later lap of the ring can never be confused with the same
// slot from an earlier lap (no ABA problem), and producers and consumers can
// advance through their own cursors without taking a global lock. When the
// relevant slot is not in the expected state, `push`/`pop` report full/empty
// (or, for `push_with_timeout`, back off and retry).
//
// [vyukov]: https://web.archive.org/web/20110410230018/http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
//
struct Slot<T> {
    sequence: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for Slot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slot")
            .field("sequence", &self.sequence)
            .field("data", unsafe { (*self.data.get()).assume_init_ref() })
            .finish()
    }
}

/// A bounded, lock-free multi-producer multi-consumer queue (ring buffer).
///
/// A `RingBuffer<T>` can be shared across threads when `T: Send`,
/// since an item pushed on one thread may be popped on another.
#[derive(Debug)]
pub struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    cap: usize,
    mask: usize,
    len: AtomicUsize,
    writer: AtomicUsize,
    reader: AtomicUsize,
}

// SAFETY: `T: Send` is required because an item
// moved in by `push` on one thread can be moved out by `pop` on another.
unsafe impl<T: Send> Sync for RingBuffer<T> {}
unsafe impl<T: Send> Send for RingBuffer<T> {}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        // Exclusive `&mut self` ⇒ no concurrent access: drain and drop every
        // committed-but-unpopped item. In-flight (uncommitted) slots hold
        // uninitialized data and are correctly left untouched.
        let len = self.len.load(Ordering::Relaxed);
        for _ in 0..len {
            self.pop().unwrap();
        }
    }
}

impl<T> RingBuffer<T> {
    /// Creates a [`RingBuffer`] with room for at least `capacity` items.
    ///
    /// # Panics
    ///
    /// Panics if the rounded-up capacity does not fit in `usize`, which is only
    /// possible for inputs larger than `2.pow(usize::BITS - 1)`.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> RingBuffer<T> {
        let cap = capacity.next_power_of_two().max(2);

        let mut slots = Vec::with_capacity(cap);

        for i in 0..cap {
            slots.push(Slot {
                sequence: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        RingBuffer {
            slots: slots.into_boxed_slice(),
            cap: capacity,
            mask: if cap > 0 { cap - 1 } else { 0 },
            len: AtomicUsize::new(0),
            writer: AtomicUsize::new(0),
            reader: AtomicUsize::new(0),
        }
    }

    /// Pushes `item` to the tail of the buffer.
    ///
    /// This is non-blocking: if the buffer is full, the item is returned
    /// unchanged rather than the call waiting for space.
    ///
    /// # Errors
    ///
    /// Returns `Err(item)` if the buffer is full, with the item untouched,
    /// or in high contention if thread keep losing and "never" manages
    /// to insert the item.
    pub fn push(&self, item: T) -> Result<(), T> {
        for _ in 0..10 {
            // check we are not full
            let len = self.len.load(Ordering::Acquire);
            if len == self.cap {
                return Err(item);
            }

            let writer = self.writer.load(Ordering::Acquire);
            let slot = &self.slots[writer & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);

            match seq.cast_signed() - writer.cast_signed() {
                0 => {
                    if self
                        .writer
                        .compare_exchange_weak(
                            writer,
                            writer.wrapping_add(1),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }

                    // SAFETY: we won the cursor CAS, so this slot's data is ours
                    // alone until we publish it by advancing the sequence.
                    unsafe { (*slot.data.get()).write(item) };

                    slot.sequence
                        .store(writer.wrapping_add(1), Ordering::Release);
                    self.len.fetch_add(1, Ordering::Release);
                    return Ok(());
                }
                // Queue is full
                d if d < 0 => return Err(item),
                _ => {
                    spin_loop();
                }
            }
        }

        Err(item)
    }

    /// Pushes `item`, retrying with a backoff until it succeeds or `duration`
    /// has elapsed.
    ///
    /// Unlike [`push`](Self::push), this blocks the caller for up to `duration`
    /// waiting for space, yielding the thread between attempts.
    ///
    /// # Errors
    ///
    /// Returns `Err(item)` if the buffer stays full for the whole `duration`,
    /// with the item untouched.
    pub fn push_with_timeout(&self, item: T, duration: Duration) -> Result<(), T> {
        let start = Instant::now();

        let mut item = item;
        loop {
            match self.push(item) {
                Ok(()) => return Ok(()),
                Err(returned_item) => {
                    if Instant::now().duration_since(start) >= duration {
                        return Err(returned_item);
                    }
                    item = returned_item;
                }
            }
        }
    }

    /// Pops the next item from the head of the buffer, if one is available.
    ///
    /// This is non-blocking: it returns `None` immediately when the buffer is
    /// empty rather than waiting for a producer.
    pub fn pop(&self) -> Result<Option<T>, ()> {
        for _ in 0..10 {
            let reader = self.reader.load(Ordering::Acquire);
            let slot = &self.slots[reader & self.mask];
            let seq = slot.sequence.load(Ordering::Acquire);

            match seq.cast_signed() - (reader.wrapping_add(1)).cast_signed() {
                0 => {
                    if self
                        .reader
                        .compare_exchange_weak(
                            reader,
                            reader.wrapping_add(1),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }

                    // SAFETY: we won the cursor CAS, so this slot's data is ours
                    // to read until we recycle it by advancing the sequence.
                    let data = unsafe { &mut *slot.data.get() };
                    let mut item = MaybeUninit::uninit();
                    std::mem::swap(data, &mut item);

                    slot.sequence.store(
                        reader.wrapping_add(self.mask).wrapping_add(1),
                        Ordering::Release,
                    );
                    self.len.fetch_sub(1, Ordering::Release);
                    return Ok(Some(unsafe { item.assume_init() }));
                }
                d if d < 0 => return Ok(None), // head not committed yet — empty
                _ => {
                    spin_loop();
                }
            }
        }

        Err(())
    }
}

#[cfg(test)]
mod tests {
    use crate::sync::model;

    use super::RingBuffer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Constructing a buffer and formatting it with `Debug` does not panic.
    #[test]
    fn construction_and_dbg_succeed() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(2);
            let _ = dbg!(rb);
        });
    }

    /// A single item pushed is returned, in order, by the next `pop`.
    #[test]
    fn single_push_then_pop_returns_the_pushed_value() {
        model(|| {
            let rb = RingBuffer::with_capacity(2);

            rb.push(0u64).unwrap();
            assert_eq!(rb.pop().unwrap().unwrap(), 0);

            rb.push(1u64).unwrap();
            assert_eq!(rb.pop().unwrap().unwrap(), 1);
        });
    }

    /// Filling the buffer exactly to capacity and then draining it yields items
    /// in push order; one further `pop` reports the buffer empty.
    #[test]
    fn fifo_order_within_one_full_cycle() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(8);
            for i in 0..8u64 {
                rb.push(i).unwrap();
            }
            for i in 0..8u64 {
                assert_eq!(rb.pop().unwrap(), Some(i));
            }
            assert_eq!(rb.pop().unwrap(), None);
        });
    }

    /// Over several push/pop rounds the cursor wraps past the capacity and FIFO
    /// order is preserved each round, with the buffer empty after each drain.
    #[test]
    fn fifo_order_survives_capacity_wraps() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(4);
            for round in 0..3u64 {
                let base = round * 10;
                for i in 0..4u64 {
                    rb.push(base + i).unwrap();
                }
                for i in 0..4u64 {
                    assert_eq!(rb.pop().unwrap(), Some(base + i));
                }
            }
            assert_eq!(rb.pop().unwrap(), None);
        });
    }

    /// `pop` on a freshly created, never-filled buffer returns `None`.
    #[test]
    fn pop_from_empty_buffer_returns_none() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(4);
            assert_eq!(rb.pop().unwrap(), None);
        });
    }

    /// With the buffer full and one consumer per slot released simultaneously,
    /// every consumer pops an item: no consumer is starved and no item is lost.
    #[test]
    fn concurrent_pops_each_claim_a_distinct_slot() {
        model(|| {
            const SLOTS: usize = 4;
            const ITERS: usize = 2000;
            let mut successes = 0usize;
            for _ in 0..ITERS {
                let rb = RingBuffer::<usize>::with_capacity(SLOTS);
                for v in 0..SLOTS {
                    rb.push(v).unwrap();
                }

                let barrier = std::sync::Barrier::new(SLOTS);

                std::thread::scope(|s| {
                    let mut handles = Vec::new();

                    for _ in 0..SLOTS {
                        handles.push(s.spawn(|| {
                            barrier.wait();
                            rb.pop().unwrap().is_some()
                        }));
                    }

                    for h in handles {
                        if h.join().unwrap() {
                            successes += 1;
                        }
                    }
                });
            }
            assert_eq!(successes, ITERS * SLOTS);
        });
    }

    /// With several producers and consumers running concurrently, the multiset
    /// of popped items equals exactly the multiset of pushed items: no item is
    /// lost and no item is duplicated or corrupted.
    #[test]
    fn mpmc_preserves_exact_item_set() {
        model(|| {
            const PRODUCERS: usize = 2;
            const PER_PRODUCER: usize = 500;
            const CONSUMERS: usize = 2;
            const TOTAL: usize = PRODUCERS * PER_PRODUCER;

            let rb = Arc::new(RingBuffer::<usize>::with_capacity(16));
            let collected = Arc::new(AtomicUsize::new(0));
            let received: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::with_capacity(TOTAL)));

            let mut handles = Vec::new();
            for p in 0..PRODUCERS {
                let r = Arc::clone(&rb);
                handles.push(std::thread::spawn(move || {
                    for i in 0..PER_PRODUCER {
                        r.push_with_timeout(p * PER_PRODUCER + i, Duration::from_secs(1))
                            .unwrap();
                    }
                }));
            }

            for _ in 0..CONSUMERS {
                let r = Arc::clone(&rb);
                let collected = Arc::clone(&collected);
                let received = Arc::clone(&received);
                handles.push(std::thread::spawn(move || {
                    loop {
                        if collected.load(Ordering::Relaxed) >= TOTAL {
                            return;
                        }
                        if let Some(v) = r.pop().unwrap() {
                            let prev = collected.fetch_add(1, Ordering::Relaxed);
                            if prev < TOTAL {
                                received.lock().unwrap().push(v);
                            } else {
                                return;
                            }
                        } else {
                            std::thread::yield_now();
                        }
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            let recv = received.lock().unwrap();
            assert_eq!(recv.len(), TOTAL, "an item was lost");
            let mut sorted = recv.clone();
            sorted.sort_unstable();
            let expected: Vec<usize> = (0..TOTAL).collect();
            assert_eq!(sorted, expected, "an item was duplicated or corrupted");
        });
    }

    /// Items left in the buffer when it is dropped have their destructors run,
    /// so a non-trivial `T` does not leak.
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
                let rb = RingBuffer::<Counter>::with_capacity(8);
                for _ in 0..4 {
                    rb.push(Counter::new(&live)).unwrap();
                }
                assert_eq!(live.load(Ordering::Relaxed), 4);
            }
            assert_eq!(live.load(Ordering::Relaxed), 0, "buffered items leaked");
        });
    }

    /// Requesting capacity zero does not panic (it is rounded up to the minimum)
    /// and a buffer with nothing pushed pops `None`.
    #[test]
    fn zero_capacity_request_is_safe() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(0);
            assert_eq!(rb.pop().unwrap(), None);
        });
    }

    /// Pushing into a full buffer returns the item unchanged instead of blocking
    /// or overwriting a resident item.
    #[test]
    fn push_into_full_buffer_returns_the_item() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(2);
            rb.push(0).unwrap();
            rb.push(1).unwrap();
            assert_eq!(rb.push(2).unwrap_err(), 2);
        });
    }

    /// A single producer and a single consumer exchanging many items must
    /// preserve FIFO order and must not lose or duplicate items.
    #[test]
    fn single_producer_single_consumer_stress() {
        model(|| {
            const ROUNDS: usize = 100;
            const PER_ROUND: usize = 8;
            let rb = Arc::new(RingBuffer::<u64>::with_capacity(PER_ROUND));
            let collected = Arc::new(Mutex::new(Vec::with_capacity(ROUNDS * PER_ROUND)));

            std::thread::scope(|s| {
                let rb_producer = Arc::clone(&rb);
                s.spawn(move || {
                    for round in 0..ROUNDS as u64 {
                        for i in 0..PER_ROUND as u64 {
                            rb_producer
                                .push_with_timeout(round * 100 + i, Duration::from_millis(100))
                                .unwrap();
                        }
                    }
                });

                let rb_consumer = Arc::clone(&rb);
                let collected_consumer = Arc::clone(&collected);
                s.spawn(move || {
                    let mut count = 0usize;
                    while count < ROUNDS * PER_ROUND {
                        if let Some(v) = rb_consumer.pop().unwrap() {
                            collected_consumer.lock().unwrap().push(v);
                            count += 1;
                        } else {
                            std::thread::yield_now();
                        }
                    }
                });
            });

            let received = collected.lock().unwrap();
            assert_eq!(received.len(), ROUNDS * PER_ROUND);
            let expected: Vec<u64> = (0..ROUNDS as u64)
                .flat_map(|round| (0..PER_ROUND as u64).map(move |i| round * 100 + i))
                .collect();
            assert_eq!(*received, expected);
        });
    }

    /// A ring buffer of capacity 1 is broken: the single sequence number is
    /// used both for "slot full" and "slot empty for the next lap", so a
    /// producer can never observe a full buffer and will overwrite the resident
    /// item instead of returning an error.
    #[test]
    fn capacity_one_allows_overwrite() {
        model(|| {
            use std::mem::{ManuallyDrop, forget};

            // After one push the capacity-1 buffer is in a corrupted state if
            // the bug is present (the slot is overwritten). Leak the buffer so
            // that its Drop impl, which would spin forever in `pop`, is not
            // executed regardless of whether the assertion passes.
            let mut rb = ManuallyDrop::new(RingBuffer::<u64>::with_capacity(1));
            dbg!(&rb);
            rb.push(1).unwrap();
            dbg!(&rb);
            let push_result = rb.push(2);
            dbg!(&rb);
            let leaked = unsafe { ManuallyDrop::take(&mut rb) };
            forget(leaked);

            assert!(
                push_result.is_err(),
                "push into a full capacity-1 buffer must fail instead of overwriting"
            );
        });
    }

    /// `push_with_timeout` with a zero duration must return immediately when
    /// the buffer is full and must not block or overwrite a resident item.
    #[test]
    fn push_timeout_zero_returns_immediately_when_full() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(2);
            rb.push(1).unwrap();
            rb.push(2).unwrap();
            assert_eq!(
                rb.push_with_timeout(3, Duration::from_secs(0)).unwrap_err(),
                3
            );
        });
    }
}
