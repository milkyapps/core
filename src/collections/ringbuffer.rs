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
//! assert_eq!(rb.pop(), Some(1));
//! assert_eq!(rb.pop(), None);
//! ```

use crate::sync::AtomicUsize;
use std::{
    cell::UnsafeCell,
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
#[derive(Debug)]
struct Slot<T> {
    sequence: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded, lock-free multi-producer multi-consumer queue (ring buffer).
///
/// A `RingBuffer<T>` can be shared across threads when `T: Send`,
/// since an item pushed on one thread may be popped on another.
#[derive(Debug)]
pub struct RingBuffer<T> {
    slots: Vec<Slot<T>>,
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
        while self.pop().is_some() {}
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
        let capacity = if capacity == 0 {
            2
        } else {
            capacity.next_power_of_two()
        };

        let mut slots = Vec::with_capacity(capacity);

        for i in 0..capacity {
            slots.push(Slot {
                sequence: AtomicUsize::new(i),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        RingBuffer {
            slots,
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
    /// Returns `Err(item)` if the buffer is full, with the item untouched.
    pub fn push(&self, item: T) -> Result<(), T> {
        let mask = self.slots.len() - 1;

        loop {
            let writer = self.writer.load(Ordering::Acquire);
            let slot = &self.slots[writer & mask];
            let seq = slot.sequence.load(Ordering::Acquire);

            match seq.cast_signed() - writer.cast_signed() {
                0 => {
                    if self
                        .writer
                        .compare_exchange_weak(
                            writer,
                            writer + 1,
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

                    slot.sequence.store(writer + 1, Ordering::Release);
                    return Ok(());
                }
                // Queue is full
                d if d < 0 => return Err(item),
                _ => {
                    std::thread::yield_now();
                }
            }
        }
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
    pub fn pop(&self) -> Option<T> {
        let mask = self.slots.len() - 1;

        loop {
            let reader = self.reader.load(Ordering::Acquire);
            let slot = &self.slots[reader & mask];
            let seq = slot.sequence.load(Ordering::Acquire);

            match seq.cast_signed() - (reader + 1).cast_signed() {
                0 => {
                    if self
                        .reader
                        .compare_exchange_weak(
                            reader,
                            reader + 1,
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

                    slot.sequence.store(reader + mask + 1, Ordering::Release);

                    return Some(unsafe { item.assume_init() });
                }
                d if d < 0 => return None, // head not committed yet — empty
                _ => {
                    std::thread::yield_now();
                }
            }
        }
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
            assert_eq!(rb.pop().unwrap(), 0);

            rb.push(1u64).unwrap();
            assert_eq!(rb.pop().unwrap(), 1);
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
                assert_eq!(rb.pop(), Some(i));
            }
            assert_eq!(rb.pop(), None);
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
                    assert_eq!(rb.pop(), Some(base + i));
                }
            }
            assert_eq!(rb.pop(), None);
        });
    }

    /// `pop` on a freshly created, never-filled buffer returns `None`.
    #[test]
    fn pop_from_empty_buffer_returns_none() {
        model(|| {
            let rb = RingBuffer::<u64>::with_capacity(4);
            assert_eq!(rb.pop(), None);
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
                            rb.pop().is_some()
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
                        if let Some(v) = r.pop() {
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
            assert_eq!(rb.pop(), None);
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
}
