use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

#[derive(Debug)]
struct Slot<T> {
    /// 0 = empty, 1 = being written, 2 = write finished, 3 = being read
    state: AtomicUsize,
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self {
            state: Default::default(),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

#[derive(Debug)]
struct RingBuffer<T> {
    slots: Vec<Slot<T>>,
    writer: AtomicUsize,
    reader: AtomicUsize,
}

unsafe impl<T: Send> Sync for RingBuffer<T> {}
unsafe impl<T: Send> Send for RingBuffer<T> {}

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        for slot in self.slots.iter_mut() {
            let state = slot.state.load(Ordering::Relaxed);
            // This is initialized
            if state == 2 {
                let mut item = MaybeUninit::uninit();
                std::mem::swap(slot.data.get_mut(), &mut item);

                let _ = unsafe { item.assume_init() };
            }
        }
    }
}

impl<T> RingBuffer<T> {
    pub fn with_capacity(capacity: usize) -> RingBuffer<T> {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(Slot {
                state: AtomicUsize::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
        }

        RingBuffer {
            slots,
            writer: AtomicUsize::new(0),
            reader: AtomicUsize::new(0),
        }
    }

    pub fn push_with_timeout(&self, item: T, duration: Duration) -> Result<(), T> {
        let start = Instant::now();

        let mut item = item;
        loop {
            match self.push(item) {
                Ok(_) => return Ok(()),
                Err(returned_item) => {
                    if Instant::now().duration_since(start) >= duration {
                        return Err(returned_item);
                    }
                    item = returned_item;
                }
            }
        }
    }

    pub fn push(&self, item: T) -> Result<(), T> {
        let cap = self.slots.len();
        if cap == 0 {
            return Err(item);
        }
        let slot = self.writer.fetch_add(1, Ordering::Relaxed);
        let idx = slot % cap;

        match self.slots[idx]
            .state
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {}
            Err(_) => return Err(item),
        }

        // SAFETY: we are the only thread with full control of this slot
        let data = unsafe { &mut *self.slots[idx].data.get() };
        data.write(item);

        self.slots[idx]
            .state
            .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
            .unwrap();

        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        let cap = self.slots.len();
        if cap == 0 {
            return None;
        }
        loop {
            let slot = self.reader.load(Ordering::Acquire);
            let idx = slot % cap;

            match self.slots[idx]
                .state
                .compare_exchange(2, 3, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {}
                // We know there is no items
                Err(0) => return None,
                // Writer is writing to this slot,
                // Or a reader is reading it,
                // we must spin
                Err(1 | 3) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(state) => {
                    unreachable!("{state}")
                }
            }

            self.reader.fetch_add(1, Ordering::Relaxed);

            // SAFETY: we are the only thread reading this
            let data = unsafe { &mut *self.slots[idx].data.get() };
            let mut item = MaybeUninit::uninit();
            std::mem::swap(data, &mut item);

            match self.slots[idx]
                .state
                .compare_exchange(3, 0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break Some(unsafe { item.assume_init() }),
                Err(_) => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RingBuffer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn with_capacity_and_drop() {
        let rb = RingBuffer::<u64>::with_capacity(2);
        dbg!(rb);
    }

    #[test]
    fn push_and_pop() {
        let rb = RingBuffer::with_capacity(2);

        rb.push(0u64).unwrap();
        let v = rb.pop();
        assert_eq!(v.unwrap(), 0);

        rb.push(1u64).unwrap();
        let v = rb.pop();
        assert_eq!(v.unwrap(), 1);
    }

    /// Single-thread FIFO and index wraparound are correct: each slot is
    /// written once and read once per cycle (no duplication, no torn reads),
    /// and the Release/Acquire on the state word publishes the data write to
    /// the reader. These tests pass.
    #[test]
    fn single_thread_fifo_within_capacity() {
        let rb = RingBuffer::<u64>::with_capacity(8);
        for i in 0..8u64 {
            rb.push(i).unwrap();
        }
        for i in 0..8u64 {
            assert_eq!(rb.pop(), Some(i));
        }
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn wraps_around_correctly() {
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
    }

    #[test]
    fn pop_on_empty_returns_none() {
        let rb = RingBuffer::<u64>::with_capacity(4);
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn pop_contention_stress() {
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
    }

    #[test]
    fn mpmc_no_loss_no_duplication() {
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
    }

    #[test]
    fn drop_must_drop_pushed_items() {
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

        let live = Arc::new(AtomicUsize::new(0));
        {
            let rb = RingBuffer::<Counter>::with_capacity(8);
            for _ in 0..4 {
                rb.push(Counter::new(&live)).unwrap();
            }
            assert_eq!(live.load(Ordering::Relaxed), 4);
        }
        assert_eq!(
            live.load(Ordering::Relaxed),
            0,
            "buffered items leaked: RingBuffer has no Drop impl and MaybeUninit does not drop T"
        );
    }

    #[test]
    fn pop_on_zero_capacity_does_not_panic() {
        let rb = RingBuffer::<u64>::with_capacity(0);
        assert_eq!(rb.pop(), None);
    }

    #[test]
    fn push_on_full_buffer_must_fail() {
        let rb = RingBuffer::<u64>::with_capacity(2);
        rb.push(0).unwrap();
        rb.push(1).unwrap();
        assert_eq!(rb.push(2).unwrap_err(), 2);
    }
}
