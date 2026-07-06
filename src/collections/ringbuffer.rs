use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
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
    buffer: Vec<Slot<T>>,
    writer: AtomicUsize,
    reader: AtomicUsize,
}

impl<T> RingBuffer<T> {
    pub fn with_capacity(capacity: usize) -> RingBuffer<T> {
        let mut buffer = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buffer.push(Slot {
                state: AtomicUsize::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
        }

        RingBuffer {
            buffer,
            writer: AtomicUsize::new(0),
            reader: AtomicUsize::new(0),
        }
    }

    pub fn push(&self, item: T) {
        let cap = self.buffer.len();
        loop {
            let slot = self.writer.fetch_add(1, Ordering::Relaxed);
            let idx = slot % cap;

            match self.buffer[idx]
                .state
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {}
                Err(_) => continue,
            }

            // SAFETY: we are the only thread with full control of this slot
            let data = unsafe { &mut *self.buffer[idx].data.get() };
            data.write(item);

            match self.buffer[idx]
                .state
                .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    break;
                }
                Err(_) => unreachable!(),
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        let cap = self.buffer.len();
        loop {
            let slot = self.reader.load(Ordering::Acquire);
            let idx = slot % cap;

            match self.buffer[idx]
                .state
                .compare_exchange(2, 3, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {}
                Err(_) => return None,
            }

            self.reader.fetch_add(1, Ordering::Relaxed);

            // SAFETY: we are the only thread reading this
            let data = unsafe { &mut *self.buffer[idx].data.get() };
            let mut item = MaybeUninit::uninit();
            std::mem::swap(data, &mut item);

            match self.buffer[idx]
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

    #[test]
    fn with_capacity_and_drop() {
        let rb = RingBuffer::<u64>::with_capacity(2);
        dbg!(rb);
    }

    #[test]
    fn push_and_pop() {
        let rb = RingBuffer::with_capacity(2);

        rb.push(0u64);
        let v = rb.pop();
        assert_eq!(v.unwrap(), 0);

        rb.push(1u64);
        let v = rb.pop();
        assert_eq!(v.unwrap(), 1);
    }
}
