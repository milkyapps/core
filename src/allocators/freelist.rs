use crate::ptr::TaggedPtr;
use crate::{ptr::AtomicTagged, smr::hazard_ptrs::HazardPointers};
use std::fmt::Debug;
use std::{
    alloc::{Layout, LayoutError, dealloc, handle_alloc_error},
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Node {
    next: AtomicTagged<Node>,
}

/// Freelist is a lockfree stack of memory buffers.
/// Buffer are allocated as needed on `pop`.
///
/// When the buffer is returned using `push` a threshold is tested,
/// if more than the threshold is allocated, the pushed buffer
/// is deallocated.
///
/// All buffers are deallocated on drop.
pub struct Freelist {
    head: AtomicTagged<Node>,
    layout: Layout,
    offset: usize,
    qty_allocated: AtomicUsize,
    qty_in_list: AtomicUsize,
    qty_retired: AtomicUsize,
    alloc_max: usize,
    list_max: usize,
    retired_max: usize,
    hp: HazardPointers<Node>,
}

impl Debug for Freelist {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut items = vec![];
        let mut current = self.head.load(Ordering::Acquire);
        while !current.ptr().is_null() {
            items.push(current);
            current = unsafe { (*current.ptr()).next.load(Ordering::Acquire) };
        }

        f.debug_struct("Freelist")
            .field("items", &items)
            .field("layout", &self.layout)
            .field("offset", &self.offset)
            .field("qty_allocated", &self.qty_allocated.load(Ordering::Relaxed))
            .field("qty_in_list", &self.qty_in_list.load(Ordering::Relaxed))
            .field("qty_retired", &self.qty_retired.load(Ordering::Relaxed))
            .field("alloc_max", &self.alloc_max)
            .field("list_max", &self.list_max)
            .field("retired_max", &self.retired_max)
            .field("hp", &"..")
            .finish()
    }
}

impl Drop for Freelist {
    fn drop(&mut self) {
        self.deallocate_retired();

        let mut buffer = self.head.load(Ordering::Acquire);
        while !buffer.ptr().is_null() {
            // SAFETY: inside drop we are sure nobody has access to
            let current = unsafe { &*buffer.ptr() };
            let next = current.next.load(Ordering::Relaxed);

            unsafe { dealloc(buffer.ptr().cast::<u8>(), self.layout) };
            buffer = next;
        }
    }
}

impl Freelist {
    /// Creates a freelist for a specific layout
    ///
    /// # Errors
    ///
    /// Function fails if cannot find a common layout for the requested layout
    /// and the layout for the `IntrusiveNode` type.
    pub fn new(layout: Layout) -> Result<Freelist, LayoutError> {
        let (layout, offset) = Layout::new::<Node>().extend(layout)?;
        Ok(Freelist {
            head: AtomicTagged::new(TaggedPtr::new(null_mut(), 0)),
            layout,
            offset,
            qty_allocated: AtomicUsize::new(0),
            qty_in_list: AtomicUsize::new(0),
            qty_retired: AtomicUsize::new(0),
            alloc_max: 1024,
            list_max: 1024,
            retired_max: 4,
            hp: HazardPointers::with_capacity(1024, 16),
        })
    }

    /// Return the freelist layout
    pub fn layout(&self) -> &Layout {
        &self.layout
    }

    fn deallocate_retired(&self) {
        let mut ptrs = vec![];
        self.hp.reclaim(&mut ptrs);

        self.qty_retired.fetch_sub(ptrs.len(), Ordering::Relaxed);
        self.qty_allocated.fetch_sub(ptrs.len(), Ordering::Relaxed);
        for ptr in ptrs {
            unsafe { dealloc(ptr.cast::<u8>(), self.layout) };
        }
    }

    /// Buffer will go back to the free list if the allocated memory is smaller
    /// than the threshold. If bigger, buffer will be retired.
    ///
    /// When the ammount of memory retired pass the threshold, memory will be reclaimed and deallocated.
    pub fn dealloc(&self, buffer: TaggedPtr<()>) -> bool {
        if buffer.ptr().is_null() {
            false
        } else {
            let mut buffer = unsafe { buffer.byte_sub::<Node>(self.offset) };
            buffer.wrapping_add_tag(1);
            let Some(local) = self.hp.local() else {
                return false;
            };

            if self.qty_in_list.load(Ordering::Relaxed) >= self.list_max {
                let Some(g) = local.protect(buffer.ptr()) else {
                    local.finish();
                    return false;
                };
                let _ = g.retire();
                local.finish();

                let retired = self.qty_retired.fetch_add(1, Ordering::Relaxed) + 1;
                if retired > self.retired_max {
                    self.deallocate_retired();
                }

                return true;
            }

            loop {
                let head = self.head.load(Ordering::Acquire);
                let head_ptr = head.ptr();
                let Some(g) = local.protect(head_ptr) else {
                    local.finish();
                    return false;
                };
                if head_ptr == self.head.load(Ordering::Acquire).ptr() {
                    // SAFETY: load-protect-check cycle complete. head is safe to be used here
                    unsafe { (*buffer.ptr()).next.store(head, Ordering::Release) };

                    if self
                        .head
                        .compare_exchange_weak(head, buffer, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        self.qty_in_list.fetch_add(1, Ordering::Relaxed);
                        g.unprotect();
                        local.finish();
                        return true;
                    }
                }

                g.unprotect();
            }
        }
    }

    /// Will pop a buffer from the list. If none exist, a buffer
    /// will be allocated from the global allocator.
    pub fn alloc(&self) -> Option<TaggedPtr<()>> {
        let local = self.hp.local()?;
        loop {
            let head = self.head.load(Ordering::Acquire);
            if head.ptr().is_null() {
                local.finish();

                if self.qty_allocated.load(Ordering::Relaxed) >= self.alloc_max {
                    return None;
                }

                // SAFETY: We check ptr is null before deref it
                // CLIPPY is incorrect here as alloc does return a pointer alligned to Node
                #[allow(clippy::cast_ptr_alignment)]
                let node = unsafe { std::alloc::alloc(self.layout).cast::<Node>() };
                if node.is_null() {
                    handle_alloc_error(self.layout);
                }
                self.qty_allocated.fetch_add(1, Ordering::Relaxed);
                let ptr = unsafe { node.byte_add(self.offset).cast::<()>() };
                return Some(TaggedPtr::new(ptr, 0));
            }

            let Some(g) = local.protect(head.ptr()) else {
                local.finish();
                return None;
            };
            if head.ptr() == self.head.load(Ordering::Acquire).ptr() {
                // SAFETY: head is load-protect-check so it is safe to deref
                let next = unsafe { (*head.ptr()).next.load(Ordering::Acquire) };
                match self.head.compare_exchange_weak(
                    head,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(new_head) => {
                        self.qty_in_list.fetch_sub(1, Ordering::Relaxed);
                        g.unprotect();
                        local.finish();
                        // ptr.incr_tag(); // TODO should we increase tag here?
                        return Some(unsafe { new_head.byte_add(self.offset) });
                    }
                    Err(_) => {
                        g.unprotect();
                    }
                }
            } else {
                g.unprotect();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{alloc::Layout, sync::atomic::Ordering};

    use crate::{allocators::freelist::Freelist, ptr::TaggedPtr, sync::model};

    #[test]
    fn default_and_drop() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let _ = Freelist::new(layout);
        });
    }

    #[test]
    fn test_alloc_dealloc() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            let buffer1 = s.alloc().unwrap();
            let buffer2 = s.alloc().unwrap();
            dbg!(&s);
            s.dealloc(buffer1);
            dbg!(&s);
            s.dealloc(buffer2);
            dbg!(&s);

            let buffer = s.alloc().unwrap();
            s.dealloc(buffer);
            dbg!(&s);
        });
    }

    #[test]
    fn freelist_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Freelist>();
        assert_sync::<Freelist>();
    }

    #[test]
    fn alloc_dealloc_recycles_same_buffer() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            assert!(s.head.load(Ordering::Acquire).ptr().is_null());
            let p0 = s.alloc().unwrap();
            let p0_ptr = p0.ptr();

            assert!(s.head.load(Ordering::Acquire).ptr().is_null());
            s.dealloc(p0);
            assert!(!s.head.load(Ordering::Acquire).ptr().is_null());

            let p1 = s.alloc().unwrap();
            let p1_ptr = p1.ptr();
            assert!(s.head.load(Ordering::Acquire).ptr().is_null());

            s.dealloc(p1);
            assert!(!s.head.load(Ordering::Acquire).ptr().is_null());

            assert_eq!(
                p0_ptr, p1_ptr,
                "pop must recycle the buffer that was just pushed"
            );
        });
    }

    #[test]
    fn alloc_aba_corrupts_freelist() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            // Prepopulate the freelist with exactly two buffers so that
            // head = A -> B -> null.
            let a = s.alloc().unwrap();
            let b = s.alloc().unwrap();
            assert!(s.dealloc(b));
            assert!(s.dealloc(a));

            let mut p: Option<TaggedPtr<()>> = None;

            crate::thread::scope(|scope| {
                scope.spawn(|| {
                    let q1 = s.alloc().unwrap();
                    let q2 = s.alloc().unwrap();
                    assert!(s.dealloc(q1));
                    assert!(s.dealloc(q2));
                });
                scope.spawn(|| {
                    p = Some(s.alloc().unwrap());
                });
            });

            // Invariant for a correct freelist: every `alloc` returns a buffer
            // that is either a distinct node popped from the list or a freshly
            // allocated one. The same address can never be handed out twice
            // without an intervening `dealloc`. In the ABA schedule the list
            // ends in a self-loop on one node, so repeated `alloc` returns the
            // same pointer over and over.
            let mut seen = Vec::new();
            for _ in 0..3 {
                if let Some(p) = s.alloc() {
                    seen.push(p);
                }
            }

            let mut duplicate = false;
            for i in 0..seen.len() {
                for j in (i + 1)..seen.len() {
                    if seen[i].ptr() == seen[j].ptr() {
                        duplicate = true;
                    }
                }
            }

            for buffer in seen {
                assert!(s.dealloc(buffer));
            }

            if let Some(buffer) = p {
                assert!(s.dealloc(buffer));
            }

            assert!(
                !duplicate,
                "freelist handed out the same buffer twice without an intervening dealloc (ABA self-loop)"
            );
        });
    }

    #[test]
    fn concurrent_stress() {
        model(|| {
            const THREADS: usize = 4;
            const ITERS: usize = 200;

            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            crate::thread::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|| {
                        let mut held = Vec::new();
                        for _ in 0..ITERS {
                            if let Some(p) = s.alloc() {
                                held.push(p);
                            }

                            if held.len() > 8 {
                                let p = held.pop().unwrap();
                                s.dealloc(p);
                            }
                        }

                        for p in held {
                            s.dealloc(p);
                        }
                    });
                }
            });
        })
    }

    #[test]
    fn alloc_is_bounded() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();
            assert!(s.alloc_max > 0, "alloc_max must be positive");

            let mut buffers = vec![];
            for _ in 0..=s.alloc_max {
                buffers.extend(s.alloc());
            }

            assert_eq!(
                buffers.len(),
                s.alloc_max,
                "Do not allocate more than threshold"
            );

            for buffer in buffers {
                s.dealloc(buffer);
            }
        })
    }

    #[test]
    fn dealloc_must_deallocate_buffers_once_allocated_exceeds_threshold() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let mut s = Freelist::new(layout).unwrap();
            s.alloc_max = 10;
            s.list_max = 5;
            s.retired_max = 4;

            // Allocate to the limit
            let mut buffers = vec![];
            for _ in 0..s.alloc_max {
                buffers.extend(s.alloc());
            }

            // Deallocate to the limit of the freelist
            // No one will be retired
            for _ in 0..s.list_max {
                assert!(s.dealloc(buffers.pop().unwrap()));
            }
            assert_eq!(s.qty_in_list.load(Ordering::Relaxed), 5);
            assert_eq!(s.qty_retired.load(Ordering::Relaxed), 0);

            // Deallocate to the limit of retired_max
            // No one will be reclaimed yet
            for _ in 0..s.retired_max {
                assert!(s.dealloc(buffers.pop().unwrap()));
            }
            assert_eq!(s.qty_in_list.load(Ordering::Relaxed), 5);
            assert_eq!(s.qty_retired.load(Ordering::Relaxed), 4);

            // Now all the retired will be reclaimed
            assert!(s.dealloc(buffers.pop().unwrap()));
            assert_eq!(s.qty_in_list.load(Ordering::Relaxed), 5);
            assert_eq!(s.qty_retired.load(Ordering::Relaxed), 0);
        })
    }
}
