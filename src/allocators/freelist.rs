use crate::smr::hazard_ptrs::HazardPointers;
use crate::sync::AtomicPtr;
use std::{
    alloc::{Layout, LayoutError, dealloc, handle_alloc_error},
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
};

struct IntrusiveNode {
    next: AtomicPtr<IntrusiveNode>,
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
    head: AtomicPtr<IntrusiveNode>,
    layout: Layout,
    allocated: AtomicUsize,
    retired: AtomicUsize,
    max_allocated: usize,
    hp: HazardPointers<IntrusiveNode>,
}

impl Drop for Freelist {
    fn drop(&mut self) {
        self.deallocate_retired();

        let mut ptr = self.head.load(Ordering::Acquire);
        while !ptr.is_null() {
            // SAFETY: inside drop we are sure nobody has access to
            let current = unsafe { &*ptr };
            let next = current.next.load(Ordering::Relaxed);

            unsafe { dealloc(ptr.cast::<u8>(), self.layout) };
            ptr = next;
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
        let node_size = std::mem::size_of::<IntrusiveNode>();
        let node_align = std::mem::align_of::<IntrusiveNode>();

        let align = layout.align();
        let align = if align.is_multiple_of(node_align) {
            align
        } else if node_align.is_multiple_of(align) {
            node_align
        } else {
            align * node_align
        };

        let layout = Layout::from_size_align(layout.size().max(node_size), align)?;
        let max_allocated = layout.size() * 1024;
        Ok(Freelist {
            head: AtomicPtr::new(null_mut()),
            layout,
            allocated: AtomicUsize::new(0),
            retired: AtomicUsize::new(0),
            max_allocated,
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

        for ptr in ptrs {
            unsafe { dealloc(ptr.cast::<u8>(), self.layout) };
        }
    }

    /// Buffer will go back to the free list if the allocated memory is smaller
    /// than the threshold. If bigger, buffer will be retired.
    ///
    /// When the ammount of memory retired pass the threshold, memory will be reclaimed and deallocated.
    pub fn dealloc(&self, ptr: *mut ()) -> bool {
        if ptr.is_null() {
            false
        } else {
            let Some(local) = self.hp.local() else {
                return false;
            };

            let allocated = self.allocated.load(Ordering::Relaxed);
            if allocated > self.max_allocated {
                let Some(g) = local.protect(ptr.cast::<IntrusiveNode>()) else {
                    local.finish();
                    return false;
                };

                let _ = g.retire();
                local.finish();
                self.allocated
                    .fetch_sub(self.layout().size(), Ordering::Relaxed);

                // If we have too many retired, deallocate them
                let old_retired = self
                    .retired
                    .fetch_add(self.layout().size(), Ordering::Relaxed);
                if old_retired > self.max_allocated {
                    self.deallocate_retired();
                }

                true
            } else {
                let mut head = self.head.load(Ordering::Acquire);

                loop {
                    // SAFETY: ptr is not shared we can safely deref_mut
                    let ptr = ptr.cast::<IntrusiveNode>();
                    unsafe { (*ptr).next.store(head, Ordering::Release) };

                    match self
                        .head
                        .compare_exchange(head, ptr, Ordering::AcqRel, Ordering::Acquire)
                    {
                        Ok(_) => {
                            local.finish();
                            break true;
                        }
                        Err(new_head) => head = new_head,
                    }
                }
            }
        }
    }

    /// Will pop a buffer from the list. If none exist, a buffer
    /// will be allocated from the global allocator.
    pub fn alloc(&self) -> Option<*mut ()> {
        let local = self.hp.local()?;
        let mut head = self.head.load(Ordering::Acquire);
        loop {
            if head.is_null() {
                local.finish();

                // SAFETY: We check ptr is null before deref it
                let node = unsafe { std::alloc::alloc(self.layout).cast::<()>() };

                if node.is_null() {
                    handle_alloc_error(self.layout);
                }

                self.allocated
                    .fetch_add(self.layout.size(), Ordering::Relaxed);

                return Some(node);
            }

            let Some(g) = local.protect(head) else {
                local.finish();
                return None;
            };
            let new_head = self.head.load(Ordering::Acquire);
            if head == new_head {
                // SAFETY: after load-protect-check we can defer head
                let next = unsafe { (*head).next.load(Ordering::Acquire) };
                match self.head.compare_exchange_weak(
                    head,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(head) => {
                        g.unprotect();
                        local.finish();
                        return Some(head.cast::<()>());
                    }
                    Err(new_head) => {
                        g.unprotect();
                        head = new_head;
                    }
                }
            } else {
                g.unprotect();
                head = new_head;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;

    use crate::{allocators::freelist::Freelist, sync::model};

    #[test]
    fn default_and_drop() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let _ = Freelist::new(layout);
        });
    }

    #[test]
    fn alloc_dealloc() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();
            let ptr = s.alloc().unwrap();
            s.dealloc(ptr);
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
        let layout = Layout::from_size_align(8, 8).unwrap();
        let s = Freelist::new(layout).unwrap();

        let p0 = s.alloc().unwrap();
        s.dealloc(p0);

        let p1 = s.alloc().unwrap();
        s.dealloc(p1);

        assert_eq!(p0, p1, "pop must recycle the buffer that was just pushed");
    }

    /// Reproducer for the ABA bug in `alloc`'s pop path.
    ///
    /// `alloc` loads `head`, protects it, reloads `head`, and only if the value
    /// is unchanged does it read `head.next` and `compare_exchange(head -> next)`.
    /// The reload only checks the *value* of `head`, not that the node is the
    /// same node. Because buffers are recycled (popped and pushed back), another
    /// thread can pop `head = A`, pop `A.next = B`, then push `A` back so that
    /// `head` is `A` again — but `A.next` is now different. The victim's CAS
    /// then succeeds (head == A again) and installs a `next` that is no longer
    /// in the freelist, corrupting the list (in the worst case into a self-loop).
    ///
    /// This test is `#[ignore]`d so it never breaks the normal suite. Run it
    /// under loom (which explores the interleaving deterministically) to see it
    /// fail and prove the bug. It fails even with the project's default
    /// `LOOM_MAX_PREEMPTIONS=2`:
    ///
    ///     RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=2 \
    ///         cargo test --lib alloc_aba_corrupts_freelist -- --ignored
    ///
    /// See BUGS.TXT for the full analysis.
    #[test]
    #[ignore]
    fn alloc_aba_corrupts_freelist() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            // Prepopulate the freelist with exactly two buffers so that
            // head = A -> B -> null.
            let a = s.alloc().unwrap(); // fresh A
            let b = s.alloc().unwrap(); // fresh B
            s.dealloc(b); // push B:  head = B
            s.dealloc(a); // push A:  head = A -> B

            // T2: pop A, pop B, push A, push B  (a full recycle of both nodes).
            // T1: a single pop — the ABA victim.
            crate::thread::scope(|scope| {
                scope.spawn(|| {
                    let q1 = s.alloc().unwrap(); // pop A
                    let q2 = s.alloc().unwrap(); // pop B
                    s.dealloc(q1); // push A (head -> A)
                    s.dealloc(q2); // push B
                });
                scope.spawn(|| {
                    let _p = s.alloc().unwrap(); // victim pop
                });
            });

            // Invariant for a correct freelist: every `alloc` returns a buffer
            // that is either a distinct node popped from the list or a freshly
            // allocated one. The same address can never be handed out twice
            // without an intervening `dealloc`. In the ABA schedule the list
            // ends in a self-loop on one node, so repeated `alloc` returns the
            // same pointer over and over.
            let mut seen: Vec<*mut ()> = Vec::new();
            for _ in 0..3 {
                if let Some(p) = s.alloc() {
                    seen.push(p);
                }
            }

            let mut duplicate = false;
            for i in 0..seen.len() {
                for j in (i + 1)..seen.len() {
                    if seen[i] == seen[j] {
                        duplicate = true;
                    }
                }
            }

            // `forget` the freelist *before* asserting: in the ABA schedule the
            // list is a self-loop, so `Drop` would walk it forever (use-after-
            // free on the first deallocated node). Forgetting keeps the failure
            // signal clean.
            std::mem::forget(s);

            assert!(
                !duplicate,
                "freelist handed out the same buffer twice without an intervening \
                 dealloc (ABA self-loop): {seen:?}"
            );
        });
    }

    #[test]
    fn concurrent_stress() {
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
    }
}
