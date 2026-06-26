use crate::sync::AtomicPtr;
use crate::{ptr::TaggedPtr, smr::hazard_ptrs::HazardPointers};
use std::{
    alloc::{Layout, LayoutError, dealloc, handle_alloc_error},
    ptr::null_mut,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Node {
    next: TaggedPtr<Node>,
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
    head: TaggedPtr<Node>,
    layout: Layout,
    offset: usize,
    allocated: AtomicUsize,
    retired: AtomicUsize,
    threshold: usize,
    hp: HazardPointers<Node>,
}

impl Drop for Freelist {
    fn drop(&mut self) {
        // self.deallocate_retired();

        // let mut ptr = self.head.load(Ordering::Acquire);
        // while !ptr.is_null() {
        //     // SAFETY: inside drop we are sure nobody has access to
        //     let current = unsafe { &*ptr };
        //     let next = current.next.load(Ordering::Relaxed);

        //     unsafe { dealloc(ptr.cast::<u8>(), self.layout) };
        //     ptr = next;
        // }
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
            head: TaggedPtr::new(null_mut(), 0),
            layout,
            offset,
            allocated: AtomicUsize::new(0),
            retired: AtomicUsize::new(0),
            threshold: 1024,
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
    pub fn dealloc(&self, ptr: TaggedPtr<()>) -> bool {
        if ptr.is_null() {
            false
        } else {
            let ptr = unsafe { ptr.byte_sub::<Node>(self.offset) };
            let (_, ptr_inner) = ptr.load();

            let local = self.hp.local().unwrap();

            loop {
                let (head, head_inner) = self.head.load();
                let g = local.protect(head).unwrap();
                if head == self.head.load().0 {
                    if head.is_null() {
                        ptr.store(0);
                    } else {
                        // SAFETY: load-protect-check cycle complete. head is safe to be deref here
                        let (_, next) = unsafe { (*head).next.load() };
                        ptr.store(next);
                    }

                    match self.head.compare_exchange_weak(
                        head_inner,
                        ptr_inner,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok((_, _, _)) => {
                            g.unprotect();
                            local.finish();
                            return true;
                        }
                        Err(_) => {}
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
            let (mut head, head_inner) = self.head.load();
            if head.is_null() {
                local.finish();
                // SAFETY: We check ptr is null before deref it
                let node = unsafe { std::alloc::alloc(self.layout).cast::<Node>() };
                if node.is_null() {
                    handle_alloc_error(self.layout);
                }
                self.allocated.fetch_add(1, Ordering::Relaxed);
                let ptr = unsafe { node.byte_add(self.offset).cast::<()>() };
                return Some(TaggedPtr::new(ptr, 0));
            } else {
                let g = local.protect(head).unwrap();
                if head == self.head.load().0 {
                    // SAFETY: head is load-protect-check so it is safe to deref
                    let (_, next_inner) = unsafe { (*head).next.load() };
                    match self.head.compare_exchange_weak(
                        head_inner,
                        next_inner,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok((_, _, new_head_inner)) => {
                            g.unprotect();
                            local.finish();
                            let ptr = TaggedPtr::<()>::from_inner(new_head_inner);
                            ptr.incr_tag();
                            return Some(unsafe { ptr.byte_add(self.offset) });
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

            assert!(s.head.load().0.is_null());
            let buffer = s.alloc().unwrap();
            assert!(s.head.load().0.is_null());
            s.dealloc(buffer);
            assert!(!s.head.load().0.is_null());
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

        assert!(s.head.load().0.is_null());
        let p0 = s.alloc().unwrap();
        let p0_ptr = p0.load().0;

        assert!(s.head.load().0.is_null());
        s.dealloc(p0);
        assert!(!s.head.load().0.is_null());

        let p1 = s.alloc().unwrap();
        let p1_ptr = p1.load().0;
        assert!(s.head.load().0.is_null());

        s.dealloc(p1);
        assert!(!s.head.load().0.is_null());

        assert_eq!(
            p0_ptr, p1_ptr,
            "pop must recycle the buffer that was just pushed"
        );
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
            let mut seen = Vec::new();
            for _ in 0..3 {
                if let Some(p) = s.alloc() {
                    seen.push(p);
                }
            }

            let mut duplicate = false;
            for i in 0..seen.len() {
                for j in (i + 1)..seen.len() {
                    if seen[i].load().0 == seen[j].load().0 {
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
