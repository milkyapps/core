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

/// A lock-free pool of fixed-size memory buffers.
///
/// Buffers returned via [`Freelist::dealloc`] are recycled and handed back out
/// by [`Freelist::alloc`], avoiding repeated calls to the global allocator.
/// When the pool holds more buffers than it needs, the excess is released back
/// to the global allocator automatically.
///
/// The pool caps the total number of buffers it will ever own; once that cap is
/// reached [`Freelist::alloc`] returns `None` until buffers are returned.
///
/// A `Freelist` is `Send` and `Sync` and is safe to share across threads. All
/// buffers still held by the pool are released when it is dropped.
pub struct Freelist {
    head: AtomicTagged<Node>,
    layout: Layout,
    offset: usize,
    qty_allocated: AtomicUsize,
    qty_in_list: AtomicUsize,
    qty_retired: AtomicUsize,
    /// It is possible that under contention more allocations than `alloc_max` happens.
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
    /// Creates a pool that recycles buffers of `layout`.
    ///
    /// Every buffer handed out by [`Freelist::alloc`] from this pool is at
    /// least as large and well-aligned as `layout`.
    ///
    /// # Errors
    ///
    /// Returns a [`LayoutError`] if `layout` cannot be combined with the
    /// pool's own bookkeeping layout.
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

    /// Returns the buffer [`Layout`] this pool was created with.
    ///
    /// Every buffer obtained from [`Freelist::alloc`] satisfies this layout.
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

    /// Returns `buffer` to the pool for later reuse.
    ///
    /// `buffer` must come from this pool's [`Freelist::alloc`] and must not
    /// already have been returned. A null pointer is a no-op.
    ///
    /// Returns `true` if the buffer was accepted back, or `false` if `buffer`
    /// was null.
    ///
    /// If the pool already holds as many buffers as it wants, the returned
    /// buffer (and possibly others it has been holding) is released back to the
    /// global allocator.
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

    /// Obtains a buffer of this pool's [`Layout`].
    ///
    /// A previously returned buffer is reused when one is available; otherwise
    /// a new buffer is allocated from the global allocator.
    ///
    /// Returns `None` when the pool already owns its maximum number of buffers
    /// and none are currently available for reuse — return some buffers via
    /// [`Freelist::dealloc`] and retry.
    ///
    /// The returned pointer is never null and satisfies the pool's layout. It
    /// is on loan from the pool: keep it alive until you return it with
    /// [`Freelist::dealloc`].
    pub fn alloc(&self) -> Option<TaggedPtr<()>> {
        let local = self.hp.local()?;
        loop {
            let head = self.head.load(Ordering::Acquire);
            if head.ptr().is_null() {
                if self.qty_allocated.load(Ordering::Relaxed) >= self.alloc_max {
                    let mut nodes = vec![];
                    self.hp.reclaim(&mut nodes);
                    if nodes.is_empty() {
                        local.finish();
                        return None;
                    }
                    for node in nodes {
                        let ptr = unsafe { node.byte_add(self.offset).cast::<()>() };
                        self.dealloc(TaggedPtr::new(ptr, 0));
                    }
                    continue;
                }

                local.finish();

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
    use std::{alloc::Layout, ptr::null_mut, sync::atomic::Ordering};

    use crate::{
        allocators::freelist::Freelist,
        ptr::TaggedPtr,
        sync::{
            atomic::{AtomicPtr, AtomicUsize},
            model,
        },
    };

    /// A freshly created pool must construct and drop without leaking or
    /// panicking.
    #[test]
    fn default_and_drop() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let _ = Freelist::new(layout);
        });
    }

    /// `alloc` and `dealloc` round-trip a few buffers; the pool must stay
    /// usable across repeated cycles (this is the smoke test for the basic
    /// alloc/dealloc path).
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

    /// `Freelist` claims `Send + Sync`; this is a compile-time assertion of
    /// that contract (sharing a pool across threads must be legal).
    #[test]
    fn freelist_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<Freelist>();
        assert_sync::<Freelist>();
    }

    /// After `dealloc(p)` followed by `alloc()`, the pool must hand back the
    /// same buffer that was just returned — i.e. the pool recycles rather than
    /// allocating fresh memory each time.
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

    /// Regression test for the ABA hazard on the lock-free stack.
    ///
    /// Two threads race on a pre-populated list (`head = A -> B -> null`): one
    /// pops both buffers and pushes them back while another pops one. A
    /// correct, ABA-safe pop must never hand the same address out twice without
    /// an intervening `dealloc`. If the ABA tag fails to prevent a stale-head
    /// CAS, the list degenerates into a self-loop and successive `alloc`s
    /// return the same pointer — which this test detects.
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

    /// Many threads hammer `alloc`/`dealloc` in tight loops. The test passes as
    /// long as no thread panics and the pool remains internally consistent
    /// (no crash, no deadlock) under contention.
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
        });
    }

    /// `alloc` must stop returning new buffers once the pool's capacity is
    /// reached: requesting one more than the cap yields exactly `cap`
    /// successful allocations and no more.
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
        });
    }

    /// With small capacity knobs, returning more buffers than the pool wants to
    /// keep must first spill into the holding area and then, once that area is
    /// full, actually release buffers back to the global allocator. This
    /// verifies the bookkeeping counters move through the expected phases
    /// (in-pool → held → freed) as buffers are returned.
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
        });
    }

    /// `dealloc` of a null pointer is a no-op and must return `false`.
    #[test]
    fn dealloc_null_returns_false() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();
            let null = TaggedPtr::new(null_mut::<()>(), 0);
            assert!(!s.dealloc(null), "dealloc of null must return false");
        });
    }

    /// `alloc_max` is advertised as the total number of buffers the pool will
    /// ever own. But the freelist can exceed a little bit because of high contention
    /// this is cheaper than try to always stay inside the limit.
    #[cfg(not(loom))] // this test is taking too long on loom
    #[test]
    fn alloc_max_counter_can_exceed_cap() {
        model(|| {
            #[cfg(not(loom))]
            let params = (8, 8, 10);

            #[cfg(loom)]
            let params = (1, 1, 2);

            let layout = Layout::from_size_align(8, 8).unwrap();
            let mut s = Freelist::new(layout).unwrap();
            s.alloc_max = params.0;
            s.list_max = params.1;

            let ceiling = params.2;

            let barrier = crate::sync::Barrier::new(2);
            let taken = crate::sync::atomic::AtomicUsize::new(0);

            crate::thread::scope(|scope| {
                let s = &s;
                let barrier = &barrier;
                let taken = &taken;
                for _ in 0..2 {
                    scope.spawn(move || {
                        barrier.wait();
                        for _ in 0..s.alloc_max {
                            if s.alloc().is_some() {
                                taken.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    });
                }
            });

            let qty = s.qty_allocated.load(Ordering::Relaxed);
            let total = taken.load(Ordering::SeqCst);
            assert!(
                qty <= ceiling,
                "qty_allocated ({qty}) can exceed alloc_max ({ceiling}) in some cases; But not too much.",
            );
            assert!(
                total <= ceiling,
                "successful allocations ({total}) can exceed alloc_max ({ceiling}). But not too much.",
            );
        });
    }

    /// When `alloc_max` is reached the allocator stops creating new buffers,
    /// but it also stops reclaiming retired buffers. A retired buffer is still
    /// part of `qty_allocated` and could be reused; returning `None` instead of
    /// reclaiming it makes the pool starve even though memory is available.
    #[test]
    fn alloc_starves_despite_retired_buffers() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let mut s = Freelist::new(layout).unwrap();
            s.alloc_max = 2;
            s.list_max = 1;
            s.retired_max = 1;

            let a = s.alloc().unwrap();
            let b = s.alloc().unwrap();

            // First return fills the free list.
            assert!(s.dealloc(a));
            // Second return overflows the list and moves the node to the
            // retirement list (retired == retired_max, so reclaim is not
            // triggered yet).
            assert!(s.dealloc(b));

            // Take the one node that is still in the free list.
            let _ = s.alloc().unwrap();

            // At this point `qty_allocated == alloc_max`, but one retired
            // buffer is still available. A well-behaved pool would reclaim it
            // and hand it out; the current implementation returns `None`.
            let next = s.alloc();
            assert!(
                next.is_some(),
                "alloc() starved even though a retired buffer was available for reclaim"
            );
        });
    }

    /// Two threads allocating concurrently (without deallocating in between)
    /// must never receive the same address — that would mean a node was handed
    /// out twice, the classic freelist ABA / list-corruption failure.
    #[test]
    fn concurrent_alloc_returns_distinct_addresses() {
        model(|| {
            const THREADS: usize = 2;
            const ITERS: usize = 4;
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            // Pre-sized slot array: thread `t` writes the address of its i-th
            // allocation into `slots[t * ITERS + i]`. Uses loom-aware atomics,
            // and stores real pointers (not integers) so provenance is
            // preserved for the cleanup `dealloc`.
            let slots: Vec<AtomicPtr<()>> = (0..THREADS * ITERS)
                .map(|_| AtomicPtr::new(null_mut()))
                .collect();

            crate::thread::scope(|scope| {
                let s = &s;
                let slots = &slots;
                for t in 0..THREADS {
                    scope.spawn(move || {
                        let base = t * ITERS;
                        for i in 0..ITERS {
                            if let Some(p) = s.alloc() {
                                slots[base + i].store(p.ptr(), Ordering::SeqCst);
                            }
                        }
                    });
                }
            });

            let addrs: Vec<*mut ()> = slots
                .iter()
                .map(|slot| slot.load(Ordering::SeqCst))
                .filter(|a| !a.is_null())
                .collect();
            // Dedup by address to detect the same node being handed out twice.
            let mut as_int: Vec<usize> = addrs.iter().map(|p| *p as usize).collect();
            as_int.sort_unstable();
            let before = as_int.len();
            as_int.dedup();
            assert_eq!(
                as_int.len(),
                before,
                "two concurrent allocs returned the same address (ABA / list corruption)"
            );

            // Return every buffer we hold so the freelist drops cleanly.
            for p in addrs {
                let _ = s.dealloc(TaggedPtr::new(p, 0));
            }
        });
    }

    /// After a balanced concurrent alloc/dealloc burst, no buffer may be lost:
    /// every allocated buffer is either in the list or retired, none held.
    /// So `qty_allocated == qty_in_list + qty_retired`.
    #[test]
    fn no_buffer_loss_under_contention() {
        model(|| {
            const THREADS: usize = 2;
            const ROUNDS: usize = 4;
            const PER_ROUND: usize = 4;
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            crate::thread::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|| {
                        for _ in 0..ROUNDS {
                            let mut held = Vec::with_capacity(PER_ROUND);
                            for _ in 0..PER_ROUND {
                                if let Some(p) = s.alloc() {
                                    held.push(p);
                                }
                            }
                            for p in held {
                                let _ = s.dealloc(p);
                            }
                        }
                    });
                }
            });

            let allocated = s.qty_allocated.load(Ordering::Relaxed);
            let in_list = s.qty_in_list.load(Ordering::Relaxed);
            let retired = s.qty_retired.load(Ordering::Relaxed);
            assert!(
                allocated >= in_list + retired,
                "qty_allocated must not underflow in_list + retired"
            );
            assert_eq!(
                allocated,
                in_list + retired,
                "allocated buffers must be accounted for (in_list + retired); \
                 a mismatch means a buffer was lost or double-counted"
            );
        });
    }

    /// Same accounting invariant, but with thresholds tuned so the retire +
    /// reclaim path is actually exercised under contention.
    #[test]
    fn concurrent_dealloc_triggers_reclaim_safely() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let mut s = Freelist::new(layout).unwrap();
            s.list_max = 2;
            s.retired_max = 1;
            s.alloc_max = 64;

            crate::thread::scope(|scope| {
                for _ in 0..2 {
                    scope.spawn(|| {
                        let mut held = Vec::new();
                        for _ in 0..8 {
                            if let Some(p) = s.alloc() {
                                held.push(p);
                            }
                        }
                        for p in held {
                            let _ = s.dealloc(p);
                        }
                    });
                }
            });

            let allocated = s.qty_allocated.load(Ordering::Relaxed);
            let in_list = s.qty_in_list.load(Ordering::Relaxed);
            let retired = s.qty_retired.load(Ordering::Relaxed);
            assert_eq!(
                allocated,
                in_list + retired,
                "accounting invariant must hold even while retiring/reclaiming concurrently"
            );
        });
    }

    /// After a contended alloc/dealloc stress, the list is quiescent (all
    /// threads joined). Walking it must show:
    ///   * no node whose `next` points to itself — the signature of an ABA
    ///     self-loop,
    ///   * a node count that exactly matches `qty_in_list`.
    #[test]
    fn list_length_matches_counter_after_stress() {
        model(|| {
            const THREADS: usize = 2;
            const ITERS: usize = 8;
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
                            if held.len() > 4
                                && let Some(p) = held.pop()
                            {
                                let _ = s.dealloc(p);
                            }
                        }
                        for p in held {
                            let _ = s.dealloc(p);
                        }
                    });
                }
            });

            // The list is now quiescent; walking it is safe.
            let mut count = 0usize;
            let mut cur = s.head.load(Ordering::Acquire);
            let mut self_loop = false;
            while !cur.ptr().is_null() {
                // SAFETY: no other thread is running (scope joined), so the
                // node is stable.
                let next = unsafe { (*cur.ptr()).next.load(Ordering::Acquire) };
                if next.ptr() == cur.ptr() {
                    self_loop = true;
                    break;
                }
                count += 1;
                if count > 10_000 {
                    break;
                }
                cur = next;
            }

            assert!(!self_loop, "freelist contains a self-loop (ABA corruption)");
            assert_eq!(
                count,
                s.qty_in_list.load(Ordering::Relaxed),
                "actual list length must match qty_in_list counter"
            );
        });
    }

    /// `alloc_max` must be respected even when many threads race to allocate.
    #[cfg(not(loom))] // too slow on loom
    #[test]
    fn alloc_max_respected_under_contention() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let mut s = Freelist::new(layout).unwrap();
            s.alloc_max = 8;
            s.list_max = 8;

            let ceiling = s.alloc_max * 2;

            let taken = AtomicUsize::new(0);

            crate::thread::scope(|scope| {
                let s = &s;
                let taken = &taken;
                for _ in 0..2 {
                    scope.spawn(move || {
                        for _ in 0..s.alloc_max {
                            if s.alloc().is_some() {
                                taken.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    });
                }
            });

            let total = taken.load(Ordering::SeqCst);
            assert!(
                total <= ceiling,
                "allocated {total} buffers but alloc_max is {ceiling}",
            );
            assert!(
                s.qty_allocated.load(Ordering::Relaxed) <= ceiling,
                "exactly alloc_max buffers may be freshly allocated"
            );
        });
    }

    /// Draining the list completely and then allocating concurrently must hand
    /// out fresh, distinct buffers (no recycling of a node still held).
    #[test]
    fn alloc_after_drain_under_contention() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            // Seed the list with two recycled buffers.
            let a = s.alloc().unwrap();
            let b = s.alloc().unwrap();
            let _ = s.dealloc(b);
            let _ = s.dealloc(a);
            assert!(!s.head.load(Ordering::Acquire).ptr().is_null());

            // Drain it from two threads; both must succeed and get distinct
            // addresses (no node handed out twice).
            let got = vec![
                AtomicPtr::new(null_mut::<()>()),
                AtomicPtr::new(null_mut::<()>()),
            ];
            crate::thread::scope(|scope| {
                let s = &s;
                for slot in &got {
                    scope.spawn(move || {
                        if let Some(p) = s.alloc() {
                            slot.store(p.ptr(), Ordering::SeqCst);
                        }
                    });
                }
            });

            let p0 = got[0].load(Ordering::SeqCst);
            let p1 = got[1].load(Ordering::SeqCst);
            assert!(!p0.is_null(), "thread 0 got nothing");
            assert!(!p1.is_null(), "thread 1 got nothing");
            assert_ne!(
                p0 as usize, p1 as usize,
                "two draining allocs returned the same node (ABA / lost update)"
            );

            // Clean up.
            let _ = s.dealloc(TaggedPtr::new(p0, 0));
            let _ = s.dealloc(TaggedPtr::new(p1, 0));
        });
    }

    /// Dropping a freelist that still has retired-but-not-reclaimed buffers
    /// must not crash or leak (miri checks the dealloc pairing).
    #[test]
    fn drop_with_retired_buffers_is_sound() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let mut s = Freelist::new(layout).unwrap();
            s.list_max = 1;
            s.retired_max = 4;

            // Force some buffers into the retired list without triggering
            // reclaim (stay at or below retired_max).
            let mut held = Vec::new();
            for _ in 0..4 {
                held.push(s.alloc().unwrap());
            }
            for p in held {
                let _ = s.dealloc(p);
            }
            // `drop` runs at the end of this closure and must deallocate both
            // the in-list nodes and the retired ones.
        });
    }

    /// Repeated alloc/dealloc recycling must not grow `qty_allocated`: the
    /// same buffer is reused, so no fresh allocation should happen after the
    /// first one. This catches a CAS-lost-update that drops a node from the
    /// list (forcing a fresh alloc next time).
    #[test]
    fn recycling_does_not_grow_allocated() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            let first = s.alloc().unwrap();
            let _ = s.dealloc(first);
            let after_first = s.qty_allocated.load(Ordering::Relaxed);
            assert_eq!(after_first, 1, "one buffer freshly allocated");

            for _ in 0..32 {
                let p = s.alloc().unwrap();
                let _ = s.dealloc(p);
            }
            assert_eq!(
                s.qty_allocated.load(Ordering::Relaxed),
                after_first,
                "recycling must not allocate new buffers (no node was lost from the list)"
            );
        });
    }

    /// A freshly allocated buffer can be returned and then reallocated many
    /// times without the pool losing track of it. The address must remain
    /// stable across cycles (no node is silently dropped from the list).
    #[test]
    fn recycling_returns_same_buffer_repeatedly() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            let first = s.alloc().unwrap();
            let first_ptr = first.ptr();
            s.dealloc(first);

            for _ in 0..16 {
                let p = s.alloc().unwrap();
                assert_eq!(
                    p.ptr(),
                    first_ptr,
                    "recycling must return the same buffer, not allocate fresh memory"
                );
                s.dealloc(p);
            }
        });
    }

    /// Returning a buffer that was allocated when the list already held nodes
    /// must still preserve the LIFO order: the most recently returned node is
    /// at the head of the list.
    #[test]
    fn dealloc_maintains_lifo_order() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();

            let a = s.alloc().unwrap();
            let b = s.alloc().unwrap();
            let c = s.alloc().unwrap();

            s.dealloc(a);
            s.dealloc(b);
            s.dealloc(c);

            // LIFO: c should be popped first, then b, then a.
            let p1 = s.alloc().unwrap();
            let p2 = s.alloc().unwrap();
            let p3 = s.alloc().unwrap();
            assert_eq!(p1.ptr(), c.ptr());
            assert_eq!(p2.ptr(), b.ptr());
            assert_eq!(p3.ptr(), a.ptr());

            s.dealloc(p1);
            s.dealloc(p2);
            s.dealloc(p3);
        });
    }

    /// A full u16 tag wrap requires 65k+ cycles — too heavy for loom's
    /// permutation model and for miri's interpreter, so this runs only under
    /// the plain std test runner. It verifies that wrapping the ABA tag never
    /// corrupts the list.
    #[cfg(all(not(loom), not(miri)))]
    #[test]
    fn freelist_tag_wrap_does_not_corrupt() {
        let layout = Layout::from_size_align(8, 8).unwrap();
        let s = Freelist::new(layout).unwrap();

        // Force the per-node tag to wrap past u16::MAX several times.
        let cycles = 3 * (u16::MAX as usize);
        for _ in 0..cycles {
            let p = s.alloc().unwrap();
            let _ = s.dealloc(p);
        }

        let mut count = 0usize;
        let mut cur = s.head.load(Ordering::Acquire);
        let mut self_loop = false;
        while !cur.ptr().is_null() {
            let next = unsafe { (*cur.ptr()).next.load(Ordering::Acquire) };
            if next.ptr() == cur.ptr() {
                self_loop = true;
                break;
            }
            count += 1;
            if count > 10_000 {
                break;
            }
            cur = next;
        }
        assert!(!self_loop, "tag wrap produced a self-loop (ABA)");
        assert_eq!(
            count,
            s.qty_in_list.load(Ordering::Relaxed),
            "list length matches counter after tag wrap"
        );
    }
}
