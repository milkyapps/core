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

    /// Push a memory buffer back to the freelist
    pub fn push(&self, ptr: *mut ()) -> bool {
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
                    let ptr = unsafe { &mut *(ptr.cast::<IntrusiveNode>()) };
                    ptr.next.store(head, Ordering::Release);

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

    /// Returns an available memory buffer, or allocates one using
    /// the default allocator.
    pub fn pop(&self) -> Option<*mut ()> {
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
    use std::alloc::{Layout, dealloc};

    use crate::{allocators::freelist::Freelist, sync::model};

    #[test]
    fn default_and_drop() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let _ = Freelist::new(layout);
        });
    }

    #[test]
    fn pop_empty() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();
            let ptr = s.pop().unwrap();

            // We need to dealloc so miri does not complain of a leak
            unsafe { dealloc(ptr.cast::<u8>(), *s.layout()) };
        });
    }

    #[test]
    fn pop_push() {
        model(|| {
            let layout = Layout::from_size_align(8, 8).unwrap();
            let s = Freelist::new(layout).unwrap();
            let ptr = s.pop().unwrap();
            s.push(ptr);
        });
    }
}
