//! Hazard pointers — safe memory reclamation for lock-free data structures.
//!
//! # What it is
//!
//! Hazard pointers is a technique for *safe memory reclamation* in
//! concurrent, lock-free data structures. The core idea, introduced by Maged
//! M. Michael, is described in the seminal paper:
//!
//! > Maged M. Michael, *"Hazard Pointers: Safe Memory Reclamation for
//! > Lock-Free Objects,"* IEEE Transactions on Parallel and Distributed
//! > Systems, vol. 15, no. 8, pp. 491–504, August 2004.
//!
//! # Rationale
//!
//! Lock-free algorithms unlink objects from a structure (e.g. a node removed
//! from a stack) before they are sure that no other thread is still reading
//! them. Naively freeing the memory immediately is unsound as another thread may
//! be using the pointed memory, leading to a use-after-free.
//!
//! Hazard pointers solves this by "protecting"
//! and "retiring" pointers, instead
//! of immediately releasing them. In practice, this means that retired pointers
//! go to a list and are only released when they are not protected anymore.
//!
//! For that to happen, the `reclaim` function must be actively
//! called. This function will return all pointers that are safe to be released,
//! leaving the caller to decide how to do this for each pointer.
//!
//! # Cannot protect a retired pointer
//!
//! A pointer can ONLY be retired if it is guaranteed that it is no longer reacheable by
//! any other thread. Which means that `protect` should not be called
//! after `retire`.
//!
//! The breaking of this invariant means that a call to `reclaim` will
//! return a pointer that can potentially be protected after being returned.
//!
//! # Example
//!
//! A single thread publishes a pointer into a hazard slot, dereferences it
//! safely, then releases the slot:
//!
//! ```
//! use milkyapps_core::hazard_ptrs::HazardPointers;
//!
//! // A registry with 8 hazard slots, cheaply shareable via `Clone`.
//! let hp = HazardPointers::<u64>::with_capacity(8, 8);
//! let local = hp.local().unwrap();
//!
//! let mut value = Box::new(42u64);
//! let ptr = value.as_mut() as *mut u64;
//!
//! // Publish `ptr` so it cannot be reclaimed while we hold the guard.
//! let guard = local.protect(ptr).unwrap();
//! // ... dereference `ptr` here; it is guaranteed not to be freed ...
//!
//! // Release the slot; `ptr` becomes eligible for reclamation again.
//! guard.unprotect();
//! local.finish();
//! ```
//!

use crate::sync::{
    Arc, UnsafeCell,
    atomic::{AtomicBool, AtomicPtr, Ordering},
};
use std::{panic, ptr::null_mut};

/// A node in the singly-linked retirement list.
#[derive(Debug)]
struct RetireNode<T> {
    /// Pointer to the next node in the retirement list.
    next: AtomicPtr<RetireNode<T>>,
    /// The retired pointer awaiting reclamation.
    ptr: *mut T,
}

/// A guard guarding a protected pointer.
///
/// Dropping a guard without first consuming it via
/// [`Guard::unprotect`] or [`Guard::retire`] is a programmer error:
/// in debug builds the [`Drop`] implementation panics
/// (a "drop bomb") to catch leaked protections early.
pub struct Guard<'a, T> {
    /// Weak pointer to the Registry
    local: &'a Local<'a, T>,
    /// Slot id guarded by this guard
    id: usize,
    /// The protected pointer, or null once the guard has been defused.
    ptr: *mut T,
}

impl<T> Drop for Guard<'_, T> {
    /// Asserts that the guard was properly disposed of.
    ///
    /// In debug builds this panics if the guard was dropped without a call to [`Guard::unprotect`] or [`Guard::retire`].
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // On debug we will panic, but we must unprotect first in case
            // the panic is caught, so the slot is not leaked.
            self.local.unprotect_with_id(self.id, self.ptr);

            // Clippy suggestion here is to difficult to read
            #[allow(clippy::manual_assert)]
            if cfg!(debug_assertions) && !std::thread::panicking() {
                panic!("Guard dropped without calling `unprotect` or `retire` (drop_bomb)");
            }
        }
    }
}

impl<T> Guard<'_, T> {
    /// Consume [`Guard`] unprotecting its pointer.
    pub fn unprotect(mut self) {
        self.local.unprotect_with_id(self.id, self.ptr);
        self.defuse();
    }

    /// Consume [`Guard`] retiring its pointer.
    ///
    /// A pointer can ONLY be retired if it is guaranteed that it is no longer reacheable by
    /// any other thread. Which means that [`Local::protect`] should not be called
    /// after [`Guard::retire`].
    pub fn retire(self) {
        self.local.push_retire_head(self.ptr);
        self.unprotect();
    }

    fn defuse(&mut self) {
        self.ptr = null_mut();
    }
}

/// Give access to protecting pointers. `Local` must be consumed
/// by [`Local::finish`] to make it clear when all pointers are
/// unprotected.
pub struct Local<'a, T> {
    drop_bomb: bool,
    hp: &'a HazardPointers<T>,
    id: usize,
}

impl<T> Drop for Local<'_, T> {
    fn drop(&mut self) {
        if self.drop_bomb {
            // All ptrs need to be unprotected, because panic can be catched.
            self.finish_by_ref();

            // Clippy suggestion here is to difficult to read
            #[allow(clippy::manual_assert)]
            if !std::thread::panicking() {
                panic!("Local must be consumed by finish method.");
            }
        }
    }
}

impl<T> Local<'_, T> {
    fn finish_by_ref(&mut self) {
        let inner = unsafe { &*self.hp.inner.get() };

        for slot in &inner.locals[self.id].slots {
            slot.store(null_mut(), Ordering::Release);
        }

        if inner.is_available[self.id]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            panic!("Local was already finished");
        }

        self.drop_bomb = false;
    }

    /// Consumes `Local` and explicit unprotected all pointers as a fallback.
    /// But the correct approach is to unprotect each pointer individually.
    pub fn finish(mut self) {
        self.finish_by_ref();
    }

    /// Protects `ptr` whilst its [`Guard`] is alive.
    pub fn protect(&self, ptr: *mut T) -> Option<Guard<'_, T>> {
        if ptr.is_null() {
            return None;
        }

        let inner = unsafe { &*self.hp.inner.get() };
        let local = &inner.locals[self.id];

        for id in 0..local.slots.len() {
            if local.slots[id]
                .compare_exchange(null_mut(), ptr, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(Guard {
                    local: self,
                    id,
                    ptr,
                });
            }
        }

        None
    }

    fn unprotect_with_id(&self, id: usize, ptr: *mut T) {
        let inner = unsafe { &*self.hp.inner.get() };
        let local = &inner.locals[self.id];

        if local.slots[id]
            .compare_exchange(ptr, null_mut(), Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            panic!("This guard is not protecting ptr")
        }
    }

    /// Allocates a [`RetireNode`] for `ptr` and atomically pushes it onto the
    /// front of the retirement list.
    ///
    /// Returns a pointer to the newly inserted node.
    fn push_retire_head(&self, ptr: *mut T) -> *mut RetireNode<T> {
        let new = Box::leak(Box::new(RetireNode {
            ptr,
            next: AtomicPtr::new(null_mut()),
        }));

        let inner = unsafe { &*self.hp.inner.get() };
        inner.locals[self.id].push_retire_node(new)
    }

    #[cfg(test)]
    fn get_slot(&self, id: usize) -> Option<*mut T> {
        let inner = unsafe { &*self.hp.inner.get() };
        let local = &inner.locals[self.id];

        local.slots.get(id).map(|x| x.load(Ordering::SeqCst))
    }

    #[cfg(test)]
    fn retire_head(&self) -> *mut RetireNode<T> {
        let inner = unsafe { &*self.hp.inner.get() };
        let local = &inner.locals[self.id];

        local.retire_head.load(Ordering::SeqCst)
    }
}

struct HazardPointersLocal<T> {
    slots: Vec<AtomicPtr<T>>,
    retire_head: AtomicPtr<RetireNode<T>>,
}

impl<T> HazardPointersLocal<T> {
    #[cfg(test)]
    fn get_slot(&self, id: usize) -> Option<*mut T> {
        self.slots.get(id).map(|x| x.load(Ordering::SeqCst))
    }

    fn push_retire_node(&self, new: &mut RetireNode<T>) -> *mut RetireNode<T> {
        let mut current = self.retire_head.load(Ordering::Acquire);
        loop {
            new.next.store(current, Ordering::SeqCst);
            match self.retire_head.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break new,
                Err(new_head) => current = new_head,
            }
        }
    }
}

struct HazardPointersInner<T> {
    is_available: Vec<AtomicBool>,
    locals: Vec<HazardPointersLocal<T>>,
}

/// Hazard pointers is a technique for *safe memory reclamation* in
/// concurrent, lock-free data structures.
#[derive(Clone)]
pub struct HazardPointers<T> {
    inner: Arc<UnsafeCell<HazardPointersInner<T>>>,
}

/// SAFETY: `HazardPointers` never dereference `*mut T`, but `reclaim`
/// returns these pointers to any thread and its caller will `deref` or drop `T`,
/// which means that `T` must be `Send`.
unsafe impl<T: Send> Sync for HazardPointers<T> {}

/// SAFETY: `HazardPointers` never dereference `*mut T`, but `reclaim`
/// returns these pointers to any thread and its caller will `deref` or drop `T`,
/// which means that `T` must be `Send`.
unsafe impl<T: Send> Send for HazardPointers<T> {}

impl<T> HazardPointers<T> {
    /// Creates `locals` slots for threads. Each having `ptrs` slots for pointers
    /// to be protected.
    #[must_use]
    pub fn with_capacity(locals: usize, ptrs: usize) -> HazardPointers<T> {
        HazardPointers {
            inner: Arc::new(UnsafeCell::new(HazardPointersInner {
                is_available: (0..locals).map(|_| AtomicBool::new(true)).collect(),
                locals: {
                    let mut v = Vec::with_capacity(locals);
                    for _ in 0..locals {
                        v.push(HazardPointersLocal {
                            slots: (0..ptrs).map(|_| AtomicPtr::new(null_mut())).collect(),
                            retire_head: AtomicPtr::new(null_mut()),
                        });
                    }
                    v
                },
            })),
        }
    }

    /// Use only in debug.
    ///
    /// Walks the retirement list from `head` and dumps every node with `dbg!`.
    /// This is intended only for ad-hoc debugging: it dereferences raw pointers
    /// without any synchronization guarantees.
    ///
    /// # Safety
    ///
    /// Iterate the data structures without any lock. Caller must guarantee nothing is running whilst
    /// this is called.
    #[allow(unused)]
    unsafe fn debug_retire_list(head: &AtomicPtr<RetireNode<T>>)
    where
        T: std::fmt::Debug,
    {
        let mut nodes = vec![];

        let mut current = head.load(Ordering::SeqCst);
        while !current.is_null() {
            let node = unsafe { &mut *current };
            current = node.next.load(Ordering::SeqCst);
            nodes.push(node);
        }

        dbg!(nodes);
    }

    /// Give access to protecting pointers. `Local` must be consumed
    /// by [`Local::finish`] to make it clear when all pointers are
    /// unprotected.
    #[must_use]
    pub fn local(&self) -> Option<Local<'_, T>> {
        let inner = unsafe { &*self.inner.get() };

        for id in 0..inner.locals.len() {
            if inner.is_available[id]
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(Local {
                    drop_bomb: true,
                    hp: self,
                    id,
                });
            }
        }

        None
    }

    fn is_protected(ptr: *mut T, inner: &HazardPointersInner<T>) -> bool {
        if ptr.is_null() {
            return false;
        }

        for local in &inner.locals {
            for slot in &local.slots {
                let slot_ptr = slot.load(Ordering::Acquire);
                if slot_ptr == ptr {
                    return true;
                }
            }
        }

        false
    }

    /// Push into `v` all retired pointer which is not being protected.
    /// Caller must decide what to do with the returned pointers.
    ///
    /// `v` will be sorted and deduped, so ideally it should be empty.
    /// To avoid allocations, one can also resuse the same `Vec` multiple times.
    pub fn reclaim(&self, reclaimed: &mut Vec<*mut T>) {
        let inner = unsafe { &*self.inner.get() };
        for l in &inner.locals {
            let mut head = l.retire_head.swap(null_mut(), Ordering::Acquire);
            while !head.is_null() {
                // SAFETY: This deref is safe because nodes are only free'd below
                let node = unsafe { &mut *head };

                if Self::is_protected(node.ptr, inner) {
                    head = node.next.load(Ordering::Acquire);
                    node.next.store(null_mut(), Ordering::Release);
                    l.push_retire_node(node);
                } else {
                    // SAFETY: This thread can take ownership because it is the only owner
                    // of this raw pointer.
                    let node = unsafe { Box::from_raw(head) };
                    reclaimed.push(node.ptr);
                    head = node.next.load(Ordering::Acquire);
                }
            }
        }

        reclaimed.sort();
        reclaimed.dedup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sync::{Barrier, atomic::AtomicUsize, model},
        thread::ThreadSafePtr,
    };

    #[test]
    fn protect_unprotect_must_use_slots() {
        model(|| {
            let ptr = &mut 2u64 as *mut u64;

            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            assert!(
                local.get_slot(0).unwrap().is_null(),
                "Slot should not be protecting any pointer"
            );
            let g = local.protect(ptr).unwrap();
            assert_eq!(
                local.get_slot(0).unwrap(),
                ptr,
                "Slow should be protecting ptr"
            );
            g.unprotect();
            assert!(
                local.get_slot(0).unwrap().is_null(),
                "Slot should not be protecting ptr anymore"
            );

            local.finish();
        });
    }

    #[test]
    fn more_protects_than_slots() {
        model(|| {
            let ptr = &mut 42u64 as *mut u64;

            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();
            let mut guards = vec![];

            for _ in 0..9 {
                guards.push(local.protect(ptr));
            }

            let some_qty = guards.iter().filter(|x| x.is_some()).count();
            let none_qty = guards.iter().filter(|x| x.is_none()).count();

            assert_eq!(some_qty, 8);
            assert_eq!(none_qty, 1);

            for g in guards.into_iter().flatten() {
                g.unprotect();
            }

            local.finish();
        });
    }

    #[test]
    fn reclaim_empty() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            // Should not panic or crash
            assert!(local.retire_head().is_null(), "Retire list should be empty");

            let mut v = Vec::new();
            hp.reclaim(&mut v);

            assert!(v.is_empty(), "Reclaim Vec should be empty");
            assert!(local.retire_head().is_null(), "Retire list should be empty");

            local.finish();
        });
    }

    #[test]
    fn single_protect_retire_and_reclaim() {
        model(|| {
            let ptr = &mut 42u64 as *mut u64;

            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            let g = local.protect(ptr).unwrap();
            g.retire();

            let mut v = Vec::with_capacity(16);
            hp.reclaim(&mut v);

            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr, "Reclaimed pointer should be the retired one");

            local.finish();
        });
    }

    #[test]
    fn protect_prevents_reclaim() {
        model(|| {
            let ptr = &mut 42u64 as *mut u64;

            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local1 = hp.local().unwrap();
            let local2 = hp.local().unwrap();

            // Simulate two threads that are protecting the same ptr
            let g11 = local1.protect(ptr).unwrap();

            let g12 = local2.protect(ptr).unwrap();
            g12.retire();

            assert!(
                local1.retire_head().is_null(),
                "Retire List should be empty"
            );
            assert!(
                !local2.retire_head().is_null(),
                "Retire List should not be empty"
            );

            // Because 'ptr' is still protected, it should NOT be reclaimed
            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert!(
                v.is_empty(),
                "ptr is still protected and should not be reclaimed"
            );

            // Now ptr is no long protected and should be reclaimed
            g11.unprotect();

            hp.reclaim(&mut v);
            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr, "Reclaimed pointer should be the retired one");

            assert!(
                local1.retire_head().is_null(),
                "Retire List should be empty"
            );
            assert!(
                local2.retire_head().is_null(),
                "Retire List should be empty"
            );

            local1.finish();
            local2.finish();
        });
    }

    #[test]
    fn multiple_retirements() {
        model(|| {
            let ptr1 = &mut 42u64 as *mut u64;
            let ptr2 = &mut 42u64 as *mut u64;
            let ptr3 = &mut 42u64 as *mut u64;

            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            let local1 = hp.local().unwrap();
            let local2 = hp.local().unwrap();
            let local3 = hp.local().unwrap();

            // Simulate three retirements without active protections
            local1.protect(ptr1).unwrap().retire();
            local2.protect(ptr2).unwrap().retire();
            local3.protect(ptr3).unwrap().retire();

            local1.finish();
            local2.finish();
            local3.finish();

            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert_eq!(v.len(), 3, "All retired pointer should have been reclaimed");
        });
    }

    #[test]
    fn partial_reclaim() {
        model(|| {
            let ptr1 = &mut 42u64 as *mut u64;
            let ptr2 = &mut 42u64 as *mut u64;

            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            // Protect p1, leave p2 unprotected
            // Put both into the retired list
            let g11 = local.protect(ptr1).unwrap();
            let g12 = local.protect(ptr1).unwrap();
            g12.retire();
            let g2 = local.protect(ptr2).unwrap();
            g2.retire();

            // Only ptr2 should be reclaimed because ptr1 is still in Hazard Array
            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr2, "Only ptr2 is not protected");

            g11.retire();
            local.finish();

            let mut v = vec![];
            hp.reclaim(&mut v);
        });
    }

    #[test]
    fn protect_multi_thread() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            let qty_threads = 2; //TODO Increate to 12
            let protect_barrier = Barrier::new(qty_threads + 1);
            let wait_asserts_barrier = Barrier::new(qty_threads + 1);

            crate::thread::scope(|scope| {
                for _ in 0..qty_threads {
                    scope.spawn(|| {
                        let ptr = &mut 42u64 as *mut u64;

                        let local = hp.local().unwrap();
                        let g = local.protect(ptr).unwrap();

                        protect_barrier.wait();
                        wait_asserts_barrier.wait();

                        g.unprotect();
                        local.finish();
                    });
                }

                protect_barrier.wait();

                for l in unsafe { &*hp.inner.get() }.locals.iter().take(qty_threads) {
                    if let Some(slot) = l.get_slot(0) {
                        assert!(!slot.is_null(), "Pointer should be protected");
                    }
                }

                wait_asserts_barrier.wait();
            });

            for l in &unsafe { &*hp.inner.get() }.locals {
                if let Some(slot) = l.get_slot(0) {
                    assert!(slot.is_null(), "Pointer should not be protected");
                }
            }
        });
    }

    #[test]
    fn high_contention_protect_and_retire() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            let qty_threads = 2; //TODO Increase to 12
            let items_per_thread = 100u64;
            let barrier = Barrier::new(qty_threads);

            let retired_qty = AtomicUsize::new(0);

            crate::thread::scope(|scope| {
                for _ in 0..qty_threads {
                    scope.spawn(|| {
                        let ptrs = (0..items_per_thread)
                            .map(|i| std::ptr::from_mut::<u64>(Box::leak(Box::new(i))))
                            .collect::<Vec<_>>();

                        // Sync start to increase contention
                        barrier.wait();

                        let local = hp.local().unwrap();
                        for ptr in ptrs {
                            if let Some(g) = local.protect(ptr) {
                                g.retire();
                                retired_qty.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        local.finish();
                    });
                }
            });

            // Final reclamation of everything
            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert_eq!(
                v.len(),
                retired_qty.load(Ordering::SeqCst),
                "All pointers that were retired should be reclaimed"
            );

            for ptr in v {
                let _ = unsafe { Box::from_raw(ptr) };
            }
        });
    }

    #[test]
    fn mixed_concurrent_access() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            // This simulates the real-world hazard pointer use case:
            // Thread A is reading/protecting a value,
            // while Thread B is trying to retire it.

            let ptr = ThreadSafePtr(std::ptr::from_mut::<u64>(&mut 42u64));

            // thread 1: g is protected
            // thread 2: g is protected and retired
            let barrier1 = Barrier::new(2);

            // thread 1: g is still protected
            // thread 2: reclaim is called
            let barrier2 = Barrier::new(2);

            crate::thread::scope(|scope| {
                scope.spawn(|| {
                    let local = hp.local().unwrap();
                    let g = local.protect(ptr.ptr()).unwrap();
                    barrier1.wait();
                    barrier2.wait();
                    g.unprotect();
                    local.finish();
                });

                scope.spawn(|| {
                    let local = hp.local().unwrap();
                    let g = local.protect(ptr.ptr()).unwrap();
                    g.retire();
                    local.finish();
                    barrier1.wait();

                    let mut v = Vec::new();
                    hp.reclaim(&mut v);
                    assert!(v.is_empty(), "g is still protected by thread 1");

                    barrier2.wait();
                });
            });

            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert_eq!(v.len(), 1, "Now g is reclaimed");
            assert_eq!(v[0], ptr.ptr(), "Now g is reclaimed");
        });
    }

    #[test]
    fn local_finish_must_set_local_as_available() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(2, 2);

            let local1 = hp.local().unwrap();
            let local2 = hp.local().unwrap();

            assert!(hp.local().is_none(), "Local should have returned None");

            local1.finish();
            local2.finish();

            let local1 = hp.local().unwrap();
            let local2 = hp.local().unwrap();

            assert!(hp.local().is_none(), "Local should have returned None");

            local1.finish();
            local2.finish();
        });
    }

    #[test]
    fn slots_remain_reusable_across_cycles() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(2, 2);

            let ptr = &mut 42u64 as *mut u64;

            for _ in 0..16 {
                let local = hp.local().unwrap();
                for _ in 0..16 {
                    let g = local.protect(ptr).unwrap();
                    g.unprotect();
                }
                local.finish();
            }
        });
    }

    #[test]
    fn guard_drop() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            let ptr = &mut 42u64 as *mut u64;

            let local = hp.local().unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                assert!(local.get_slot(0).unwrap().is_null(), "slot should be free");
                let _g = local.protect(ptr);
                assert!(!local.get_slot(0).unwrap().is_null(), "ptr is protected");
            }));

            assert!(
                local.get_slot(0).unwrap().is_null(),
                "slot should be free now"
            );

            if cfg!(debug_assertions) {
                assert!(
                    result.is_err(),
                    "On Debug, dropping a live guard must trip the drop bomb"
                );
            } else {
                assert!(
                    result.is_ok(),
                    "On Release, dropping a live guard does not panic"
                );
            }

            local.finish();
        });
    }

    #[test]
    fn double_retire() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            let ptr = &mut 42u64 as *mut u64;

            let local = hp.local().unwrap();
            let g1 = local.protect(ptr).unwrap();
            g1.retire();
            let g2 = local.protect(ptr).unwrap();
            g2.retire();

            let mut v = vec![];
            hp.reclaim(&mut v);

            dbg!(&v);
            assert!(v.len() == 1, "Pointer should be returned only once");

            local.finish();
        });
    }

    #[test]
    fn protect_null_pointer() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            assert!(
                local.protect(null_mut()).is_none(),
                "Should return None for null pointers"
            );

            local.finish();
        });
    }

    #[test]
    fn empty_slots_must_not_protect_any_pointer() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();
            let ptr = &mut 42u64 as *mut u64;

            // No slot is protecting anything, so a non-null pointer that was
            // never protected should be considered unprotected.
            let mut reclaimed = Vec::new();
            hp.reclaim(&mut reclaimed);
            assert!(reclaimed.is_empty());

            // Sanity: actually protecting and then retiring the pointer works.
            let g = local.protect(ptr).unwrap();
            g.retire();
            let mut reclaimed = Vec::new();
            hp.reclaim(&mut reclaimed);
            assert_eq!(reclaimed.len(), 1);
            assert_eq!(reclaimed[0], ptr);

            local.finish();
        });
    }

    #[test]
    fn zero_locals_returns_none() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(0, 8);
            assert!(
                hp.local().is_none(),
                "With zero locals, local() must return None"
            );
        });
    }
}
