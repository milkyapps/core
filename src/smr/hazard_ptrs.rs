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
//! Hazard pointers solves this by "protecting" and "retiring" pointers, instead
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
//! use milkyapps_core::smr::hazard_ptrs::HazardPointers;
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

#[derive(Debug)]
struct RetireNode<T> {
    next: AtomicPtr<RetireNode<T>>,
    ptr: *mut T,
}

/// A guard keeping a pointer protected.
///
/// While a `Guard` is alive, the pointer it holds cannot be reclaimed by
/// [`HazardPointers::reclaim`]. Dispose of every guard explicitly with either
/// [`Guard::unprotect`] (release the protection) or [`Guard::retire`]
/// (release the protection *and* schedule the pointer for reclamation).
///
/// Dropping a live guard without consuming it is a programmer error: in debug
/// builds the [`Drop`] implementation panics (a "drop bomb") to catch leaked
/// protections early.
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
    /// Releases the protection and consumes the guard.
    ///
    /// After this returns, the pointer is no longer kept alive and may be
    /// reclaimed by a future [`HazardPointers::reclaim`].
    pub fn unprotect(mut self) {
        if !self.ptr.is_null() {
            self.local.unprotect_with_id(self.id, self.ptr);
            self.defuse();
        }
    }

    /// Releases the protection and schedules `ptr` for reclamation.
    ///
    /// The pointer becomes eligible for [`HazardPointers::reclaim`] as soon as
    /// no other guard is protecting it.
    ///
    /// Returns `true` if the pointer was scheduled, or `false` if this guard
    /// held a null pointer (a no-op).
    ///
    /// # Safety contract
    ///
    /// A pointer may ONLY be retired once it is guaranteed to be unreachable to
    /// every other thread: after this call, [`Local::protect`] must not be
    /// called on this pointer again. Retiring a pointer that another thread
    /// can still reach lets [`HazardPointers::reclaim`] hand it back to the
    /// caller while it is still in use — a use-after-free.
    #[must_use]
    pub fn retire(self) -> bool {
        if self.ptr.is_null() {
            false
        } else {
            self.local.push_retire_head(self.ptr);
            self.unprotect();
            true
        }
    }

    fn defuse(&mut self) {
        self.ptr = null_mut();
    }
}

/// A per-thread handle through which pointers are protected and retired.
///
/// At most one thread may use a `Local` at a time. A `Local` must be released
/// with [`Local::finish`] when the thread is done with it; dropping a `Local`
/// without finishing is a programmer error (debug builds panic).
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

    /// Releases this handle.
    ///
    /// Any pointers still protected through this handle are unprotected as a
    /// fallback. The preferred pattern is to release each guard individually
    /// with [`Guard::unprotect`] or [`Guard::retire`]; this call is the
    /// safety net that makes sure nothing is left protected when the handle is
    /// dropped.
    pub fn finish(mut self) {
        self.finish_by_ref();
    }

    /// Publishes `ptr` so it cannot be reclaimed while the returned [`Guard`]
    /// is alive.
    ///
    /// Returns `None` if every protection slot for this handle is already in
    /// use. Protecting a null pointer succeeds and returns a guard that holds
    /// null (which never needs releasing and retires as a no-op).
    pub fn protect(&self, ptr: *mut T) -> Option<Guard<'_, T>> {
        if ptr.is_null() {
            return Some(Guard {
                local: self,
                id: 0,
                ptr,
            });
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

/// A registry implementing *safe memory reclamation* for concurrent,
/// lock-free data structures.
///
/// Each thread acquires a [`Local`] handle via [`HazardPointers::local`],
/// protects pointers it is about to dereference with [`Local::protect`], and
/// retires pointers it has unlinked with [`Guard::retire`]. Retired pointers
/// that are no longer protected by any thread are handed back to the caller by
/// [`HazardPointers::reclaim`], which then owns and must free them.
///
/// A registry is cheaply shareable (`Clone` shares the same underlying state)
/// and is `Send`/`Sync` when `T: Send`.
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
    /// Creates a registry sized for a known concurrency level.
    ///
    /// `locals` is the maximum number of threads that can hold a [`Local`]
    /// handle at the same time; [`HazardPointers::local`] returns `None` once
    /// they are all in use.
    ///
    /// `ptrs` is the maximum number of pointers a single thread can protect
    /// at the same time; [`Local::protect`] returns `None` once a thread has
    /// used up all its slots.
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

    /// Acquires a per-thread handle for protecting and retiring pointers.
    ///
    /// Returns `None` if all `locals` handles (see [`HazardPointers::with_capacity`])
    /// are currently held. The returned [`Local`] must be released with
    /// [`Local::finish`] when the thread is done.
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

    /// Collects retired pointers that are safe to free into `reclaimed`.
    ///
    /// A retired pointer is returned only once no guard is protecting it. The
    /// caller takes ownership of every pointer in `reclaimed` and is
    /// responsible for freeing/dropping them.
    ///
    /// `reclaimed` is sorted and deduplicated before returning, so it is safe
    /// to reuse the same `Vec` across calls (passing it in non-empty is fine;
    /// existing entries are kept and included in the sort/dedup).
    pub fn reclaim(&self, reclaimed: &mut Vec<*mut T>) {
        let inner = unsafe { &*self.inner.get() };
        for local in &inner.locals {
            let mut head = local.retire_head.swap(null_mut(), Ordering::Acquire);
            while !head.is_null() {
                // SAFETY: This deref is safe because nodes are only free'd below
                let node = unsafe { &mut *head };

                if Self::is_protected(node.ptr, inner) {
                    head = node.next.load(Ordering::Acquire);
                    node.next.store(null_mut(), Ordering::Release);
                    local.push_retire_node(node);
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

    /// `HazardPointers<T: Send>` claims `Send + Sync`; this is a compile-time
    /// assertion of that contract (sharing a registry across threads must be
    /// legal).
    #[test]
    fn hazard_pointers_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<HazardPointers<u64>>();
        assert_sync::<HazardPointers<u64>>();
    }

    /// `protect` must publish the pointer into a slot and `unprotect` must
    /// release it: after protect the slot holds the pointer, after unprotect
    /// the slot is empty again.
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

    /// Protecting more distinct pointers than a handle has slots must saturate:
    /// exactly `ptrs` protects succeed and the rest return `None`, with no
    /// slot reused for two live guards.
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

    /// `reclaim` on a registry with nothing retired must return an empty Vec
    /// and must not panic or crash.
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

    /// The simplest full cycle: protect a pointer, retire it (which releases
    /// the protection), then `reclaim` must hand that single pointer back to
    /// the caller.
    #[test]
    fn single_protect_retire_and_reclaim() {
        model(|| {
            let ptr = &mut 42u64 as *mut u64;

            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            let g = local.protect(ptr).unwrap();
            let _ = g.retire();

            let mut v = Vec::with_capacity(16);
            hp.reclaim(&mut v);

            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr, "Reclaimed pointer should be the retired one");

            local.finish();
        });
    }

    /// A retired pointer that another thread is still protecting must NOT be
    /// reclaimed, and must be reclaimed as soon as that protection is released.
    /// This is the core safety guarantee of hazard pointers.
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
            let _ = g12.retire();

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

    /// Retiring distinct pointers from several handles, with no lingering
    /// protections, must let a single `reclaim` return all of them.
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
            let _ = local1.protect(ptr1).unwrap().retire();
            let _ = local2.protect(ptr2).unwrap().retire();
            let _ = local3.protect(ptr3).unwrap().retire();

            local1.finish();
            local2.finish();
            local3.finish();

            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert_eq!(v.len(), 3, "All retired pointer should have been reclaimed");
        });
    }

    /// Among several retired pointers, `reclaim` must return only the ones not
    /// currently protected and hold back the rest — a partial reclamation.
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
            let _ = g12.retire();
            let g2 = local.protect(ptr2).unwrap();
            let _ = g2.retire();

            // Only ptr2 should be reclaimed because ptr1 is still in Hazard Array
            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
            assert_eq!(v[0], ptr2, "Only ptr2 is not protected");

            let _ = g11.retire();
            local.finish();

            let mut v = vec![];
            hp.reclaim(&mut v);
        });
    }

    /// Several threads protect a pointer concurrently; the registry must show
    /// the pointer as protected while they hold it, and as unprotected once
    /// they all release and finish.
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

    /// Under contention, every pointer retired by the threads must eventually
    /// be reclaimable: a final `reclaim` after the threads finish must return
    /// exactly as many pointers as were retired (no loss, no duplication).
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
                                let _ = g.retire();
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

    /// The real-world pattern: one thread protects a value while another
    /// retires it and calls `reclaim`. The value must survive `reclaim` while
    /// the protection is held, and be handed back by the next `reclaim` once
    /// the protector releases.
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
                    let _ = g.retire();
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

    /// `finish` must release the handle so it can be acquired again: once all
    /// handles are taken `local()` returns `None`, and after finishing them the
    /// same number can be acquired again.
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

    /// Protection slots must be reusable: many protect/unprotect cycles on
    /// the same handle, repeated across many handle acquire/finish cycles,
    /// must keep working without exhausting slots or panicking.
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

    /// Dropping a live guard (without `unprotect`/`retire`) must trip the drop
    /// bomb on debug builds (panic) and must still release the slot so it is
    /// usable afterwards; on release builds the drop is silent but the slot is
    /// still released.
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

    /// Retiring the same pointer twice must still yield it exactly once from
    /// `reclaim` (the dedup contract) — never zero (loss) and never twice
    /// (double free).
    #[test]
    fn double_retire() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            let ptr = &mut 42u64 as *mut u64;

            let local = hp.local().unwrap();
            let g1 = local.protect(ptr).unwrap();
            let _ = g1.retire();
            let g2 = local.protect(ptr).unwrap();
            let _ = g2.retire();

            let mut v = vec![];
            hp.reclaim(&mut v);

            dbg!(&v);
            assert!(v.len() == 1, "Pointer should be returned only once");

            local.finish();
        });
    }

    /// Protecting a null pointer is a permitted no-op: it succeeds, `unprotect`
    /// is a no-op, and `retire` on it returns `false` (nulls are never
    /// reclaimable).
    #[test]
    fn protect_null_pointer() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            let g = local.protect(null_mut()).unwrap();
            g.unprotect();

            let g = local.protect(null_mut()).unwrap();
            assert!(!g.retire(), "Retiring null pointer should return false");

            local.finish();
        });
    }

    /// With nothing protected, `reclaim` must return nothing; and a freshly
    /// protected-then-retired pointer must come back from `reclaim` exactly
    /// once. This guards against a stale slot falsely appearing as protected.
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
            let _ = g.retire();
            let mut reclaimed = Vec::new();
            hp.reclaim(&mut reclaimed);
            assert_eq!(reclaimed.len(), 1);
            assert_eq!(reclaimed[0], ptr);

            local.finish();
        });
    }

    /// A registry with zero handles must always return `None` from `local()`
    /// (no thread can acquire a handle).
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

    // -------------------------------------------------------------------------
    // ABA / concurrency robustness tests.
    //
    // Focus on the retirement list (a lock-free stack of RetireNode pushed via
    // an untagged AtomicPtr CAS — the prime ABA surface) and on `reclaim`
    // being safe to call concurrently with `retire`.
    //
    // Invariants exercised:
    //   * a protected pointer is never reclaimed,
    //   * every retired pointer is reclaimed exactly once (no loss, no double),
    //   * `reclaim` is idempotent once the retire list is drained,
    //   * `local()` never hands the same slot to two threads.
    //
    // Tests deliberately avoid freeing reclaimed pointers in the concurrent
    // cases so that a hypothetical double-reclaim surfaces as a detected
    // condition (via a claimed-bit swap) rather than as test-side UB.
    // -------------------------------------------------------------------------

    /// After every retired pointer has been handed back, a second `reclaim`
    /// must return nothing — `reclaim` must not re-publish already-reclaimed
    /// pointers (which would happen if a freed retirement node's address were
    /// reused, the ABA hazard on the retirement list).
    #[test]
    fn reclaim_is_idempotent_once_drained() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let local = hp.local().unwrap();

            let ptrs: Vec<*mut u64> = (0..4)
                .map(|_| Box::leak(Box::new(42u64)) as *mut u64)
                .collect();
            for &p in &ptrs {
                let _ = local.protect(p).unwrap().retire();
            }
            local.finish();

            let mut v1 = Vec::new();
            hp.reclaim(&mut v1);
            assert_eq!(
                v1.len(),
                ptrs.len(),
                "first reclaim returns all retired ptrs"
            );

            let mut v2 = Vec::new();
            hp.reclaim(&mut v2);
            assert!(
                v2.is_empty(),
                "second reclaim on a drained retire list must return nothing (ABA leak)"
            );

            // Free only the singly-reclaimed pointers (sound: reclaimed once).
            for p in v1 {
                // SAFETY: `p` was a leaked `Box` and reclaim handed us unique
                // ownership of it exactly once.
                unsafe {
                    drop(Box::from_raw(p));
                }
            }
        });
    }

    /// Two threads call `reclaim` concurrently on a retire list of N unique
    /// pointers. Each pointer must be claimed exactly once across both calls —
    /// never zero (loss) and never twice (double free / ABA). Detection is via
    /// a per-pointer `AtomicBool` claimed-bit, so no UB is incurred if the
    /// implementation double-reclaims.
    #[test]
    fn concurrent_reclaim_no_double_reclaim() {
        model(|| {
            const N: usize = 8;
            let hp = HazardPointers::<usize>::with_capacity(8, 8);

            // Leaked boxes whose *value* is the index; never freed by the test
            // so that a double-reclaim is observable without UB.
            let ptrs: Vec<*mut usize> = (0..N)
                .map(|i| Box::leak(Box::new(i)) as *mut usize)
                .collect();
            let claimed: Vec<AtomicBool> = (0..N).map(|_| AtomicBool::new(false)).collect();

            // Retire all from one local so the retire list is populated before
            // the reclaimer threads race on it.
            let local = hp.local().unwrap();
            for &p in &ptrs {
                let _ = local.protect(p).unwrap().retire();
            }
            local.finish();

            let claimed_ref = &claimed;
            let hp = &hp;
            crate::thread::scope(|scope| {
                for _ in 0..2 {
                    scope.spawn(move || {
                        let mut v = Vec::new();
                        hp.reclaim(&mut v);
                        for p in v {
                            // SAFETY: `p` is a leaked Box we (tentatively) own
                            // via reclaim; reading the index is a shared read
                            // with no concurrent writer.
                            let idx = unsafe { *p };
                            if claimed_ref[idx].swap(true, Ordering::AcqRel) {
                                panic!("pointer {idx} reclaimed twice (double free / ABA)");
                            }
                        }
                    });
                }
            });

            let total: usize = claimed
                .iter()
                .map(|b| usize::from(b.load(Ordering::SeqCst)))
                .sum();
            assert_eq!(
                total, N,
                "every retired pointer must be reclaimed exactly once (no loss, no double)"
            );
        });
    }

    /// Threads retire unique pointers while a concurrent reclaimer drains the
    /// list. After joining plus a final drain, the number reclaimed must equal
    /// the number retired — no pointer lost, none reclaimed twice. Pointers
    /// are leaked (not freed) so a double-reclaim is detected by count rather
    /// than by UB.
    #[test]
    fn concurrent_retire_and_reclaim_counts_match() {
        model(|| {
            const THREADS: usize = 2;
            const PER: usize = 4;
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let retired = AtomicUsize::new(0);
            let reclaimed = AtomicUsize::new(0);

            crate::thread::scope(|scope| {
                let hp = &hp;
                let retired = &retired;
                let reclaimed = &reclaimed;
                for _ in 0..THREADS {
                    scope.spawn(move || {
                        let local = hp.local().unwrap();
                        for _ in 0..PER {
                            let p = Box::leak(Box::new(42u64)) as *mut u64;
                            let _ = local.protect(p).unwrap().retire();
                            retired.fetch_add(1, Ordering::Relaxed);
                        }
                        local.finish();
                    });
                }
                // Concurrent reclaimer: drain repeatedly while retirers run.
                scope.spawn(move || {
                    let mut v = Vec::new();
                    for _ in 0..8 {
                        hp.reclaim(&mut v);
                    }
                    reclaimed.fetch_add(v.len(), Ordering::Relaxed);
                    // Intentionally do not free: avoids UB if impl double-reclaims.
                });
            });

            // Final drain after everyone joined.
            let mut v = Vec::new();
            hp.reclaim(&mut v);
            reclaimed.fetch_add(v.len(), Ordering::Relaxed);

            assert_eq!(
                retired.load(Ordering::SeqCst),
                reclaimed.load(Ordering::SeqCst),
                "every retired pointer must be reclaimed exactly once (no loss, no double)"
            );
        });
    }

    /// Two threads protect the same pointer; a third retires it and tries to
    /// reclaim. The pointer must not be reclaimed while either protector
    /// holds it, and must be reclaimed once both release.
    #[test]
    fn two_protectors_block_reclaim() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let value: &mut u64 = Box::leak(Box::new(42u64));
            let ptr = ThreadSafePtr(value as *mut u64);

            // 3 threads meet at each barrier (2 protectors + 1 reclaimer).
            let b_protect = Barrier::new(3);
            let b_release = Barrier::new(3);
            let b_unprotected = Barrier::new(3);

            crate::thread::scope(|scope| {
                for _ in 0..2 {
                    scope.spawn(|| {
                        let local = hp.local().unwrap();
                        let g = local.protect(ptr.ptr()).unwrap();
                        b_protect.wait();
                        b_release.wait();
                        g.unprotect();
                        b_unprotected.wait();
                        local.finish();
                    });
                }
                scope.spawn(|| {
                    let local = hp.local().unwrap();
                    let g = local.protect(ptr.ptr()).unwrap();
                    let _ = g.retire();
                    local.finish();

                    b_protect.wait();
                    let mut v = Vec::new();
                    hp.reclaim(&mut v);
                    assert!(
                        v.is_empty(),
                        "ptr still protected by two threads; must not be reclaimed"
                    );

                    b_release.wait();
                    b_unprotected.wait();

                    let mut v = Vec::new();
                    hp.reclaim(&mut v);
                    assert_eq!(v.len(), 1, "ptr reclaimed after both protectors release");
                    assert_eq!(v[0], ptr.ptr());
                });
            });
        });
    }

    /// With `locals = 2` and three threads contending for `local()` at the
    /// same time, exactly two must acquire a local and one must get `None` —
    /// the availability CAS must never hand the same local to two threads
    /// concurrently. A barrier makes all three call `local()` before any
    /// releases via `finish()`.
    #[test]
    fn local_exhaustion_returns_none_under_contention() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(2, 2);
            let acquired = AtomicUsize::new(0);
            let failed = AtomicUsize::new(0);
            let barrier = Barrier::new(3);

            crate::thread::scope(|scope| {
                for _ in 0..3 {
                    scope.spawn(|| {
                        match hp.local() {
                            Some(l) => {
                                acquired.fetch_add(1, Ordering::SeqCst);
                                // Hold the local across the barrier so all
                                // three contend simultaneously.
                                barrier.wait();
                                l.finish();
                            }
                            None => {
                                failed.fetch_add(1, Ordering::SeqCst);
                                barrier.wait();
                            }
                        }
                    });
                }
            });

            assert_eq!(
                acquired.load(Ordering::SeqCst) + failed.load(Ordering::SeqCst),
                3,
                "every thread must observe exactly one outcome"
            );
            assert_eq!(acquired.load(Ordering::SeqCst), 2, "only two locals exist");
            assert_eq!(failed.load(Ordering::SeqCst), 1, "one thread must get None");
        });
    }

    /// Interleave `retire` and `reclaim` on the same handle across threads in a
    /// tight loop. This stresses the CAS that pushes onto the retirement list
    /// against the swap that drains it — the surface where an ABA on the
    /// untagged retirement-list head would corrupt the list. We assert no panic
    /// and no deadlock (loom will flag a hang); a final drain must leave the
    /// list empty.
    #[test]
    fn retire_reclaim_burst_no_panic_or_deadlock() {
        model(|| {
            const THREADS: usize = 2;
            const ITERS: usize = 8;
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            crate::thread::scope(|scope| {
                for _ in 0..THREADS {
                    scope.spawn(|| {
                        let local = hp.local().unwrap();
                        for _ in 0..ITERS {
                            let p = Box::leak(Box::new(42u64)) as *mut u64;
                            let _ = local.protect(p).unwrap().retire();
                            let mut v = Vec::new();
                            hp.reclaim(&mut v);
                            // Intentionally do not free.
                        }
                        local.finish();
                    });
                }
            });

            let mut v = Vec::new();
            hp.reclaim(&mut v);
            assert!(
                v.is_empty(),
                "after everyone joined, a final reclaim must drain everything"
            );
        });
    }

    /// A pointer retired and reclaimed, then had its address reused for a new
    /// retirement, must not trip an ABA: reclaim must not return the new
    /// retirement's pointer as if it were the old one. Single-threaded but
    /// exercises the retire→reclaim→reuse→retire→reclaim cycle.
    #[test]
    fn retire_reclaim_reuse_retire_cycle() {
        model(|| {
            let hp = HazardPointers::<u64>::with_capacity(8, 8);

            for _ in 0..16 {
                let local = hp.local().unwrap();
                // Allocate, retire, reclaim, then explicitly free so the
                // address may be reused by the next Box::new.
                let p = Box::into_raw(Box::new(42u64));
                let _ = local.protect(p).unwrap().retire();
                local.finish();

                let mut v = Vec::new();
                hp.reclaim(&mut v);
                assert_eq!(v.len(), 1, "exactly one pointer reclaimed per cycle");
                assert_eq!(v[0], p, "reclaimed pointer must be the one we retired");
                // SAFETY: reclaim gave us unique ownership.
                unsafe {
                    drop(Box::from_raw(p));
                }
            }
        });
    }

    /// Many threads all protecting the same pointer while one thread retires
    /// and repeatedly reclaims: the pointer must never be reclaimed until
    /// every protector has released. Three barriers separate the phases so
    /// the reclaimer's final reclaim strictly happens-after the protectors'
    /// `unprotect`.
    #[test]
    fn many_protectors_one_reclaimer_no_early_reclaim() {
        model(|| {
            const PROTECTORS: usize = 2;
            let hp = HazardPointers::<u64>::with_capacity(8, 8);
            let value: &mut u64 = Box::leak(Box::new(42u64));
            let ptr = ThreadSafePtr(value as *mut u64);
            let ready = Barrier::new(PROTECTORS + 1);
            let release = Barrier::new(PROTECTORS + 1);
            let unprotected = Barrier::new(PROTECTORS + 1);

            crate::thread::scope(|scope| {
                for _ in 0..PROTECTORS {
                    scope.spawn(|| {
                        let local = hp.local().unwrap();
                        let g = local.protect(ptr.ptr()).unwrap();
                        ready.wait();
                        release.wait();
                        g.unprotect();
                        unprotected.wait();
                        local.finish();
                    });
                }
                scope.spawn(|| {
                    let local = hp.local().unwrap();
                    let g = local.protect(ptr.ptr()).unwrap();
                    let _ = g.retire();
                    local.finish();

                    ready.wait();
                    for _ in 0..3 {
                        let mut v = Vec::new();
                        hp.reclaim(&mut v);
                        assert!(v.is_empty(), "pointer reclaimed while still protected");
                    }
                    release.wait();
                    unprotected.wait();
                    let mut v = Vec::new();
                    hp.reclaim(&mut v);
                    assert_eq!(
                        v.len(),
                        1,
                        "pointer reclaimed after all protectors released"
                    );
                });
            });
        });
    }
}
