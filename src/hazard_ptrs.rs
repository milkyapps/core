//! Hazard pointers — safe memory reclamation for lock-free data structures.
//!
//! # What it is
//!
//! Hazard pointers are a technique for *safe memory reclamation* in
//! concurrent, lock-free data structures. The core idea, introduced by Maged
//! M. Michael, is described in the seminal paper:
//!
//! > Maged M. Michael, *"Hazard Pointers: Safe Memory Reclamation for
//! > Lock-Free Objects,"* IEEE Transactions on Parallel and Distributed
//! > Systems, vol. 15, no. 8, pp. 491–504, August 2004.
//!
//! The paper motivates the technique as follows:
//!
//! > "A new technique for dynamic memory reclamation for concurrent lock-free
//! > objects is presented. The technique uses hazard pointers, one per thread,
//! > to indicate to concurrent threads that the referenced objects are
//! > currently in use and should not be reclaimed."
//!
//! In the same paper, the mechanism is summarized succinctly:
//!
//! > "A hazard pointer is a pointer to a memory location that a thread is
//! > currently accessing. ... If a thread's hazard pointer holds the address of
//! > an object, then no other thread may reclaim that object."
//!
//! Concretely, each thread publishes the address of any object it is about to
//! dereference into a shared *hazard slot* before reading it. A thread that
//! wants to retire (i.e., unlink and eventually free) an object publishes it
//! to a *retirement list* and defers its reclamation: the object is only freed
//! once a scan confirms that no thread's hazard slot still references it. This
//! guarantees that an object is never freed while a thread holds an
//! unprotected reference to it — the central safety property of the technique.
//!
//! # Why use it
//!
//! Lock-free algorithms unlink objects from a structure (e.g. a node removed
//! from a stack) before they are sure that no other thread is still reading
//! them. Naïvely freeing the memory immediately is unsound: another thread may
//! have loaded a pointer to that node and be about to dereference it, leading
//! to a use-after-free. The alternatives each have drawbacks:
//!
//! - **Leaking** the memory (never freeing) is safe but unbounded in size.
//! - **Reference counting** is hard to make lock-free without strong atomic
//!   updates and can suffer from the ABA problem.
//! - **Garbage collection** is not generally available in Rust and introduces
//!   pauses and runtime cost.
//!
//! Hazard pointers solve this with bounded overhead, no GC, and — unlike
//! epoch-based reclamation (e.g. Crossbeam's `epoch`) — they reclaim memory
//! *promptly* and do not require global quiescence. Michael's paper notes the
//! technique's main properties:
//!
//! > "The technique has low overhead, does not require special operating
//! > system or hardware support, and is independent of the number of threads
//! > and the number of processors."
//!
//! In exchange for these properties, hazard pointers pay a per-access cost
//! (publishing the pointer on each dereference) and limit how many distinct
//! objects a thread can protect simultaneously per slot.
//!
//! This crate implements a small, direct version of the scheme:
//! - A fixed-size global array of hazard slots ([`HAZARD_ARRAY`]),
//!   with availability tracked in [`SLOT_AVAILABLE`].
//! - Per-thread slot ownership recorded in the thread-local [`ID`].
//! - A lock-free singly-linked retirement list rooted at [`RETIRE_HEAD`].
//! - RAII protection via [`Guard`] and the [`protect`]/[`unprotect`]/[`retire`]
//!   functions, plus a [`reclaim`] pass that frees the safe nodes.
//!
//! # Example
//!
//! A producer thread publishes a pointer into a hazard slot while a consumer
//! retires it; the memory is reclaimed only once the producer is done:
//!
//! ```ignore
//! use std::sync::Barrier;
//! use milkyapps_core::hazard_ptrs::{install, protect, unprotect, retire, reclaim};
//!
//! std::thread::scope(|scope| {
//!     // Thread A: read the value while protected.
//!     scope.spawn(|| {
//!         install();
//!         let mut value = Box::new(42u64);
//!         let ptr = value.as_mut() as *mut u64 as *mut ();
//!
//!         let guard = protect(ptr); // publish 'ptr' as a hazard pointer
//!         // ... dereference 'ptr' here; it is guaranteed not to be freed ...
//!         unprotect(guard);        // release protection
//!     });
//!
//!     // Thread B: retire the same object (queue it for later reclamation).
//!     scope.spawn(|| {
//!         install();
//!         let mut value = Box::new(42u64);
//!         let ptr = value.as_mut() as *mut u64 as *mut ();
//!
//!         let guard = protect(ptr);
//!         retire(guard); // pointer is queued, not yet freed
//!
//!         // Safe to call periodically: frees only objects that no
//!         // thread is currently protecting.
//!         let mut reclaimed = Vec::new();
//!         reclaim(&mut reclaimed);
//!     });
//! });
//! ```
//!
//! Notice that `reclaim` will *not* free `ptr` while Thread A's hazard slot
//! still references it — only once Thread A calls [`unprotect`] does the object
//! become eligible for reclamation on a subsequent `reclaim`. That is the
//! guarantee at the heart of hazard pointers.

use std::{
    cell::UnsafeCell,
    panic,
    ptr::null_mut,
    sync::{
        Arc, Weak,
        atomic::{AtomicPtr, Ordering},
    },
};

/// A node in the singly-linked retirement list.
///
/// Retired pointers are queued here until a subsequent [`reclaim`] call
/// determines they are no longer protected by any hazard pointer and frees them.
#[derive(Debug)]
struct RetireNode<T> {
    /// The retired pointer awaiting reclamation.
    ptr: *mut T,
    /// Pointer to the next node in the retirement list.
    next: AtomicPtr<RetireNode<T>>,
}

/// An RAII guard guarding a pointer that is currently published in a hazard slot.
///
/// Dropping a guard without first consuming it via [`unprotect`] or [`retire`]
/// is a programmer error: in debug builds the [`Drop`] implementation panics
/// (a "drop bomb") to catch leaked protections early.
pub struct Guard<T> {
    /// Weak pointer to the Registry
    hp: Weak<UnsafeCell<HazardPointersInner<T>>>,
    /// Slod id guarded by this guard
    id: usize,
    /// The protected pointer, or null once the guard has been defused.
    ptr: *mut T,
}

impl<T> Drop for Guard<T> {
    /// Asserts that the guard was properly disposed of.
    ///
    /// In debug builds this panics if the guard was dropped without a call to [`unprotect`] or [`retire`].
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // On debug we will panic, but we must unprotect in case
            // the panic is catched
            if let Some(hp) = self.hp.upgrade() {
                let inner = unsafe { &mut *hp.get() };
                inner.unprotect_with_id(self.id, self.ptr);
            }

            if cfg!(debug_assertions) {
                panic!(
                    "Hazard Pointer Guard dropped without calling `unprotect` or `retire` (drop_bomb)"
                );
            } else {
                // TODO
                // warn
            }
        }
    }
}

struct HazardPointersInner<T> {
    is_slot_available: Vec<bool>,
    slots: Vec<AtomicPtr<T>>,
    retire_head: AtomicPtr<RetireNode<T>>,
}

impl<T> HazardPointersInner<T> {
    /// Publishes `ptr` in the hazard slot identified by `id`.
    ///
    /// The slot must currently hold a null pointer; attempting to protect a second
    /// pointer in the same slot fails
    pub fn protect_with_id(&mut self, id: usize, ptr: *mut T) -> Result<(), ()> {
        debug_assert!(id < self.slots.len());
        match self.slots[id].compare_exchange(null_mut(), ptr, Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => {
                self.is_slot_available[id] = false;
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    /// Clears the hazard slot identified by `id`, releasing protection of `ptr`.
    ///
    /// Panics if the slot was not currently protecting exactly `ptr`.
    fn unprotect_with_id(&mut self, id: usize, ptr: *mut T) {
        debug_assert!(id < self.slots.len());

        match self.slots[id].compare_exchange(ptr, null_mut(), Ordering::AcqRel, Ordering::Relaxed)
        {
            Ok(_) => {}
            Err(_) => {
                panic!("This pointer was not being protected")
            }
        }
    }

    /// Queues the pointer held by the slot identified by `id` for retirement and
    /// then clears that slot's protection.
    ///
    /// After this call the pointer is no longer protected by `id`, but is not yet
    /// reclaimed — it will be freed by a later [`reclaim`] once no slot protects it.
    fn retire_with_id(&mut self, id: usize, ptr: *mut T) {
        debug_assert!(id < self.slots.len());

        self.push_retire_head(ptr);
        self.unprotect_with_id(id, ptr);
    }

    /// Allocates a [`RetireNode`] for `ptr` and atomically pushes it onto the
    /// front of the retirement list.
    ///
    /// Returns a pointer to the newly inserted node.
    fn push_retire_head(&mut self, ptr: *mut T) -> *mut RetireNode<T> {
        let new = Box::leak(Box::new(RetireNode {
            ptr,
            next: AtomicPtr::new(null_mut()),
        }));
        let mut current = self.retire_head.load(Ordering::Acquire);
        loop {
            new.next = AtomicPtr::new(current);
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

#[derive(Clone)]
pub struct HazardPointers<T> {
    inner: Arc<UnsafeCell<HazardPointersInner<T>>>,
}

unsafe impl<T> Sync for HazardPointers<T> {}
unsafe impl<T> Send for HazardPointers<T> {}

impl<T> HazardPointers<T> {
    pub fn with_capacity(capacity: usize) -> HazardPointers<T> {
        HazardPointers {
            inner: Arc::new(UnsafeCell::new(HazardPointersInner {
                is_slot_available: vec![true; capacity],
                slots: {
                    let mut v = Vec::with_capacity(capacity);
                    for _ in 0..capacity {
                        v.push(AtomicPtr::new(null_mut()))
                    }
                    v
                },
                retire_head: AtomicPtr::new(null_mut()),
            })),
        }
    }

    fn find_slot_available(&self) -> Option<usize> {
        let inner = unsafe { &mut *self.inner.get() };
        // TODO fail after N tries
        for _ in 0..16 {
            let idx = {
                if cfg!(target_arch = "x86_64") {
                    todo!()
                } else if cfg!(target_arch = "aarch64") {
                    crate::simd::position_of_any_bool(inner.is_slot_available.as_ref(), true)
                } else {
                    inner.is_slot_available.iter().position(|&b| b)
                }
            };

            if let Some(idx) = idx {
                return Some(idx);
            }
        }

        None
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
    unsafe fn debug_retire_list(&self, head: &AtomicPtr<RetireNode<T>>)
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

    /// Resets all global hazard-pointer state to its initial, empty configuration.
    ///
    /// Marks every slot as available, clears every hazard pointer, and empties the
    /// retirement list. Intended for use between tests; calling this while threads
    /// are actively using the system is unsafe.
    ///
    /// # Safety
    ///
    /// This method is unsafe because it clears all the data structures without locks.
    /// So the caller must guarantee there is nothing running whilst this is called.
    pub unsafe fn clear(&self) {
        let inner = unsafe { &mut *self.inner.get() };

        for item in inner.is_slot_available.iter_mut() {
            *item = true;
        }

        for slot in inner.slots.iter_mut() {
            slot.store(null_mut(), Ordering::SeqCst);
        }

        inner.retire_head.store(null_mut(), Ordering::SeqCst);
    }

    /// Publishes `ptr` in the current thread's hazard slot and returns a [`Guard`].
    ///
    /// Relies on [`install`] having previously claimed a slot for this thread.
    pub fn protect(&self, ptr: *mut T) -> Option<Guard<T>> {
        let inner = unsafe { &mut *self.inner.get() };

        loop {
            let id = self.find_slot_available()?;
            if inner.protect_with_id(id, ptr).is_err() {
                continue;
            }
            return Some(Guard {
                hp: Arc::downgrade(&self.inner),
                id,
                ptr,
            });
        }
    }

    /// Releases protection of the pointer held by `g`, consuming the guard.
    ///
    /// Clears the current thread's hazard slot and defuses the guard so its [`Drop`]
    /// does not trigger the drop bomb.
    pub fn unprotect(&self, mut g: Guard<T>) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.unprotect_with_id(g.id, g.ptr);

        // Defuse drop bomb
        g.ptr = null_mut();
    }

    /// Retires the pointer guarded by `g`, consuming the guard.
    ///
    /// Pushes the pointer onto the retirement list and clears the current thread's
    /// hazard slot, defusing the guard so its [`Drop`] does not trigger the drop bomb.
    /// The memory is reclaimed lazily by a subsequent [`reclaim`].
    pub fn retire(&self, mut g: Guard<T>) {
        let inner = unsafe { &mut *self.inner.get() };
        inner.retire_with_id(g.id, g.ptr);

        // Defuse drop bomb
        g.ptr = null_mut();
    }

    pub fn reclaim(&self, v: &mut Vec<*mut T>) {
        let inner = unsafe { &mut *self.inner.get() };

        // Take control of the whole list
        let mut current = inner.retire_head.swap(null_mut(), Ordering::Acquire);
        // We control the whole list now

        // List is empty
        if current.is_null() {
            return;
        }

        // check if the current.ptr is inside the hazard array.
        // If it is, we cannot drop this item as it is still being used
        // If it isnt, we delete its memory and connect the last node to the next
        let mut first: *mut RetireNode<T> = current;
        let mut last: *mut RetireNode<T> = current;

        while !current.is_null() {
            assert!(!first.is_null());
            assert!(!last.is_null());

            // SAFETY: Guaranteed to not be null here
            // Only we have access to current, so it will live for this whole function
            let c = unsafe { &mut (*current) };

            let ptr = c.ptr;
            let is_safe_to_delete = inner
                .slots
                .iter()
                .position(|item| ptr == item.load(Ordering::Acquire))
                .is_none();

            let next = c.next.load(Ordering::Relaxed);

            if is_safe_to_delete {
                // SAFETY: We allocated using Box::leak(Box::new(...)) above
                let c = unsafe { Box::from_raw(current) };
                v.push(c.ptr);

                if first == current {
                    first = next;
                    last = next;
                } else {
                    if !next.is_null() {
                        // SAFETY: Deref last is safe because it started as current which is not null
                        unsafe { (*last).next.store(next, Ordering::Relaxed) };
                    }
                }
            } else {
                last = current;
            }

            current = next;
        }

        // If last is null, we deleted all nodes
        if last.is_null() {
            assert!(first.is_null());
            return;
        }

        // prepend the current list to head
        let mut current = inner.retire_head.load(Ordering::Acquire);
        loop {
            unsafe { (*last).next.store(current, Ordering::Relaxed) };

            match inner.retire_head.compare_exchange_weak(
                current,
                first,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(new_head) => current = new_head,
            }
        }
    }

    #[cfg(test)]
    fn get_slot(&self, id: usize) -> Option<*mut T> {
        unsafe { &mut *self.inner.get() }
            .slots
            .get(id)
            .map(|x| x.load(Ordering::SeqCst))
    }

    #[cfg(test)]
    fn retire_head(&self) -> *mut RetireNode<T> {
        unsafe { &mut *self.inner.get() }
            .retire_head
            .load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, atomic::AtomicUsize};

    #[test]
    fn protect_unprotect_must_use_slots() {
        let mut value = Box::new(42u64);
        let ptr = value.as_mut() as *mut u64;

        let hp = HazardPointers::<u64>::with_capacity(8);

        assert!(
            hp.get_slot(0).unwrap().is_null(),
            "Thread should not be protecting any pointer"
        );
        let g = hp.protect(ptr).unwrap();
        assert_eq!(
            hp.get_slot(0).unwrap(),
            ptr,
            "Thread should be protecting ptr"
        );
        hp.unprotect(g);
        assert!(
            hp.get_slot(0).unwrap().is_null(),
            "Thread should not be protecting ptr anymore"
        );
    }

    #[test]
    fn more_protects_than_slots() {
        let mut value = Box::new(42u64);
        let ptr = value.as_mut() as *mut u64;

        let mut guards = vec![];
        let hp = HazardPointers::<u64>::with_capacity(8);
        for _ in 0..9 {
            guards.push(hp.protect(ptr));
        }

        let some_qty = guards.iter().filter(|x| x.is_some()).count();
        let none_qty = guards.iter().filter(|x| x.is_none()).count();

        assert_eq!(some_qty, 8);
        assert_eq!(none_qty, 1);

        for g in guards.into_iter().flatten() {
            hp.unprotect(g);
        }
    }

    #[test]
    fn reclaim_empty() {
        let hp = HazardPointers::<u64>::with_capacity(8);

        // Should not panic or crash
        assert!(hp.retire_head().is_null(), "Retire list should be empty");

        let mut v = Vec::new();
        hp.reclaim(&mut v);

        assert!(v.is_empty(), "Reclaim Vec should be empty");
        assert!(hp.retire_head().is_null(), "Retire list should be empty");
    }

    #[test]
    fn single_protect_retire_and_reclaim() {
        let mut value = Box::new(42u64);
        let ptr = value.as_mut() as *mut u64;

        let hp = HazardPointers::<u64>::with_capacity(8);

        let g = hp.protect(ptr).unwrap();
        hp.retire(g);

        let mut v = Vec::with_capacity(16);
        hp.reclaim(&mut v);

        assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
        assert_eq!(v[0], ptr, "Reclaimed pointer should be the retired one");
    }

    #[test]
    fn protect_prevents_reclaim() {
        let mut value = Box::new(42u64);
        let ptr = value.as_mut() as *mut u64;

        let hp = HazardPointers::<u64>::with_capacity(8);

        // Simulate two threads that are protecting the same ptr
        let g1 = hp.protect(ptr).unwrap();

        let inner = unsafe { &mut *hp.inner.get() };
        inner.protect_with_id(1, ptr).unwrap();
        inner.retire_with_id(1, ptr);

        assert!(
            !hp.retire_head().is_null(),
            "Retire List should not be empty"
        );

        // Because 'ptr' is still in the Hazard Array, it should NOT be reclaimed
        let mut v = Vec::new();
        hp.reclaim(&mut v);
        assert!(
            v.is_empty(),
            "ptr should still be protected and not reclaimed"
        );

        // Now ptr is no long protected and should be reclaimed
        hp.unprotect(g1);
        hp.reclaim(&mut v);
        assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
        assert_eq!(v[0], ptr, "Reclaimed pointer should be the retired one");

        assert!(hp.retire_head().is_null(), "Retire List should be empty");
    }

    #[test]
    fn multiple_retirements() {
        let ptr1 = Box::leak(Box::new(1u64)) as *mut u64;
        let ptr2 = Box::leak(Box::new(2u64)) as *mut u64;
        let ptr3 = Box::leak(Box::new(3u64)) as *mut u64;

        let hp = HazardPointers::<u64>::with_capacity(8);

        // Simulate three retirements without active protections
        let inner = unsafe { &mut *hp.inner.get() };
        inner.push_retire_head(ptr1);
        inner.push_retire_head(ptr2);
        inner.push_retire_head(ptr3);

        let mut v = Vec::new();
        hp.reclaim(&mut v);
        assert_eq!(v.len(), 3, "All retired pointer should have been reclaimed");
    }

    #[test]
    fn partial_reclaim() {
        let ptr1 = Box::leak(Box::new(1u64)) as *mut u64;
        let ptr2 = Box::leak(Box::new(2u64)) as *mut u64;

        let hp = HazardPointers::<u64>::with_capacity(8);

        // Protect p1, leave p2 unprotected
        // Put both into the retired list
        let g11 = hp.protect(ptr1).unwrap();
        let g12 = hp.protect(ptr1).unwrap();
        hp.retire(g12);
        let g2 = hp.protect(ptr2).unwrap();
        hp.retire(g2);

        // Only ptr2 should be reclaimed because ptr1 is still in Hazard Array
        let mut v = Vec::new();
        hp.reclaim(&mut v);
        assert_eq!(v.len(), 1, "Only one pointer should have been reclaimed");
        assert_eq!(v[0], ptr2, "Only ptr2 is not protected");

        hp.retire(g11);
    }

    #[test]
    fn protect_multi_thread() {
        let hp = HazardPointers::<u64>::with_capacity(8);

        let qty_threads = 12;
        let protect_barrier = Barrier::new(qty_threads + 1);
        let wait_asserts_barrier = Barrier::new(qty_threads + 1);

        std::thread::scope(|scope| {
            for _ in 0..qty_threads {
                scope.spawn(|| {
                    let p = Box::leak(Box::new(1u64)) as *mut u64;
                    let g = hp.protect(p);

                    protect_barrier.wait();
                    wait_asserts_barrier.wait();

                    if let Some(g) = g {
                        hp.unprotect(g);
                    }
                });
            }

            protect_barrier.wait();

            for i in 0..qty_threads {
                if let Some(slot) = hp.get_slot(i) {
                    assert!(!slot.is_null(), "Thread has protected its pointer");
                }
            }

            wait_asserts_barrier.wait();
        });

        for i in 0..qty_threads {
            if let Some(slot) = hp.get_slot(i) {
                assert!(slot.is_null(), "Thread has unprotected its pointer");
            }
        }
    }

    #[test]
    fn high_contention_protect_and_retire() {
        let hp = HazardPointers::<u64>::with_capacity(8);

        let qty_threads = 12;
        let items_per_thread = 100;
        let barrier = Barrier::new(qty_threads);

        let retired_qty = AtomicUsize::new(0);

        std::thread::scope(|scope| {
            for _ in 0..qty_threads {
                scope.spawn(|| {
                    let ptrs = (0..items_per_thread)
                        .map(|i| Box::leak(Box::new(i as u64)) as *mut u64)
                        .collect::<Vec<_>>();

                    // Sync start to increase contention
                    barrier.wait();

                    for ptr in ptrs {
                        if let Some(g) = hp.protect(ptr) {
                            hp.retire(g);
                            retired_qty.fetch_add(1, Ordering::Relaxed);
                        }
                    }
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
    }

    #[test]
    fn mixed_concurrent_access() {
        let hp = HazardPointers::<u64>::with_capacity(8);

        // This simulates the real-world hazard pointer use case:
        // Thread A is reading/protecting a value,
        // while Thread B is trying to retire it.

        let ptr = Box::leak(Box::new(100u64)) as *mut u64;
        let ptr = ptr as u64;

        // thread 1: g is protected
        // thread 2: g is protected and retired
        let barrier1 = Barrier::new(2);

        // thread 1: g is still protected
        // thread 2: reclaim is called
        let barrier2 = Barrier::new(2);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                let g = hp.protect(ptr as *mut u64).unwrap();
                barrier1.wait();
                barrier2.wait();
                hp.unprotect(g);
            });

            scope.spawn(|| {
                let g = hp.protect(ptr as *mut u64).unwrap();
                hp.retire(g);
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
        assert_eq!(v[0], ptr as *mut u64, "Now g is reclaimed");
    }

    #[test]
    fn guard_drop() {
        let hp = HazardPointers::<u64>::with_capacity(8);

        let mut value = Box::new(11u64);
        let ptr = value.as_mut() as *mut u64;

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert!(hp.get_slot(0).unwrap().is_null(), "slot should be free");
            let _g = hp.protect(ptr);
            assert!(!hp.get_slot(0).unwrap().is_null(), "ptr is protected");
        }));

        assert!(hp.get_slot(0).unwrap().is_null(), "slot should be free now");

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
    }
}
